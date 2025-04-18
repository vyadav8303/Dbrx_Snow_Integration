class IncidentProcessor:
    def __init__(self, spark, key_vault_url, table_config, destination_directory):
        self.spark = spark
        self.key_vault_url = key_vault_url
        self.destination_directory = destination_directory
        self.catalog_name = table_config['catalog']
        self.volume_name = table_config['schema']
        self.table_name = table_config['table']
        self.full_table_name = f"{self.catalog_name}.{self.volume_name}.{self.table_name}"

        self.secret_client = None
        self.databricks_token = None
        self.databricks_instance = None
        self.token_url = None
        self.client_id = None
        self.client_secret = None
        self.username = None
        self.password = None

        self._setup_key_vault()
        self._ensure_table_exists()

    def _setup_key_vault(self):
        credential = DefaultAzureCredential()
        self.secret_client = SecretClient(vault_url=self.key_vault_url, credential=credential)

        self.databricks_token = self.secret_client.get_secret("databricks-token").value
        self.databricks_instance = self.secret_client.get_secret("databricks-instance").value
        self.token_url = self.secret_client.get_secret("servicenow-token-url").value
        self.client_id = self.secret_client.get_secret("servicenow-client-id").value
        self.client_secret = self.secret_client.get_secret("servicenow-client-secret").value
        self.username = self.secret_client.get_secret("servicenow-username").value
        self.password = self.secret_client.get_secret("servicenow-password").value

        logger.info("Secrets loaded from Key Vault.")

    def _ensure_table_exists(self):
        self.spark.sql(f"USE CATALOG {self.catalog_name}")
        self.spark.sql(f"USE SCHEMA {self.volume_name}")

        if not self.spark.catalog.tableExists(self.table_name):
            logger.info("Creating new Delta table for processed incidents.")
            self.spark.createDataFrame([], "file_name STRING, file_hash STRING, processed_at TIMESTAMP, job_id STRING, job_run_id STRING, incident_number STRING, incident_created_at TIMESTAMP, incident_created_by STRING") \
                .write.format("delta").saveAsTable(self.full_table_name)

    def get_service_now_token(self):
        payload = {
            'grant_type': 'password',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'username': self.username,
            'password': self.password
        }
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        response = requests.post(self.token_url, data=payload, headers=headers)
        response.raise_for_status()
        return response.json()['access_token']

    def get_job_metadata(self, job_run_id):
        url = f"https://{self.databricks_instance}/api/2.0/jobs/runs/get"
        response = requests.get(url, headers={"Authorization": f"Bearer {self.databricks_token}"}, params={"run_id": job_run_id})
        response.raise_for_status()
        return response.json()

    def get_task_logs(self, run_id):
        url = f"https://{self.databricks_instance}/api/2.0/jobs/runs/get-output"
        response = requests.get(url, headers={"Authorization": f"Bearer {self.databricks_token}"}, params={"run_id": run_id})
        response.raise_for_status()
        logs = response.json()
        return logs.get("notebook_output", {}).get("result", "No notebook output found."), logs.get("error", "No error found.")

    def post_incident(self, payload, token):
        incident_url = "https://'service_now_url'.service-now.com/api/now/table/incident"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        response = requests.post(incident_url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        result = response.json().get("result", {})
        return result.get("task_effective_number", None)

    def compute_file_hash(self, file_path):
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def is_file_processed(self, file_hash):
        df = self.spark.read.table(self.full_table_name).filter(col("file_hash") == file_hash)
        return df.count() > 0

    def log_processed_file(self, file_name, file_hash, job_id, job_run_id, incident_number):
        self.spark.createDataFrame([(
            file_name, file_hash, datetime.now(), job_id, job_run_id,
            incident_number, datetime.now(), "pipeline_script"
        )], [
            "file_name", "file_hash", "processed_at", "job_id", "job_run_id",
            "incident_number", "incident_created_at", "incident_created_by"
        ]).write.mode("append").format("delta").saveAsTable(self.full_table_name)

    def process_file(self, file_path, token):
        logger.info(f"Processing file: {file_path}")
        file_hash = self.compute_file_hash(file_path)
        file_name = os.path.basename(file_path)

        if self.is_file_processed(file_hash):
            logger.info(f"Skipping already processed file: {file_name}")
            return

        try:
            with open(file_path, "r") as json_file:
                file_data = json.load(json_file)
                job_run_id = file_data.get('job_run_id')
                job_id = file_data.get('job_id')
                job_name = file_data.get('job_name', 'Unknown Job Name')

            if not job_run_id:
                raise ValueError(f"Missing 'job_run_id' in file {file_path}")

            job_metadata = self.get_job_metadata(job_run_id)
            job_name = job_name if job_name != 'Unknown Job Name' else job_metadata.get("job_name", "Unknown Job Name")
            tasks = job_metadata.get("tasks", [])
            failed_tasks = []

            for task in tasks:
                task_run_id = task.get("run_id")
                task_key = task.get("task_key")
                task_state = task.get("state")
                task_name = task.get("task_name", task_key)

                if task_state.get("life_cycle_state") == "TERMINATED":
                    retries = 3
                    for attempt in range(retries):
                        try:
                            notebook_output, error = self.get_task_logs(task_run_id)
                            description = f"Task {task_name} ({task_key}) logs:\n{notebook_output}\n"
                            if error:
                                description += f"Error: {error}"
                            failed_tasks.append(description)
                            break
                        except Exception as e:
                            if attempt < retries - 1:
                                time.sleep(5)
                            else:
                                failed_tasks.append(f"Task: {task_name} ({task_key}), Error: {str(e)}")

            if failed_tasks:
                combined_error_message = "\n".join(failed_tasks)
                payload = {
                    "urgency": "2",
                    "impact": "3",
                    "short_description": f"Databricks Job {job_name} ({job_id}) Failed",
                    "caller_id": "sddc_user",
                    "assignment_group": "your_assignement_group",
                    "description": f"Job {job_name} (ID: {job_id}, Run ID: {job_run_id}) failed:\n\n{combined_error_message}",
                    "correlation_id": job_run_id,
                    "correlation_display": f"databricks_job_failure_{job_run_id}"
                }
                incident_number = self.post_incident(payload, token)
                self.log_processed_file(file_name, file_hash, job_id, job_run_id, incident_number)

            os.remove(file_path)
            logger.info(f"File {file_path} deleted successfully.")

        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")

    def process_all_files(self):
        token = self.get_service_now_token()
        files = [os.path.join(self.destination_directory, f) for f in os.listdir(self.destination_directory) if f.endswith(".json")]
        files.sort(key=lambda f: os.path.getctime(f))

        for file_path in files:
            try:
                self.process_file(file_path, token)
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {e}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("IncidentProcessingPipeline").getOrCreate()

    processor = IncidentProcessor(
        spark=spark,
        key_vault_url="your_key_vault_url",
        table_config={
            "catalog": "your_catalogue",
            "schema": "your_Schema",
            "table": "processed_incidents"
        },
        destination_directory="your_destination_directory_to_store_files"
    )

    processor.process_all_files()

