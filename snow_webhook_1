import json
from datetime import datetime

# Set up widgets to capture parameters from Databricks
dbutils.widgets.text("job_id", "", "Job ID")
dbutils.widgets.text("job_run_id", "", "Job Run ID")
dbutils.widgets.text("workspace_id", "", "Workspace ID")
dbutils.widgets.text("job_name", "", "Job Name")  # Adding widget for job name

# Retrieve the values from the widgets
job_id = dbutils.widgets.get("job_id")
job_run_id = dbutils.widgets.get("job_run_id")
workspace_id = dbutils.widgets.get("workspace_id")
job_name = dbutils.widgets.get("job_name")  # Get job name passed as a parameter

# Function to store failure details in DBFS (with specified folder path)
def store_failure_metadata(job_id, run_id, workspace_id):
    # Use the folder path you provided
    folder_path = "dbfs/path/service_now_dump" #replace you dbfs folder path
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Prepare the failure metadata as a JSON object
    failure_metadata = {
        "job_id": job_id,
        "job_run_id": job_run_id,
        "workspace_id": workspace_id,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "job_name": job_name
    }
    
    # Store the failure metadata as a JSON file in the specified folder
    failure_metadata_file_path = f"{folder_path}failure_metadata_{job_name}_{job_run_id}_{timestamp}.json"
    print(failure_metadata)
    # Save failure metadata to the file
    with open(failure_metadata_file_path, 'w') as json_file:
        json.dump(failure_metadata, json_file, indent=4)

    return failure_metadata_file_path

# Call the function to store failure metadata
log_file = store_failure_metadata(job_id, job_run_id, workspace_id)
print(f"Failure metadata stored in: {log_file}")
