import json
import boto3

glue = boto3.client("glue")

GLUE_JOB_NAME = "FinalGlue"

def lambda_handler(event, context):

    print("Raw S3 event received")
    print(json.dumps(event))

    # start glue job
    resp = glue.start_job_run(JobName=GLUE_JOB_NAME)

    run_id = resp["JobRunId"]
    print(f"Started Glue Job: {run_id}")

    return {
        "status": "GLUE_JOB_STARTED",
        "run_id": run_id
    }
