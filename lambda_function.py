import json
import time
import boto3
import os

glue = boto3.client("glue")

GLUE_JOB = os.environ.get("GLUE_JOB_NAME", "FinalGlue")
CRAWLERS = os.environ.get("CRAWLER_NAMES", "airline,customers").split(",")


# ================================
# Wait for Glue Job
# ================================
def wait_for_glue(job_name, run_id):
    while True:
        res = glue.get_job_run(
            JobName=job_name,
            RunId=run_id
        )

        state = res["JobRun"]["JobRunState"]
        print(f"Glue state = {state}")

        if state == "SUCCEEDED":
            return True

        if state in ["FAILED", "STOPPED", "TIMEOUT"]:
            raise Exception(f"Glue failed: {state}")

        time.sleep(30)


# ================================
# Run crawler safely
# ================================
def run_crawler(name):
    try:
        glue.start_crawler(Name=name)
        print(f"Started crawler {name}")
    except glue.exceptions.CrawlerRunningException:
        print(f"{name} already running")

    while True:
        s = glue.get_crawler(Name=name)["Crawler"]["State"]
        print(f"{name} state = {s}")
        if s == "READY":
            break
        time.sleep(20)


# ================================
# Lambda handler
# ================================
def lambda_handler(event, context):

    print("Event received:")
    print(json.dumps(event))

    # ---- basic S3 filter guard ----
    try:
        record = event["Records"][0]
        key = record["s3"]["object"]["key"]

        # avoid self-trigger loops
        if key.startswith("scripts/") or key.startswith("gold/"):
            print("Ignoring non-raw upload")
            return {"status": "ignored"}
    except Exception:
        print("Non S3 event — ignored")
        return {"status": "ignored"}

    # ============================
    # Start Glue
    # ============================
    job = glue.start_job_run(JobName=GLUE_JOB)
    run_id = job["JobRunId"]
    print(f"Started Glue run {run_id}")

    wait_for_glue(GLUE_JOB, run_id)

    # ============================
    # Run Crawlers
    # ============================
    for c in CRAWLERS:
        run_crawler(c.strip())

    return {
        "status": "success",
        "glue_run": run_id
    }
