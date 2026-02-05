import json
import time
import boto3
import os

glue = boto3.client("glue")

GLUE_JOB = os.environ.get("GLUE_JOB_NAME", "FinalGlue")
CRAWLERS = os.environ.get("CRAWLERS", "airline,customers").split(",")


# ==============================
# Glue wait helper
# ==============================
def wait_for_glue(job_name, run_id):
    while True:
        r = glue.get_job_run(JobName=job_name, RunId=run_id)
        state = r["JobRun"]["JobRunState"]

        print(f"Glue state: {state}")

        if state == "SUCCEEDED":
            return

        if state in ["FAILED", "STOPPED", "TIMEOUT"]:
            raise Exception(f"Glue failed: {state}")

        time.sleep(30)


# ==============================
# Crawler helper
# ==============================
def run_crawler(name):
    try:
        glue.start_crawler(Name=name)
        print(f"Crawler started: {name}")
    except glue.exceptions.CrawlerRunningException:
        print(f"{name} already running")

    while True:
        c = glue.get_crawler(Name=name)
        state = c["Crawler"]["State"]

        if state == "READY":
            status = c["Crawler"]["LastCrawl"]["Status"]
            print(f"{name} crawl result: {status}")

            if status != "SUCCEEDED":
                raise Exception(f"{name} crawler failed")

            return

        time.sleep(20)


# ==============================
# Lambda handler
# ==============================
def lambda_handler(event, context):

    print("S3 Trigger received")
    print(json.dumps(event))

    # ---- Start Glue ----
    job = glue.start_job_run(JobName=GLUE_JOB)
    run_id = job["JobRunId"]
    print("Glue started:", run_id)

    wait_for_glue(GLUE_JOB, run_id)

    # ---- Run Crawlers ----
    for c in CRAWLERS:
        run_crawler(c.strip())

    return {
        "status": "SUCCESS",
        "message": "Glue + Crawlers completed"
    }
