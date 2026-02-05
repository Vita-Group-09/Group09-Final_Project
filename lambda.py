import json
import time
import boto3

glue = boto3.client("glue")

GLUE_JOB_NAME = "FinalGlue"
CRAWLERS = ["airline", "customers"]

def wait_for_glue_job(job_name, run_id):
    while True:
        res = glue.get_job_run(
            JobName=job_name,
            RunId=run_id
        )
        state = res["JobRun"]["JobRunState"]
        print(f"Glue job state: {state}")

        if state == "SUCCEEDED":
            return True

        if state in ["FAILED", "STOPPED", "TIMEOUT"]:
            raise Exception(f"Glue job failed: {state}")

        time.sleep(30)

def run_crawler_and_wait(name):
    try:
        glue.start_crawler(Name=name)
        print(f"Started crawler: {name}")
    except glue.exceptions.CrawlerRunningException:
        print(f"{name} already running")

    while True:
        c = glue.get_crawler(Name=name)
        state = c["Crawler"]["State"]
        print(f"{name} crawler state: {state}")

        if state == "READY":
            status = c["Crawler"]["LastCrawl"]["Status"]
            print(f"{name} last crawl status: {status}")

            if status != "SUCCEEDED":
                raise Exception(f"{name} crawler failed")

            return

        time.sleep(20)


def lambda_handler(event, context):
    print("Triggered by S3 upload")
    print(json.dumps(event))

    # ---------- Start Glue Job ----------
    job = glue.start_job_run(JobName=GLUE_JOB_NAME)
    run_id = job["JobRunId"]
    print(f"Started Glue job: {run_id}")

    wait_for_glue_job(GLUE_JOB_NAME, run_id)

    # ---------- Run Crawlers ----------
    for crawler in CRAWLERS:
        run_crawler_and_wait(crawler)

    return {
        "status": "SUCCESS",
        "message": "Glue + Crawlers completed"
    }
