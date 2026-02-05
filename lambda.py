import json
import time
import os
import boto3

glue = boto3.client("glue")

GLUE_JOB = os.environ.get("GLUE_JOB", "FinalGlue")
CRAWLERS = ["airline", "customers"]


def wait_glue(job, run_id):
    while True:
        r = glue.get_job_run(JobName=job, RunId=run_id)
        s = r["JobRun"]["JobRunState"]
        print("Glue state:", s)

        if s == "SUCCEEDED":
            return
        if s in ["FAILED", "STOPPED", "TIMEOUT"]:
            raise Exception(f"Glue failed: {s}")

        time.sleep(30)


def wait_crawler(name):
    while True:
        s = glue.get_crawler(Name=name)["Crawler"]["State"]
        print(name, s)
        if s == "READY":
            return
        time.sleep(20)


def lambda_handler(event, context):
    print("Event:", json.dumps(event))

    run = glue.start_job_run(JobName=GLUE_JOB)
    rid = run["JobRunId"]

    wait_glue(GLUE_JOB, rid)

    for c in CRAWLERS:
        glue.start_crawler(Name=c)
        wait_crawler(c)

    return {"status": "done"}
