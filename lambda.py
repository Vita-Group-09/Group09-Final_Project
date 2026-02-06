import json
import time
import boto3

glue = boto3.client("glue")

GLUE_JOB = "FinalGlue"
CRAWLERS = ["airline", "customers"]


def wait_glue(job, run_id):
    while True:
        s = glue.get_job_run(JobName=job, RunId=run_id)["JobRun"]["JobRunState"]
        print("Glue:", s)

        if s == "SUCCEEDED":
            return

        if s in ["FAILED", "STOPPED", "TIMEOUT"]:
            raise Exception("Glue failed")

        time.sleep(30)


def run_crawler(name):
    try:
        glue.start_crawler(Name=name)
    except glue.exceptions.CrawlerRunningException:
        pass

    while True:
        s = glue.get_crawler(Name=name)["Crawler"]["State"]
        print(name, s)

        if s == "READY":
            return

        time.sleep(20)


def lambda_handler(event, context):
    print("EVENT:", json.dumps(event))

    run = glue.start_job_run(JobName=GLUE_JOB)
    rid = run["JobRunId"]

    wait_glue(GLUE_JOB, rid)

    for c in CRAWLERS:
        run_crawler(c)

    return {"status": "done"}
