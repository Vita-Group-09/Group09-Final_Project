import json
import time
import boto3

cf = boto3.client("cloudformation")
glue = boto3.client("glue")

GLUE_STACK = "glue-cicd-stack"
CRAWLER_STACK = "glue-crawler-stack"

GLUE_JOB_NAME = "FinalGlue"

def wait_stack(stack):
    while True:
        s = cf.describe_stacks(StackName=stack)["Stacks"][0]["StackStatus"]
        print(f"Stack {stack}: {s}")

        if s.endswith("_COMPLETE"):
            return

        if "FAILED" in s or "ROLLBACK" in s:
            raise Exception(f"Stack failed: {s}")

        time.sleep(15)


def wait_glue(job, run_id):
    while True:
        r = glue.get_job_run(JobName=job, RunId=run_id)
        s = r["JobRun"]["JobRunState"]
        print("Glue:", s)

        if s == "SUCCEEDED":
            return

        if s in ["FAILED","STOPPED","TIMEOUT"]:
            raise Exception("Glue failed")

        time.sleep(30)


def wait_crawler(name):
    while True:
        s = glue.get_crawler(Name=name)["Crawler"]["State"]
        print(name, s)
        if s == "READY":
            return
        time.sleep(20)


def lambda_handler(event, context):

    print("Triggered by S3")
    print(json.dumps(event))

    # ==============================
    # Deploy / Update Glue CFT
    # ==============================
    cf.deploy_stack = cf.create_stack  # safe alias if needed

    try:
        cf.update_stack(
            StackName=GLUE_STACK,
            UsePreviousTemplate=True,
            Capabilities=['CAPABILITY_NAMED_IAM']
        )
    except cf.exceptions.ClientError as e:
        if "No updates" in str(e):
            print("No Glue stack updates")
        else:
            raise

    wait_stack(GLUE_STACK)

    # ==============================
    # Run Glue Job
    # ==============================
    run = glue.start_job_run(JobName=GLUE_JOB_NAME)
    wait_glue(GLUE_JOB_NAME, run["JobRunId"])

    # ==============================
    # Deploy Crawlers CFT
    # ==============================
    try:
        cf.update_stack(
            StackName=CRAWLER_STACK,
            UsePreviousTemplate=True,
            Capabilities=['CAPABILITY_NAMED_IAM']
        )
    except cf.exceptions.ClientError as e:
        if "No updates" in str(e):
            print("No crawler updates")
        else:
            raise

    wait_stack(CRAWLER_STACK)

    # ==============================
    # Run Crawlers
    # ==============================
    for c in ["airline","customers"]:
        try:
            glue.start_crawler(Name=c)
        except:
            pass
        wait_crawler(c)

    return {"status": "SUCCESS"}
