import boto3
from botocore.exceptions import ClientError

REGION = "ap-south-1"
BUCKET = "airport-airline-operations-analytics-platform"
SCRIPT_KEY = "scripts/glue_job.py"

GLUE_JOB_NAME = "Final Glue"

AIRLINE_DB = "airline"
CUSTOMERS_DB = "customers"

AIRLINE_CRAWLER = "airline"
CUSTOMERS_CRAWLER = "customers"

AIRLINE_PATH = f"s3://{BUCKET}/silver/airline/"
CUSTOMERS_PATH = f"s3://{BUCKET}/silver/customers/"

GLUE_JOB_ROLE = "glue-job-role"
GLUE_CRAWLER_ROLE = "glue-crawler-role"

iam = boto3.client("iam", region_name=REGION)
glue = boto3.client("glue", region_name=REGION)


# ---------- IAM ROLE ----------
def get_or_create_role(role_name):
    try:
        role = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument="""{
              "Version": "2012-10-17",
              "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "glue.amazonaws.com"},
                "Action": "sts:AssumeRole"
              }]
            }"""
        )
        print(f"Created role: {role_name}")
        return role["Role"]["Arn"]
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            return iam.get_role(RoleName=role_name)["Role"]["Arn"]
        else:
            raise


def attach_policy(role, policy):
    iam.attach_role_policy(RoleName=role, PolicyArn=policy)


# ---------- GLUE JOB ----------
def create_glue_job(role_arn):
    try:
        glue.create_job(
            Name=GLUE_JOB_NAME,
            Role=role_arn,
            Command={
                "Name": "glueetl",
                "ScriptLocation": f"s3://{BUCKET}/{SCRIPT_KEY}",
                "PythonVersion": "3"
            },
            GlueVersion="4.0",
            WorkerType="G.1X",
            NumberOfWorkers=2,
            ExecutionProperty={"MaxConcurrentRuns": 1}
        )
        print("Glue Job created")
    except ClientError as e:
        if e.response["Error"]["Code"] == "AlreadyExistsException":
            print("Glue Job already exists")
        else:
            raise


# ---------- DATABASE ----------
def create_database(db):
    try:
        glue.create_database(DatabaseInput={"Name": db})
        print(f"Database created: {db}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "AlreadyExistsException":
            print(f"Database already exists: {db}")
        else:
            raise


# ---------- CRAWLER ----------
def create_crawler(name, role_arn, db, path):
    try:
        glue.create_crawler(
            Name=name,
            Role=role_arn,
            DatabaseName=db,
            Targets={"S3Targets": [{"Path": path}]},
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "DEPRECATE_IN_DATABASE"
            }
        )
        print(f"Crawler created: {name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "AlreadyExistsException":
            print(f"Crawler already exists: {name}")
        else:
            raise


# ---------- MAIN ----------
def main():
    job_role = get_or_create_role(GLUE_JOB_ROLE)
    crawler_role = get_or_create_role(GLUE_CRAWLER_ROLE)

    attach_policy(GLUE_JOB_ROLE, "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole")
    attach_policy(GLUE_JOB_ROLE, "arn:aws:iam::aws:policy/AmazonS3FullAccess")

    attach_policy(GLUE_CRAWLER_ROLE, "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole")
    attach_policy(GLUE_CRAWLER_ROLE, "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")

    create_glue_job(job_role)

    create_database(AIRLINE_DB)
    create_database(CUSTOMERS_DB)

    create_crawler(AIRLINE_CRAWLER, crawler_role, AIRLINE_DB, AIRLINE_PATH)
    create_crawler(CUSTOMERS_CRAWLER, crawler_role, CUSTOMERS_DB, CUSTOMERS_PATH)

    print("SDK deployment completed")


if __name__ == "__main__":
    main()
