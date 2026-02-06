import json
import boto3

glue = boto3.client("glue")

CRAWLERS = ["airline", "customers"]

def lambda_handler(event, context):

    print("Gold S3 event received")
    print(json.dumps(event))

    for name in CRAWLERS:
        try:
            glue.start_crawler(Name=name)
            print(f"Started crawler: {name}")
        except glue.exceptions.CrawlerRunningException:
            print(f"{name} already running")

    return {
        "status": "CRAWLERS_STARTED"
    }
