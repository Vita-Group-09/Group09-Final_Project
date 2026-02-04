import json
import boto3
import os

sf = boto3.client("stepfunctions")

def lambda_handler(event, context):
    print("EVENT:", json.dumps(event))

    try:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        response = sf.start_execution(
            stateMachineArn=os.environ["STEP_FUNCTION_ARN"],
            input=json.dumps({
                "trigger": "s3",
                "bucket": bucket,
                "key": key
            })
        )

        print("Step Function started:", response)

    except Exception as e:
        print("ERROR starting Step Function:", str(e))
        raise e

    return {"status": "ok"}
