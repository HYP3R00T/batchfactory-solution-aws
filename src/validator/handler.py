"""
Validator Lambda - Validates CSV files and queues them for processing.

Triggered by S3 when a CSV file is uploaded to the uploads/ folder.
- Checks that required columns (id, value, timestamp) exist
- If valid: sends job to SQS for processing
- If invalid: marks job as FAILED
"""

import os
import json
import csv
import io
import boto3
import logging
from datetime import datetime

# Setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
sqs = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb")

# Environment variables
JOBS_TABLE = os.environ["JOBS_TABLE"]
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]

# Required CSV columns per the challenge spec
REQUIRED_COLUMNS = ["id", "value", "timestamp"]


def get_timestamp():
    """Return current UTC time in ISO format."""
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def lambda_handler(event, context):
    """Main handler - validates CSV and queues for processing."""

    # Extract S3 info from the event
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    # Use filename (without extension) as the job ID
    job_id = os.path.splitext(os.path.basename(key))[0]

    logger.info(f"Validating job {job_id}: s3://{bucket}/{key}")

    # Get DynamoDB table
    table = dynamodb.Table(JOBS_TABLE)

    # Create initial job record with VALIDATING status
    table.put_item(Item={
        "jobId": job_id,
        "status": "VALIDATING",
        "s3Source": f"s3://{bucket}/{key}",
        "startedAt": get_timestamp(),
        "message": "Validating CSV structure"
    })

    # Read the CSV file from S3
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
    except Exception as e:
        logger.error(f"Failed to read file: {e}")
        table.update_item(
            Key={"jobId": job_id},
            UpdateExpression="SET #s = :s, #m = :m, finishedAt = :f",
            ExpressionAttributeNames={"#s": "status", "#m": "message"},
            ExpressionAttributeValues={
                ":s": "FAILED",
                ":m": f"Could not read file: {e}",
                ":f": get_timestamp()
            }
        )
        return {"status": "failed", "jobId": job_id}

    # Validate CSV has required columns
    reader = csv.DictReader(io.StringIO(content))
    headers = reader.fieldnames or []

    missing = [col for col in REQUIRED_COLUMNS if col not in headers]
    if missing:
        logger.error(f"Missing columns: {missing}")
        table.update_item(
            Key={"jobId": job_id},
            UpdateExpression="SET #s = :s, #m = :m, finishedAt = :f",
            ExpressionAttributeNames={"#s": "status", "#m": "message"},
            ExpressionAttributeValues={
                ":s": "FAILED",
                ":m": f"Missing required columns: {missing}",
                ":f": get_timestamp()
            }
        )
        return {"status": "failed", "jobId": job_id}

    # CSV is valid - send to SQS for processing
    sqs.send_message(
        QueueUrl=SQS_QUEUE_URL,
        MessageBody=json.dumps({
            "jobId": job_id,
            "bucket": bucket,
            "key": key
        })
    )

    # Update status to PENDING (waiting for processor)
    table.update_item(
        Key={"jobId": job_id},
        UpdateExpression="SET #s = :s, #m = :m",
        ExpressionAttributeNames={"#s": "status", "#m": "message"},
        ExpressionAttributeValues={
            ":s": "PENDING",
            ":m": "Queued for processing"
        }
    )

    logger.info(f"Job {job_id} validated and queued")
    return {"status": "ok", "jobId": job_id}
