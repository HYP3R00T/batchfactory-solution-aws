"""
Processor Lambda - Converts CSV to JSON and stores results.

Triggered by SQS messages from the Validator Lambda.
- Reads the CSV file from S3
- Converts each row to JSON
- Writes output to processed/<jobId>/output.json
- Updates job record in DynamoDB
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
dynamodb = boto3.resource("dynamodb")

# Environment variables
JOBS_TABLE = os.environ["JOBS_TABLE"]
PROCESSED_PREFIX = os.environ.get("PROCESSED_PREFIX", "processed/")

# Required CSV columns
REQUIRED_COLUMNS = ["id", "value", "timestamp"]


def get_timestamp():
    """Return current UTC time in ISO format."""
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def lambda_handler(event, context):
    """Main handler - processes CSV and converts to JSON."""

    # Get message from SQS
    record = event["Records"][0]
    message = json.loads(record["body"])

    job_id = message["jobId"]
    bucket = message["bucket"]
    key = message["key"]

    logger.info(f"Processing job {job_id}: s3://{bucket}/{key}")

    # Get DynamoDB table
    table = dynamodb.Table(JOBS_TABLE)

    # Update status to PROCESSING
    table.update_item(
        Key={"jobId": job_id},
        UpdateExpression="SET #s = :s, #m = :m",
        ExpressionAttributeNames={"#s": "status", "#m": "message"},
        ExpressionAttributeValues={
            ":s": "PROCESSING",
            ":m": "Converting CSV to JSON"
        }
    )

    # Read CSV from S3
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
        raise

    # Process CSV rows
    reader = csv.DictReader(io.StringIO(content))
    records = []
    error_count = 0

    for row in reader:
        # Check if required fields have values
        if all(row.get(col) for col in REQUIRED_COLUMNS):
            records.append({
                "id": row["id"],
                "value": row["value"],
                "timestamp": row["timestamp"]
            })
        else:
            error_count += 1

    row_count = len(records) + error_count

    # Write JSON output to S3
    output_key = f"{PROCESSED_PREFIX}{job_id}/output.json"
    s3.put_object(
        Bucket=bucket,
        Key=output_key,
        Body=json.dumps(records, indent=2),
        ContentType="application/json"
    )

    # Update job record with results
    status = "COMPLETED"
    message = f"Processed {row_count} rows"
    if error_count > 0:
        message += f" ({error_count} errors)"

    table.update_item(
        Key={"jobId": job_id},
        UpdateExpression="SET #s = :s, #m = :m, rowCount = :r, errorCount = :e, s3OutputPrefix = :o, finishedAt = :f",
        ExpressionAttributeNames={"#s": "status", "#m": "message"},
        ExpressionAttributeValues={
            ":s": status,
            ":m": message,
            ":r": row_count,
            ":e": error_count,
            ":o": f"{PROCESSED_PREFIX}{job_id}/",
            ":f": get_timestamp()
        }
    )

    logger.info(f"Job {job_id} completed: {message}")
    return {"status": "ok", "jobId": job_id, "rowCount": row_count}
