"""
Validator Lambda - Validates CSV files and queues them for processing.

Triggered by S3 when a CSV file is uploaded to the uploads/ folder.
- Checks that required columns (id, value, timestamp) exist
- Validates timestamp format (ISO8601 or Unix epoch)
- If valid: sends job to SQS for processing
- If invalid: moves file to rejected/ prefix and marks job as FAILED
"""

import os
import json
import csv
import io
import re
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

# Regex patterns for timestamp validation
ISO8601_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?$"
)
EPOCH_PATTERN = re.compile(r"^\d{10,13}$")  # Unix epoch (seconds or milliseconds)


def get_timestamp():
    """Return current UTC time in ISO format."""
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def is_valid_timestamp(value):
    """Check if value is a valid ISO8601 or Unix epoch timestamp."""
    if not value:
        return False
    value = value.strip()
    return bool(ISO8601_PATTERN.match(value) or EPOCH_PATTERN.match(value))


def move_to_rejected(bucket, source_key, job_id):
    """Move file from uploads/ to rejected/ prefix."""
    filename = os.path.basename(source_key)
    rejected_key = f"rejected/{job_id}/{filename}"

    # Copy to rejected location
    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": source_key},
        Key=rejected_key
    )

    # Delete original
    s3.delete_object(Bucket=bucket, Key=source_key)

    logger.info(f"Moved {source_key} to {rejected_key}")
    return rejected_key


def mark_as_failed(table, job_id, message, bucket=None, source_key=None):
    """Update job status to FAILED and optionally move file to rejected."""
    rejected_key = None
    if bucket and source_key:
        try:
            rejected_key = move_to_rejected(bucket, source_key, job_id)
        except Exception as e:
            logger.error(f"Failed to move file to rejected: {e}")

    update_expr = "SET #s = :s, #m = :m, finishedAt = :f"
    expr_names = {"#s": "status", "#m": "message"}
    expr_values = {
        ":s": "FAILED",
        ":m": message,
        ":f": get_timestamp()
    }

    if rejected_key:
        update_expr += ", rejectedKey = :r"
        expr_values[":r"] = rejected_key

    table.update_item(
        Key={"jobId": job_id},
        UpdateExpression=update_expr,
        ExpressionAttributeNames=expr_names,
        ExpressionAttributeValues=expr_values
    )


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
        mark_as_failed(table, job_id, f"Could not read file: {e}")
        return {"status": "failed", "jobId": job_id}

    # Validate CSV has required columns
    reader = csv.DictReader(io.StringIO(content))
    headers = reader.fieldnames or []

    missing = [col for col in REQUIRED_COLUMNS if col not in headers]
    if missing:
        logger.error(f"Missing columns: {missing}")
        mark_as_failed(
            table, job_id,
            f"Missing required columns: {missing}",
            bucket, key
        )
        return {"status": "failed", "jobId": job_id}

    # Validate timestamp format in first row (sample validation)
    rows = list(csv.DictReader(io.StringIO(content)))
    if rows:
        # Check first non-empty timestamp to validate format
        for row in rows[:5]:  # Check first 5 rows
            ts_value = row.get("timestamp", "").strip()
            if ts_value and not is_valid_timestamp(ts_value):
                logger.error(f"Invalid timestamp format: {ts_value}")
                mark_as_failed(
                    table, job_id,
                    f"Invalid timestamp format: '{ts_value}'. Expected ISO8601 or Unix epoch.",
                    bucket, key
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
