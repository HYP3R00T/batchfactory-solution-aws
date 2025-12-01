import os
import json
import csv
import io
import uuid
import datetime
import urllib.parse

import boto3
import logging

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

# Basic logger so errors and info appear in CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

JOBS_TABLE = os.environ.get("JOBS_TABLE")
INGEST_BUCKET = os.environ.get("INGEST_BUCKET")
PROCESSED_PREFIX = os.environ.get("PROCESSED_PREFIX", "processed/")

REQUIRED_COLUMNS = ["id", "value", "timestamp"]


def now_iso():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def write_job_record(table, item):
    table.put_item(Item=item)


def update_job_record(table, job_id, updates: dict):
    # Convert updates into UpdateExpression using ExpressionAttributeNames
    expr = []
    attr_vals = {}
    attr_names = {}
    for i, (k, v) in enumerate(updates.items()):
        name_tok = f"#k{i}"
        val_tok = f":v{i}"
        expr.append(f"{name_tok} = {val_tok}")
        attr_names[name_tok] = k
        attr_vals[val_tok] = v

    update_expression = "SET " + ", ".join(expr)
    table.update_item(
        Key={"jobId": job_id},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=attr_vals,
        ExpressionAttributeNames=attr_names,
    )


def validate_and_process_csv(body_text):
    reader = csv.DictReader(io.StringIO(body_text))
    header = reader.fieldnames or []
    missing = [c for c in REQUIRED_COLUMNS if c not in header]
    if missing:
        return {"ok": False, "message": f"Missing required columns: {missing}", "row_count": 0, "error_count": 0, "records": []}

    valid_records = []
    row_count = 0
    error_count = 0

    for row in reader:
        row_count += 1
        # basic per-row validation
        if not row.get("id") or not row.get("value") or not row.get("timestamp"):
            error_count += 1
            continue
        # keep as-is; consumers can interpret timestamp
        valid_records.append({"id": row.get("id"), "value": row.get("value"), "timestamp": row.get("timestamp")})

    return {"ok": True, "message": "Processed", "row_count": row_count, "error_count": error_count, "records": valid_records}


def lambda_handler(event, context):
    table = dynamodb.Table(JOBS_TABLE)
    started_at = now_iso()

    # Try to extract S3 info from event and derive job_id from file name
    bucket = None
    key = None
    s3_source = None
    try:
        record = event.get("Records", [])[0]
        s3rec = record.get("s3", {})
        bucket = s3rec.get("bucket", {}).get("name")
        key = urllib.parse.unquote_plus(s3rec.get("object", {}).get("key"))
        s3_source = f"s3://{bucket}/{key}"
    except Exception:
        bucket = None
        key = None
        s3_source = None

    # Use file basename (without extension) as jobId; fallback to uuid if unavailable
    if key:
        job_id = os.path.splitext(os.path.basename(key))[0]
    else:
        job_id = str(uuid.uuid4())

    # Create a base job record
    job_item = {
        "jobId": job_id,
        "status": "VALIDATING",
        "s3Source": s3_source,
        "s3OutputPrefix": None,
        "rowCount": 0,
        "errorCount": 0,
        "startedAt": started_at,
        "finishedAt": None,
        "message": ""
    }

    # persist initial job
    try:
        write_job_record(table, job_item)
    except Exception as e:
        logger.exception("Failed to write initial job record")
        return {"status": "error", "message": f"Failed to write initial job record: {e}"}

    if not bucket or not key:
        logger.error("Invalid S3 event payload: missing bucket or key, event=%s", event)
        update_job_record(table, job_id, {"status": "FAILED", "finishedAt": now_iso(), "message": "Invalid S3 event payload"})
        return {"status": "failed", "jobId": job_id}

    # Read object
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read().decode("utf-8")
    except Exception as e:
        logger.exception("Failed to read S3 object")
        update_job_record(table, job_id, {"status": "FAILED", "finishedAt": now_iso(), "message": f"Failed to read S3 object: {e}"})
        return {"status": "failed", "jobId": job_id}

    # Validate and process
    try:
        result = validate_and_process_csv(body)
    except Exception as e:
        logger.exception("Processing error")
        update_job_record(table, job_id, {"status": "FAILED", "finishedAt": now_iso(), "message": f"Processing error: {e}"})
        return {"status": "failed", "jobId": job_id}

    # Write processed output if any valid records
    output_prefix = f"{PROCESSED_PREFIX}{job_id}/"
    output_key = f"{output_prefix}output.json"
    try:
        if result["row_count"] > 0 and len(result["records"]) > 0:
            s3.put_object(Bucket=bucket, Key=output_key, Body=json.dumps(result["records"]).encode("utf-8"), ContentType="application/json")
        # Update job
        status = "COMPLETED" if result["error_count"] == 0 else "COMPLETED_WITH_ERRORS"
        update_job_record(table, job_id, {
            "status": status,
            "s3OutputPrefix": output_prefix,
            "rowCount": result["row_count"],
            "errorCount": result["error_count"],
            "finishedAt": now_iso(),
            "message": result.get("message", "")
        })
    except Exception as e:
        logger.exception("Failed to write processed output or update job")
        update_job_record(table, job_id, {"status": "FAILED", "finishedAt": now_iso(), "message": f"Failed to write processed output or update job: {e}"})
        return {"status": "failed", "jobId": job_id}

    return {"status": "ok", "jobId": job_id, "rowCount": result["row_count"], "errorCount": result["error_count"]}
