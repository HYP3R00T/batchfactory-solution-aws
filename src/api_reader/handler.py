"""
API Reader Lambda - Returns job status from DynamoDB.

Invoked by API Gateway for GET /jobs/{id} requests.
- Returns 200 with job data if found
- Returns 404 if job not found
- Returns 400 if id parameter is missing
"""

import os
import json
from decimal import Decimal
import boto3

# Setup
dynamodb = boto3.resource("dynamodb")
JOBS_TABLE = os.environ.get("JOBS_TABLE")


class DecimalEncoder(json.JSONEncoder):
    """JSON encoder that converts Decimal -> int/float for JSON serialisation.

    DynamoDB represents numbers as Decimal which json.dumps cannot serialize by default.
    This encoder converts Decimal objects to int if a whole number, otherwise float.
    """

    def default(self, obj):
        if isinstance(obj, Decimal):
            # If decimal is integer-valued, convert to int to keep JSON tidy
            if obj % 1 == 0:
                return int(obj)
            return float(obj)
        return super().default(obj)


def lambda_handler(event, context):
    """Main handler - retrieves job status by ID."""

    table = dynamodb.Table(JOBS_TABLE)

    # Get job ID from path parameter
    path_params = event.get("pathParameters") or {}
    job_id = path_params.get("id")

    if not job_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Missing job ID"})
        }

    # Look up job in DynamoDB
    try:
        response = table.get_item(Key={"jobId": job_id})
        item = response.get("Item")

        if not item:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": "Job not found", "jobId": job_id})
            }

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(item, cls=DecimalEncoder)
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
