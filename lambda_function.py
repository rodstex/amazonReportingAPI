"""
Lambda entry point for orchestrating Amazon Advertising reporting.

Responsibilities:
    1. Refresh an OAuth access token for Amazon Ads.
    2. Request an asynchronous report for a given profile/configuration.
    3. Schedule a follow-up job in EventBridge Scheduler to collect the report and
       store it in S3 (or process it downstream).

This function is designed to be invoked either:
    - Directly via EventBridge with constant JSON input, or
    - Indirectly via SNS, where the SNS message is a JSON string with the same keys.
"""

import json
import os
import re
import requests
import datetime
import logging
from typing import Dict, Any

import boto3


############################ Logging ############################
# Configure a module-level logger. Lambda automatically sends stdout/stderr
# to CloudWatch Logs; using structured logging (key=value) makes it easier
# to search and build metrics.
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ---------- Helpers ----------
def get_access_token() -> str:
    """
    Obtain a fresh Amazon Ads access token using the configured OAuth client.

    Required environment variables:
        - grant_type
        - client_id
        - client_secret
        - refresh_token
        - refresh_token_url
    """
    logger.info("Refreshing Amazon Ads access token")

    payload = {
        "grant_type": os.environ.get('grant_type'),
        "client_id": os.environ.get('client_id'),
        "client_secret": os.environ.get('client_secret'),
        "refresh_token": os.environ.get('refresh_token'),
    }

    refresh_url = os.environ.get('refresh_token_url')
    if not refresh_url:
        logger.error("Missing required environment variable: refresh_token_url")
        raise RuntimeError("Environment variable 'refresh_token_url' must be set")

    try:
        r = requests.post(url=refresh_url, data=payload, timeout=10)
    except Exception as exc:
        logger.exception("Failed to call token endpoint", extra={"refresh_token_url": refresh_url})
        raise RuntimeError("Error while requesting access token") from exc

    if r.status_code == 200:
        token = r.json().get("access_token")
        if not token:
            logger.error("Token endpoint succeeded but no 'access_token' in response")
            raise RuntimeError("Missing 'access_token' in token response")

        logger.info("Successfully refreshed access token")
        return token

    logger.error(
        "Failed to refresh access token",
        extra={
            "status_code": r.status_code,
            "response_body": r.text[:500],  # avoid flooding logs
        },
    )
    raise RuntimeError(f"Failed to refresh access token, status_code={r.status_code}")

def request_report(
    access_token: str, 
    start_date: str, 
    end_date: str,
    endpoint_url: str,
    report_name: str,
    profile_id: str,
    configuration_json: str
) -> dict:

    headers = {
        'Content-Type': 'application/vnd.createasyncreportrequest.v3+json',
        'Amazon-Advertising-API-ClientId': os.environ.get('client_id'),
        'Amazon-Advertising-API-Scope': f'{profile_id}',
        'Authorization': f'Bearer {access_token}',
    }
    payload = {
        "name": report_name,
        "startDate": start_date,
        "endDate": end_date,
        "configuration": configuration_json
    }

    logger.info(
        "Requesting Amazon Ads report",
        extra={
            "endpoint_url": endpoint_url,
            "report_name": report_name,
            "profile_id": profile_id,
            "start_date": start_date,
            "end_date": end_date,
        },
    )

    try:
        r = requests.post(url=endpoint_url, headers=headers, json=payload, timeout=15)
    except Exception as exc:
        logger.exception(
            "Error while requesting report",
            extra={"endpoint_url": endpoint_url, "profile_id": profile_id},
        )
        raise RuntimeError("Network error while requesting report") from exc

    if r.status_code != 200:
        if (r.status_code == 425) or r.json()['code'] == '425':

            input_str = r.json()["detail"]
            pattern = r"The Request is a duplicate of : (.+)"

            # Using re.search to find the match
            match = re.search(pattern, input_str)
            if match:
                report_id = match.group(1)
                logger.info(
                    "Detected duplicate report request; reusing existing report_id",
                    extra={"report_id": report_id},
                )
        else:
            # Log full error details for troubleshooting.
            try:
                error_body = r.json()
            except Exception:
                error_body = {"raw": r.text}

            logger.error(
                "Report request failed",
                extra={
                    "status_code": r.status_code,
                    "error_body": error_body,
                },
            )
            raise ValueError(
                f"Report request failed. Status: {r.status_code}, Body: {error_body}"
            )
    else:
        report_id = r.json()['reportId']
        logger.info("Report request accepted", extra={"report_id": report_id})

    return {
        'status': 200,
        'report_id': report_id
    }

def generate_report_inputs(bucket_name:str, file_key:str, time_key:str, profile_id:str, field_names:str, report_id:str) -> dict:
    """
    Build the payload that will be used as constant input for the scheduled job
    that collects the generated report.
    """
    return {
        'bucket_name': bucket_name,
        'file_key': file_key,
        'time_key': time_key,
        'profile_id': profile_id,
        'field_names': field_names,
        'report_id': report_id
    }

def job_scheduler(schedule_name:str, schedule_group:str, target_arn:str, role_arn:str, constant_input:str) -> dict:
    """
    Create or update an EventBridge schedule that will periodically invoke the
    downstream job responsible for collecting the Amazon Ads report.

    The schedule:
        - Runs every 15 minutes.
        - Starts in 1 hour from "now" (UTC).
        - Ends 2 hours after the start (i.e., 1 hour window of retries).

    Args:
        schedule_name: Unique name of the EventBridge schedule.
        schedule_group: Group name for schedule organization.
        target_arn: ARN of the target (e.g., Lambda) that will collect the report.
        role_arn: IAM role assumed by EventBridge Scheduler to invoke the target.
        constant_input: JSON-serializable payload used as constant input for the target.
    """

    # Create or update a schedule that runs every 15 minutes with a bounded window.
    # Note: use FlexibleTimeWindow='OFF' for exact fire times.
    scheduler = boto3.client("scheduler")

    now = datetime.datetime.now(datetime.timezone.utc)
    start_dt = (now + datetime.timedelta(hours=1)).replace(microsecond=0)
    end_dt   = (start_dt + datetime.timedelta(hours=2))

    # Log the exact scheduler configuration for future debugging.
    logger.info(
        "Updating EventBridge schedule",
        extra={
            "schedule_name": schedule_name,
            "schedule_group": schedule_group,
            "target_arn": target_arn,
            "role_arn": role_arn,
            "start_utc": start_dt.isoformat(),
            "end_utc": end_dt.isoformat(),
            "constant_input": constant_input,
        },
    )

    scheduler.update_schedule(
        Name=schedule_name,
        GroupName=schedule_group,
        ScheduleExpression="rate(15 minutes)",
        FlexibleTimeWindow={"Mode": "OFF"},
        StartDate=start_dt,
        EndDate=end_dt,
        Target={
            "Arn": target_arn,
            "RoleArn": role_arn,
            "Input": json.dumps(constant_input),
            # Optional: set DeadLetterConfig, RetryPolicy, etc.
        },
        State="ENABLED",
        Description=f"Runs every 15m from {start_dt} to {end_dt} (UTC)"
    )

    logger.info(
        "EventBridge schedule updated successfully",
        extra={
            "schedule_name": schedule_name,
            "schedule_group": schedule_group,
        },
    )

    return {
        "status": "updated_scheduler",
        "schedule_name": schedule_name,
        "group": schedule_group,
        "start_utc": start_dt.isoformat(),
        "end_utc": end_dt.isoformat()
    }


def _resolve_inputs(event: dict) -> Dict[str, Any]:
    """
    Normalize incoming events into a single dictionary of inputs.

    Supports:
      1) EventBridge with constant input (event is already the JSON payload)
      2) SNS -> Lambda (event["Records"][0]["Sns"]["Message"] is a JSON string
         with the same keys)
    """
    logger.info(
        "Resolving event inputs",
        extra={
            "raw_event_has_records": bool(event.get("Records")) if isinstance(event, dict) else False
        },
    )

    # SNS?
    if isinstance(event, dict) and event.get("Records"):
        rec0 = event["Records"][0]
        # SNS envelope
        if "Sns" in rec0 and "Message" in rec0["Sns"]:
            msg = rec0["Sns"]["Message"]
            try:
                payload = json.loads(msg)
            except Exception as e:
                logger.exception(
                    "SNS Message is not valid JSON",
                    extra={"message_snippet": str(msg)[:200]},
                )
                raise ValueError("SNS Message must be JSON with the required keys.") from e

            source_payload = payload
        else:
            raise ValueError("Unsupported event record type. Expecting SNS.")
    else:
        # EventBridge constant input (already the object you need)
        if not isinstance(event, dict):
            raise ValueError("EventBridge event must be a JSON object.")
        source_payload = event

    # Extract required parameters
    required_keys = [
        "endpoint_url",
        "bucket_name",
        "file_key",
        "report_name",
        "profile_id",
        "field_names",
        "configuration_json",
        "schedule_name",
        "schedule_group",
        "target_arn",
        "role_arn",
    ]

    result: Dict[str, Any] = {}
    missing = []
    for key in required_keys:
        value = source_payload.get(key)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing.append(key)
        result[key] = value

    if missing:
        logger.error(
            "Missing required input keys",
            extra={
                "missing_keys": missing,
                "payload_snippet": {k: source_payload.get(k) for k in required_keys},
            },
        )
        raise ValueError(f"Missing required keys in event payload: {', '.join(missing)}")

    logger.info(
        "Successfully resolved inputs",
        extra={
            "bucket_name": result.get("bucket_name"),
            "file_key": result.get("file_key"),
            "schedule_name": result.get("schedule_name"),
            "schedule_group": result.get("schedule_group"),
            "profile_id": result.get("profile_id"),
        },
    )

    return result

############################ Lambda Handler ############################

def lambda_handler(event, context):
    """
    Lambda handler.

    Steps:
        1. Resolve and validate event inputs (SNS or EventBridge).
        2. Refresh Amazon Ads access token.
        3. Compute reporting date window.
        4. Request asynchronous report.
        5. Schedule a follow-up job to collect the report.
    """
    logger.info("Lambda invocation started")

    # Step 1 - Grab Event Params
    income_inputs = _resolve_inputs(event)

    # Event-driven parameters
    endpoint_url = income_inputs.get("endpoint_url")
    bucket_name = income_inputs.get("bucket_name")
    file_key = income_inputs.get("file_key")
    report_name = income_inputs.get("report_name")
    profile_id = income_inputs.get("profile_id")
    field_names = income_inputs.get("field_names")
    configuration_json = income_inputs.get("configuration_json")
    schedule_name = income_inputs.get("schedule_name")
    schedule_group = income_inputs.get("schedule_group")
    target_arn = income_inputs.get("target_arn")
    role_arn = income_inputs.get("role_arn")

    # Step 2 - Refresh the Access Token
    logger.info("Obtaining access token for Amazon Ads API")
    access_token = get_access_token()
    
    # Step 3 - Define Report Time Window
    start_date = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    end_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    time_key = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    logger.info(
        "Computed report time window and time_key",
        extra={
            "start_date": start_date,
            "end_date": end_date,
            "time_key": time_key,
        },
    )
    
    # Step 4 - Request Report
    res = request_report(
        access_token=access_token, 
        start_date=start_date, 
        end_date=end_date,
        endpoint_url=endpoint_url,
        report_name=report_name,
        profile_id=profile_id,
        configuration_json=configuration_json
    )

    # Check Report Status
    if res['status']==200:
        report_id = res['report_id']
        logger.info("Report request completed with status 200", extra={"report_id": report_id})

        # Generate Report Collection Input
        job_input = generate_report_inputs(bucket_name, file_key, time_key, profile_id, field_names, report_id)
        logger.info("Generated job input for scheduler", extra={"job_input": job_input})

        # Schedule Report Collection
        schedule_res = job_scheduler(schedule_name, schedule_group, target_arn, role_arn, job_input)
        logger.info(
            "Scheduler configured for report collection",
            extra={
                "schedule_name": schedule_res["schedule_name"],
                "start_utc": schedule_res["start_utc"],
                "end_utc": schedule_res["end_utc"],
            },
        )

        return {
            'run_status': 'OK',
            'schedule_name': schedule_res['schedule_name'],
        }
    
    else:
        logger.error("Report request did not return status 200", extra={"response": res})
        return {
            'run_status': 'FAILED'
        }
