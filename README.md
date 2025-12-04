## Amazon Reporting API Lambda

This repository contains an AWS Lambda function that:

- **Requests an asynchronous Amazon Advertising report** for a given profile and configuration.
- **Schedules a follow-up job** via **Amazon EventBridge Scheduler** to collect the generated report and store it in S3.
- Supports triggers from **SNS** (wrapping the payload) or **EventBridge** (constant input).

### High-level flow

1. **Trigger**: Lambda is invoked either by:
   - An **SNS notification** whose `Message` is a JSON string.
   - An **EventBridge rule** using constant input (same JSON shape as SNS message).
2. **Input resolution**: `_resolve_inputs(event)` normalizes the payload into a single dictionary containing:
   - `endpoint_url`, `bucket_name`, `file_key`, `report_name`, `profile_id`,
     `field_names`, `configuration_json`, `schedule_name`, `schedule_group`,
     `target_arn`, `role_arn`.
3. **Access token**: `get_access_token()` refreshes the Amazon Ads access token using OAuth
   credentials stored in environment variables.
4. **Report request**: `request_report()` calls the Amazon Ads Reporting API to create an
   asynchronous report for the last 7 days (excluding today).
5. **Scheduler**: If the report request is accepted, `job_scheduler()` configures or updates an
   EventBridge schedule that will:
   - Run every 15 minutes for a 2‑hour window.
   - Invoke the target Lambda (or other AWS resource) with `job_input` that contains all
     required information to collect and save the report.

### Environment variables

The Lambda expects the following environment variables to be defined:

- **OAuth / Amazon Ads**
  - `grant_type`
  - `client_id`
  - `client_secret`
  - `refresh_token`
  - `refresh_token_url`

These are used by `get_access_token()` to obtain a fresh bearer token.

### Event payload formats

#### SNS → Lambda

SNS `Message` must be a JSON string with the following keys:

```json
{
  "endpoint_url": "https://advertising-api.amazon.com/reporting/reports",
  "bucket_name": "my-report-bucket",
  "file_key": "amazon/reports/base_prefix",
  "report_name": "my-report-name",
  "profile_id": "1234567890",
  "field_names": "field1,field2,...",
  "configuration_json": { "groupBy": ["campaign"] },
  "schedule_name": "my-scheduler-name",
  "schedule_group": "my-scheduler-group",
  "target_arn": "arn:aws:lambda:us-east-1:123456789012:function:collector",
  "role_arn": "arn:aws:iam::123456789012:role/EventBridgeSchedulerRole"
}
```

#### EventBridge (constant input) → Lambda

The EventBridge rule should use **constant input** with the exact same JSON shape as above
passed as the event object.

### Local development notes

- The Lambda entry point is `lambda_handler(event, context)` in `lambda_function.py`.
- You can test locally by:
  - Creating a virtual environment.
  - Installing dependencies from `requirements.txt`.
  - Manually calling `lambda_handler()` from a small driver script or a REPL with a mock event.

Example (very minimal) local invocation:

```python
from lambda_function import lambda_handler

test_event = {
    "endpoint_url": "...",
    "bucket_name": "...",
    "file_key": "...",
    "report_name": "...",
    "profile_id": "...",
    "field_names": "...",
    "configuration_json": {},
    "schedule_name": "...",
    "schedule_group": "...",
    "target_arn": "...",
    "role_arn": "..."
}

lambda_handler(test_event, None)
```

### Deployment

This function is designed to be deployed as a standard AWS Lambda:

- **Runtime**: Python 3.x (check your current Lambda runtime; 3.10+ recommended).
- **Handler**: `lambda_function.lambda_handler`.
- **IAM permissions**:
  - `logs:*` for CloudWatch Logs.
  - `scheduler:UpdateSchedule` (and related EventBridge Scheduler permissions).
  - Any permissions required by the **target** resource (typically handled by `role_arn`).

Packaging options:

- Via AWS Console (uploading a ZIP with `lambda_function.py` and dependencies).
- Via IaC tools (CloudFormation, SAM, CDK, Terraform, etc.).


