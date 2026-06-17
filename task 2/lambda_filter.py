"""
lambda_filter  —  InsultFilter WORKER Lambda (deploy in the AWS console).

This is the cloud equivalent of the Task-1 InsultFilter worker. It is invoked
(one invocation per text) by the EC2 manager (Exercise 1) or by the stream()
primitive (Exercise 2). Each invocation censors the insults in a single text
and stores the censored result back in S3.

Event format (sent by the manager / stream):
    { "text_id": "1", "text": "Hey idiot, stop being so stupid!" }

Runtime / deployment:
  - Runtime: Python 3.13
  - Layer:   NOT required (only uses boto3, which is built into the runtime)
  - Role:    LabRole (needs S3 read on insults.txt + S3 write on filtered/)
  - Timeout: ~15 s is plenty
  - EDIT the BUCKET constant below to match config.py.
"""

import json
import boto3

# ── EDIT THESE to match config.py ────────────────────────────────────────────
BUCKET          = "sd-insult-filter-axgaam"
INSULTS_KEY     = "insults.txt"
FILTERED_PREFIX = "filtered/"


def _load_insults(s3):
    """Read the insult list (one per line) from S3."""
    raw = s3.get_object(Bucket=BUCKET, Key=INSULTS_KEY)["Body"].read().decode()
    return [line.strip() for line in raw.splitlines() if line.strip()]


def lambda_handler(event, context):
    text    = event.get("text", "")
    text_id = str(event.get("text_id", "unknown"))
    print(f"Filtering text {text_id}: {text!r}")

    s3      = boto3.client("s3")
    insults = _load_insults(s3)

    # Censor every insult (case-insensitive), counting replacements
    censored = 0
    filtered = text
    for insult in insults:
        while insult.lower() in filtered.lower():
            i = filtered.lower().index(insult.lower())
            filtered = filtered[:i] + "CENSORED" + filtered[i + len(insult):]
            censored += 1

    # Save the censored text back to S3
    out_key = f"{FILTERED_PREFIX}{text_id}.txt"
    s3.put_object(Bucket=BUCKET, Key=out_key, Body=filtered.encode())

    print(f"Text {text_id}: {censored} insult(s) censored -> s3://{BUCKET}/{out_key}")
    return {
        "statusCode": 200,
        "body": json.dumps({
            "text_id":  text_id,
            "filtered": filtered,
            "censored": censored,
        }),
    }
