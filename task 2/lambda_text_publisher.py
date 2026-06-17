"""
lambda_text_publisher  —  S3-triggered publisher Lambda (deploy in the AWS console).

Cloud equivalent of the example's "Order Publisher". It is triggered when a file
of texts is uploaded to the S3 bucket, parses the texts, and publishes each one
to the RabbitMQ 'filter_queue' so the manager / stream() can dispatch them to
filter workers.

Supported upload formats:
  - CSV with a header  TextID,Text         (e.g. texts.csv)
  - plain .txt with one text per line

Runtime / deployment:
  - Runtime: Python 3.13
  - Layer:   REQUIRED -> the pika layer (python/lib/python3.13/site-packages/pika)
             upload layer_content.zip as a Lambda Layer and attach its ARN.
  - Role:    LabRole (needs S3 read)
  - Trigger: S3 -> "All object create events" on your bucket
  - Timeout: ~30 s (S3 read + RabbitMQ connect + publish)
  - EDIT the RABBIT_HOST / credentials below to match your EC2 broker.

Tip (from the lab): first test WITHOUT the RabbitMQ code (just print the texts),
then add the pika publishing once the broker is reachable.
"""

import boto3
import csv
import io
import json
import pika
from urllib.parse import unquote
from botocore.exceptions import ClientError

# ── EDIT THESE to match your EC2 RabbitMQ broker (ec2_steps.txt) ──────────────
RABBIT_HOST  = "54.89.50.195"
RABBIT_USER  = "user"
RABBIT_PASS  = "password123"
FILTER_QUEUE = "filter_queue"


def lambda_handler(event, context):
    try:
        # Extract bucket + key from the S3 event and decode the key
        bucket = event["Records"][0]["s3"]["bucket"]["name"]
        key    = unquote(event["Records"][0]["s3"]["object"]["key"])
        print(f"Processing object: {key} from bucket: {bucket}")

        # Read the uploaded file from S3
        s3 = boto3.client("s3")
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                msg = f"Object '{key}' not found in bucket '{bucket}'."
                print(msg)
                return {"statusCode": 404, "body": msg}
            raise

        content = response["Body"].read().decode("utf-8")

        # Parse texts from the file
        texts = []
        if key.lower().endswith(".csv"):
            for row in csv.DictReader(io.StringIO(content)):
                texts.append((row.get("TextID") or str(len(texts) + 1),
                              row.get("Text", "")))
        else:
            for i, line in enumerate(content.splitlines()):
                if line.strip():
                    texts.append((str(i + 1), line.strip()))

        # Connect to RabbitMQ and publish each text
        credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
        connection  = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBIT_HOST, credentials=credentials)
        )
        channel = connection.channel()
        channel.queue_declare(queue=FILTER_QUEUE)

        for text_id, text in texts:
            message = json.dumps({"text_id": text_id, "text": text})
            channel.basic_publish(exchange="", routing_key=FILTER_QUEUE, body=message)
            print("Published text:", text_id)

        connection.close()
        return {"statusCode": 200, "body": f"Published {len(texts)} texts to RabbitMQ"}

    except Exception as e:
        print("Error publishing texts:", str(e))
        return {"statusCode": 500, "body": "Error publishing texts: " + str(e)}
