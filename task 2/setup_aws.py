"""
One-time setup: creates the S3 bucket, uploads the insult list, and uploads a set
of sample text files under the texts/ prefix (used by the Lithops exercise 3).

The RabbitMQ broker lives on the EC2 VM (see ec2_steps.txt), so no SQS queues are
created here.

Run once, after configuring AWS Academy credentials:
    python setup_aws.py
"""

import boto3
from config import REGION, BUCKET, INSULTS, INSULTS_KEY, TEXTS_PREFIX

SAMPLE_TEXTS = [
    "Hey idiot, why are you so stupid this morning?",
    "You are a genius, great work today!",
    "Stop being a nerd and go outside.",
    "The weather is nice and calm today.",
    "Don't be a fool, think before you speak.",
    "You dummy, you forgot to close the door again.",
    "This is a perfectly normal and polite sentence.",
    "That was really smart thinking, well done.",
]


def main():
    s3 = boto3.client("s3", region_name=REGION)

    # ── S3 bucket ────────────────────────────────────────────────────────────
    print(f"Creating S3 bucket: {BUCKET}")
    try:
        if REGION == "us-east-1":
            s3.create_bucket(Bucket=BUCKET)
        else:
            s3.create_bucket(
                Bucket=BUCKET,
                CreateBucketConfiguration={"LocationConstraint": REGION},
            )
        print(f"  Created: s3://{BUCKET}")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"  Already exists: s3://{BUCKET}")
    except Exception as e:
        print(f"  WARNING: {e}")

    # ── Insult list ──────────────────────────────────────────────────────────
    print(f"Uploading insult list -> s3://{BUCKET}/{INSULTS_KEY}")
    s3.put_object(Bucket=BUCKET, Key=INSULTS_KEY, Body="\n".join(INSULTS).encode())

    # ── Sample text files (for Lithops exercise 3) ───────────────────────────
    print(f"Uploading {len(SAMPLE_TEXTS)} sample text files -> s3://{BUCKET}/{TEXTS_PREFIX}")
    for i, text in enumerate(SAMPLE_TEXTS):
        key = f"{TEXTS_PREFIX}text_{i:03d}.txt"
        s3.put_object(Bucket=BUCKET, Key=key, Body=text.encode())
        print(f"  {key}")

    print("\nSetup complete.")
    print("For Exercise 1, upload texts.csv to the bucket root to trigger "
          "lambda_text_publisher (after you have deployed it and wired the S3 trigger).")


if __name__ == "__main__":
    main()
