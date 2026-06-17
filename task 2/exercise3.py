"""
Exercise 3 — Lithops map/reduce: filter insults across S3 text files.

Architecture
------------
  S3 (texts/ prefix)  ──map──►  Lambda per file: censor insults, write back to S3
                      ◄─reduce─  sum total censored insults

Output
------
  - Filtered files saved to s3://BUCKET/filtered/
  - Total count of censored insults printed at the end

Usage
-----
  python exercise3.py
"""

import boto3
import lithops
from config import REGION, BUCKET, INSULTS_KEY, TEXTS_PREFIX, FILTERED_PREFIX


# ── Map function: one Lambda invocation per file ──────────────────────────────

def filter_file(key):
    """
    Read a text file from S3, replace every insult with CENSORED,
    write the filtered file back to S3 under filtered/ prefix.
    Returns the number of insults censored in this file.
    """
    import boto3 as _boto3

    s3 = _boto3.client("s3")

    # Read source text
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode()

    # Read insult list
    raw     = s3.get_object(Bucket=BUCKET, Key=INSULTS_KEY)["Body"].read().decode()
    insults = [line.strip() for line in raw.splitlines() if line.strip()]

    # Censor (case-insensitive, preserves surrounding text)
    count = 0
    for insult in insults:
        while insult.lower() in body.lower():
            idx  = body.lower().index(insult.lower())
            body = body[:idx] + "CENSORED" + body[idx + len(insult):]
            count += 1

    # Write filtered file
    out_key = FILTERED_PREFIX + key.split("/")[-1]
    s3.put_object(Bucket=BUCKET, Key=out_key, Body=body.encode())

    return count


# ── Reduce function: aggregate counts from all map workers ────────────────────

def sum_counts(results):
    return sum(results)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    s3 = boto3.client("s3", region_name=REGION)

    # List all text files to process
    paginator = s3.get_paginator("list_objects_v2")
    pages     = paginator.paginate(Bucket=BUCKET, Prefix=TEXTS_PREFIX)
    keys      = [
        obj["Key"]
        for page in pages
        for obj in page.get("Contents", [])
        if not obj["Key"].endswith("/")
    ]

    if not keys:
        print(f"No files found at s3://{BUCKET}/{TEXTS_PREFIX}")
        print("Run setup_aws.py first.")
        return

    print(f"Processing {len(keys)} file(s) with Lambda map/reduce...\n")
    for k in keys:
        print(f"  {k}")
    print()

    fexec   = lithops.FunctionExecutor()
    futures = fexec.map_reduce(filter_file, keys, sum_counts)
    total   = fexec.get_result(futures)

    print(f"\nTotal insults censored across all files: {total}")
    print(f"Filtered files stored at s3://{BUCKET}/{FILTERED_PREFIX}")


if __name__ == "__main__":
    main()
