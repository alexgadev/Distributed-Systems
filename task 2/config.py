"""
Shared configuration for the EC2-side scripts (manager.py, stream.py, send_texts.py)
and the Lithops exercise (exercise3.py).

NOTE: the Lambda function files (lambda_filter.py, lambda_text_publisher.py) are
deployed standalone in the AWS console, so they keep their OWN copies of these
constants at the top of each file — edit them there too.
"""

REGION = "us-east-1"

# Change BUCKET to a globally unique name (e.g. add your name / student ID)
BUCKET = "sd-insult-filter-axgaam"

# ── RabbitMQ broker running on the EC2 VM (see ec2_steps.txt) ─────────────────
RABBIT_HOST = "54.89.50.195"     # public IP / DNS of your RabbitMQ EC2 instance
RABBIT_USER = "user"
RABBIT_PASS = "password123"

FILTER_QUEUE  = "filter_queue"      # texts waiting to be censored
RESULTS_QUEUE = "results_queue"     # (optional) censored results

# ── Lambda worker function name, as deployed in the AWS console ───────────────
FILTER_LAMBDA = "lambda_filter"

# ── S3 layout (used by the Lambda worker and the Lithops exercise) ────────────
INSULTS_KEY     = "insults.txt"
TEXTS_PREFIX    = "texts/"
FILTERED_PREFIX = "filtered/"

# Insult words used by the filter
INSULTS = ["idiot", "stupid", "nerd", "fool", "dummy"]
