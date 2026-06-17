# Task 2 — Scaling Distributed Systems in the Cloud (InsultFilter)

The Task-1 InsultFilter is ported to AWS following the same methodology as the
"Distributed Order Processing" lab: a **RabbitMQ broker on an EC2 VM**, **Lambda
workers** deployed in the console (with a **Lambda Layer** for `pika`), and an
**EC2 Manager** that pulls work from the queue and invokes the workers via the
**boto3 Lambda API**. Exercise 3 uses **Lithops** map/reduce for bulk S3 files.

## Architecture (Exercises 1 & 2)

```
                upload texts.csv
   [client] ───────────────────────► S3 bucket
                                         │  (S3 event trigger)
                                         ▼
                            lambda_text_publisher  ──► RabbitMQ 'filter_queue'  (on EC2)
                                                              │
   send_texts.py ─────────────────────────────────────────► │  (alt. direct producer)
                                                              ▼
                                          Manager (EC2)  /  stream() (EC2)
                                                              │  boto3 invoke
                                          ┌───────────────────┼───────────────────┐
                                          ▼                   ▼                   ▼
                                   lambda_filter       lambda_filter       lambda_filter   ...
                                          │                   │                   │
                                          └──────► S3  s3://BUCKET/filtered/ ◄─────┘
```

- **Exercise 1** — `manager.py` invokes one Lambda **asynchronously** per text
  (`InvocationType='Event'`): unbounded dynamic scaling.
- **Exercise 2** — `stream(function, maxfunc, queue)` invokes Lambdas with a
  **concurrency cap** (`maxfunc`) that scales up/down with the queue depth.
- **Exercise 3** — `exercise3.py` uses Lithops `map_reduce` over the `texts/`
  files in S3 (no broker / no EC2).

## File map

| File | Where it runs | Role |
|------|---------------|------|
| `config.py` | EC2 | shared config for the EC2 scripts + Lithops |
| `ec2_steps.txt` | EC2 | commands to install RabbitMQ on the VM |
| `setup_aws.py` | local | create the S3 bucket, upload `insults.txt` + sample `texts/` |
| `lambda_filter.py` | **Lambda (console)** | worker: censor one text, write to `filtered/` |
| `lambda_text_publisher.py` | **Lambda (console)** | S3-triggered: publish texts to RabbitMQ |
| `manager.py` | EC2 | **Exercise 1** dispatcher (one async Lambda per text) |
| `stream.py` | EC2 | **Exercise 2** `stream(function, maxfunc, queue)` |
| `send_texts.py` | EC2/local | producer to inject texts into `filter_queue` |
| `texts.csv` | S3 upload | sample input that triggers `lambda_text_publisher` |
| `exercise3.py` | local | **Exercise 3** Lithops map/reduce |
| `lithops_config_template.yaml` | local | Lithops backend config for Exercise 3 |

## Prerequisites

- AWS Academy Learner Lab started (green dot); `LabRole` for Lambda, and a
  `LabInstanceProfile` for EC2.
- AWS credentials configured locally (`aws configure` / env vars).
- The **pika Lambda Layer**: `layer_content.zip` (contents under
  `python/lib/python3.13/site-packages/`, providing `pika`). `boto3` is already
  in the Lambda runtime, so it is **not** needed in the layer.
- **Lambda runtime: Python 3.13** (matches the layer).

## Setup & deployment

1. **Edit `config.py`** — set `BUCKET` (globally unique) and, after step 3,
   `RABBIT_HOST` (the EC2 public IP). Set the same `BUCKET` at the top of
   `lambda_filter.py` and the broker details at the top of
   `lambda_text_publisher.py`.

2. **Provision S3**
   ```bash
   python setup_aws.py
   ```

3. **Deploy RabbitMQ on EC2** — launch a `t2.micro` Debian instance with
   `LabInstanceProfile` and a security group allowing ports `22`, `5672`,
   `15672`, then run the commands in `ec2_steps.txt` on it.

4. **Create the pika Lambda Layer** — Lambda console → Layers → upload
   `layer_content.zip` → copy its ARN.

5. **Deploy the Lambdas** (runtime Python 3.13, role `LabRole`):
   - `lambda_filter` — paste `lambda_filter.py`, set `BUCKET`. No layer needed.
     Timeout ~15 s.
   - `lambda_text_publisher` — paste `lambda_text_publisher.py`, set the broker
     details, **attach the pika layer ARN**, add an **S3 trigger** ("all object
     create events") on your bucket. Timeout ~30 s.

## Running the exercises

### Exercise 1 — dynamic scaling (one Lambda per text)
On the EC2 VM:
```bash
python3 manager.py
```
Inject load (from the VM or anywhere with the broker config):
```bash
python3 send_texts.py 50            # or: upload texts.csv to the S3 bucket
```
**Verify:** RabbitMQ console (`http://<EC2_PUBLIC_IP>:15672`) shows the queue
draining, CloudWatch shows many concurrent `lambda_filter` invocations, and
`s3://BUCKET/filtered/` fills with censored files.

### Exercise 2 — stream(function, maxfunc, queue)
On the EC2 VM:
```bash
python3 stream.py 5                 # maxfunc = 5
python3 send_texts.py 100           # burst of load
```
**Verify:** the `[stream] active workers = k / 5` log never exceeds `maxfunc`,
ramps up under load, and idles back to 0 when the queue drains.

### Exercise 3 — Lithops map/reduce over S3
Copy `lithops_config_template.yaml` to `~/.lithops/config` (fill in account id /
bucket), then:
```bash
python exercise3.py
```
Outputs the censored files to `s3://BUCKET/filtered/` and prints the total number
of insults censored across all files.
