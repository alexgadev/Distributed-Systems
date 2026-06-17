"""
stream.py  —  Exercise 2: stream(function, maxfunc, queue)

A generic streaming primitive that consumes messages from a queue and launches
Lambda 'function' instances to process them, auto-scaling the number of
concurrent invocations up and down with the queue load, capped at 'maxfunc'.

It reuses the lambda_filter worker from Exercise 1 (just pass its name as
'function'). This is the bounded version of manager.py: instead of firing one
async Lambda per message with no limit, it keeps at most 'maxfunc' invocations
in flight at once.

How the cap / autoscaling works:
  - Up to (maxfunc - in_flight) messages are pulled from the queue each round and
    handed to a thread pool of size maxfunc, which invokes the Lambda
    synchronously (InvocationType='RequestResponse').
  - A message is acknowledged only after its worker finishes (nack + requeue on
    error), so nothing is lost.
  - The number of busy workers therefore tracks the queue depth: it ramps up to
    maxfunc under load and idles back to 0 when the queue drains.

Run on the EC2 instance:
    python3 stream.py 5            # maxfunc = 5
"""

import sys
import json
import time
import boto3
import pika
from concurrent.futures import ThreadPoolExecutor

from config import (
    REGION, RABBIT_HOST, RABBIT_USER, RABBIT_PASS,
    FILTER_QUEUE, FILTER_LAMBDA,
)


def _connect(queue):
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    connection  = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBIT_HOST, credentials=credentials)
    )
    channel = connection.channel()
    channel.queue_declare(queue=queue)
    return connection, channel


def stream(function, maxfunc, queue):
    """
    Consume `queue`, invoking Lambda `function` with up to `maxfunc` concurrent
    executions, scaling with the queue load.

    Parameters
    ----------
    function : str   Lambda function name (e.g. "lambda_filter")
    maxfunc  : int   maximum number of concurrent Lambda invocations
    queue    : str   RabbitMQ queue name to consume from
    """
    lambda_client      = boto3.client("lambda", region_name=REGION)
    connection, channel = _connect(queue)
    pool               = ThreadPoolExecutor(max_workers=maxfunc)
    in_flight          = {}   # future -> delivery_tag

    def invoke(body):
        resp = lambda_client.invoke(
            FunctionName=function,
            InvocationType="RequestResponse",   # synchronous -> caps concurrency
            Payload=body,
        )
        return json.loads(resp["Payload"].read())

    print(f"[stream] function={function}  maxfunc={maxfunc}  queue={queue}")
    try:
        while True:
            # ── scale up: launch workers up to maxfunc based on queue load ────
            while len(in_flight) < maxfunc:
                method, _props, body = channel.basic_get(queue=queue, auto_ack=False)
                if method is None:
                    break
                fut = pool.submit(invoke, body)
                in_flight[fut] = method.delivery_tag

            # ── reap finished workers and ack their messages ─────────────────
            for fut in [f for f in in_flight if f.done()]:
                tag = in_flight.pop(fut)
                try:
                    result = fut.result()
                    print(f"[stream] done: {result.get('body', result)}")
                    channel.basic_ack(delivery_tag=tag)
                except Exception as e:
                    print("[stream] worker error:", e)
                    channel.basic_nack(delivery_tag=tag, requeue=True)

            print(f"[stream] active workers = {len(in_flight)} / {maxfunc}")
            time.sleep(1 if not in_flight else 0.2)   # idle when queue is empty

    except KeyboardInterrupt:
        print("[stream] stopping...")
    finally:
        pool.shutdown(wait=True)
        connection.close()


if __name__ == "__main__":
    maxfunc = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    stream(FILTER_LAMBDA, maxfunc, FILTER_QUEUE)
