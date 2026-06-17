"""
manager.py  —  Exercise 1: the Manager / dispatcher (runs on the EC2 VM).

Cloud equivalent of the example's subscriber.py. It subscribes to the RabbitMQ
'filter_queue' and, for every text, invokes the lambda_filter worker
ASYNCHRONOUSLY (InvocationType='Event'). Because each text triggers its own
Lambda invocation, the number of filter workers grows and shrinks with the
number of pending texts — this is the dynamic scaling demonstrated in Exercise 1.

Run on the EC2 instance (which has pika, boto3 and config.py):
    python3 manager.py

Inject load from anywhere with the same broker config:
    python3 send_texts.py 50          # 50 texts -> ~50 concurrent Lambdas
"""

import json
import boto3
import pika

from config import (
    REGION, RABBIT_HOST, RABBIT_USER, RABBIT_PASS,
    FILTER_QUEUE, FILTER_LAMBDA,
)


def invoke_filter_lambda(lambda_client, message):
    """Invoke the filter worker asynchronously (fire-and-forget)."""
    lambda_client.invoke(
        FunctionName=FILTER_LAMBDA,
        InvocationType="Event",          # asynchronous -> dynamic scaling
        Payload=json.dumps(message),
    )
    print("Filter Lambda invoked for text:", message.get("text_id"))


def make_callback(lambda_client):
    def callback(ch, method, properties, body):
        try:
            message = json.loads(body)
            print("Text received:", message.get("text_id"))
            invoke_filter_lambda(lambda_client, message)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print("Error processing message:", e)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    return callback


def main():
    lambda_client = boto3.client("lambda", region_name=REGION)

    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    connection  = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBIT_HOST, credentials=credentials)
    )
    channel = connection.channel()
    channel.queue_declare(queue=FILTER_QUEUE)

    channel.basic_consume(queue=FILTER_QUEUE, on_message_callback=make_callback(lambda_client))
    print(f"Manager active on '{FILTER_QUEUE}'. Each text spawns a Lambda worker.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Manager stopping...")
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
