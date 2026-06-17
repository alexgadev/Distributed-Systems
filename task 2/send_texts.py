"""
send_texts.py  —  simple producer to inject texts into the RabbitMQ filter queue.

Use it to drive the dynamic-scaling demo without going through S3:
    python3 send_texts.py 50       # publish 50 texts to 'filter_queue'

(Alternatively, upload texts.csv to the S3 bucket to trigger lambda_text_publisher.)
"""

import sys
import json
import pika

from config import RABBIT_HOST, RABBIT_USER, RABBIT_PASS, FILTER_QUEUE

SAMPLE_TEXTS = [
    "Hey idiot, why are you so stupid this morning? You nerd.",
    "You are a genius, great work today!",
    "Stop being a nerd and go outside.",
    "Don't be a fool, think before you speak.",
    "You dummy, you forgot to close the door again.",
    "This is a perfectly normal and polite sentence.",
]


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 20

    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    connection  = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBIT_HOST, credentials=credentials)
    )
    channel = connection.channel()
    channel.queue_declare(queue=FILTER_QUEUE)

    for i in range(n):
        text    = SAMPLE_TEXTS[i % len(SAMPLE_TEXTS)]
        message = json.dumps({"text_id": str(i + 1), "text": text})
        channel.basic_publish(exchange="", routing_key=FILTER_QUEUE, body=message)

    print(f"Sent {n} texts to '{FILTER_QUEUE}'")
    connection.close()


if __name__ == "__main__":
    main()
