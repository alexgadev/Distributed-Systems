import json
import os
import sys
import time


SAMPLE_TEXT = "Hey idiot, why are you so stupid this morning? You nerd."


def make_runner(middleware, target):
    """Returns a callable f(payload) -> None for the chosen middleware/target."""

    if middleware == "xmlrpc":
        import xmlrpc.client
        if target == "service":
            proxy = xmlrpc.client.ServerProxy("http://localhost:8000")
            return proxy.add_insult
        proxy = xmlrpc.client.ServerProxy("http://localhost:8001")
        return proxy.submit_text

    if middleware == "pyro":
        import Pyro4
        cfg = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pyro", "settings.json")
        with open(cfg) as f:
            data = json.load(f)
        if target == "service":
            return Pyro4.Proxy(data["service_uri"]).add_insult
        return Pyro4.Proxy(data["filter_uri"]).submit_text

    if middleware == "redis":
        import redis as redis_lib
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "redis"))
        if target == "service":
            from insult_service import InsultService
            return InsultService().add_insult
        from insult_filter import INSULT_QUEUE
        r = redis_lib.Redis(decode_responses=True)
        return lambda text: r.rpush(INSULT_QUEUE, text)

    if middleware == "rabbitmq":
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "rabbitmq"))
        if target == "service":
            from test_client import InsultClient
            return InsultClient().add_insult
        import pika
        conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        ch = conn.channel()
        ch.queue_declare(queue="insult_filter_queue", durable=True)
        return lambda text: ch.basic_publish(
            exchange="",
            routing_key="insult_filter_queue",
            properties=pika.BasicProperties(delivery_mode=2),
            body=text,
        )

    raise ValueError("Unknown middleware: " + middleware)


def main():
    if len(sys.argv) < 2:
        sys.stderr.write(__doc__)
        sys.exit(2)

    middleware = sys.argv[1]
    target = sys.argv[2] if len(sys.argv) > 2 else "service"
    n = int(sys.argv[3]) if len(sys.argv) > 3 else 1000
    cid = sys.argv[4] if len(sys.argv) > 4 else str(os.getpid())

    runner = make_runner(middleware, target)

    if target == "service":
        payloads = [f"insult-{cid}-{i}" for i in range(n)]
    else:
        payloads = [SAMPLE_TEXT] * n

    start = time.perf_counter()
    for p in payloads:
        runner(p)
    elapsed = time.perf_counter() - start

    print(json.dumps({"elapsed": elapsed, "n": n, "throughput": n / elapsed if elapsed > 0 else 0.0}))


if __name__ == "__main__":
    main()
