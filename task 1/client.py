"""
Stress-test client for all four middleware implementations.

Usage
-----
  Sequential, single run:
    python client.py <middleware> <target> [n]

  Loop over 1 / 10 / 100 / 1000 requests automatically:
    python client.py <middleware> <target> --all

  Concurrent clients (each sends n requests simultaneously):
    python client.py <middleware> <target> [n] --concurrent <c>

  Target the XML-RPC orchestrator instead of the default backend port:
    python client.py xmlrpc service 1000 --port 9000
    python client.py xmlrpc filter  1000 --port 9001

Arguments
---------
  middleware    xmlrpc | pyro | redis | rabbitmq
  target        service | filter
  n             requests per client (default 1000)
  --all         run n = 1, 10, 100, 1000; print one JSON line each
  --concurrent  number of parallel client processes (default 1)
  --port        override the XML-RPC endpoint port (XML-RPC only)

Output
------
One JSON object per measurement:
  {"middleware":"xmlrpc","target":"service","n":100,"concurrent":1,
   "elapsed":0.45,"throughput":222.2}
"""

import json
import os
import sys
import time
import argparse
from multiprocessing import Process, Queue as MPQueue

SAMPLE_TEXT = "Hey idiot, why are you so stupid this morning? You nerd."
LOAD_SIZES  = [1, 10, 100, 1000]


def make_runner(middleware, target, port=None):
    """Return a callable f(payload) for the chosen middleware/target."""

    if middleware == "xmlrpc":
        import xmlrpc.client
        default = 8000 if target == "service" else 8001
        proxy   = xmlrpc.client.ServerProxy(f"http://localhost:{port or default}")
        return proxy.add_insult if target == "service" else proxy.submit_text

    if middleware == "pyro":
        import Pyro4
        cfg = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pyro", "settings.json")
        with open(cfg) as f:
            data = json.load(f)
        if target == "service":
            uri = data.get("orchestrator_service_uri") or data["service_uri"]
            return Pyro4.Proxy(uri).add_insult
        uri = data.get("orchestrator_filter_uri") or data["filter_uri"]
        return Pyro4.Proxy(uri).submit_text

    if middleware == "redis":
        import redis as redis_lib
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "redis"))
        if target == "service":
            from insult_service import INSULT_LIST
            r = redis_lib.Redis(decode_responses=True)
            return lambda insult: r.sadd(INSULT_LIST, insult)
        from insult_filter import FILTER_QUEUE
        r = redis_lib.Redis(decode_responses=True)
        return lambda text: r.rpush(FILTER_QUEUE, text)

    if middleware == "rabbitmq":
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "rabbitmq"))
        if target == "service":
            from test_client import InsultClient
            return InsultClient().add_insult
        import pika
        conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        ch   = conn.channel()
        ch.queue_declare(queue="insult_filter_queue", durable=True)
        return lambda text: ch.basic_publish(
            exchange="",
            routing_key="insult_filter_queue",
            properties=pika.BasicProperties(delivery_mode=2),
            body=text,
        )

    raise ValueError("Unknown middleware: " + middleware)


def _payloads(target, n, cid):
    """Return a list or texts depending on the target. """
    if target == "service":
        return [f"insult-{cid}-{i}" for i in range(n)]
    return [SAMPLE_TEXT] * n


def _run_sequential(middleware, target, n, port, cid):
    """Runs the program for a single client."""
    runner = make_runner(middleware, target, port)
    items  = _payloads(target, n, cid)
    t0 = time.perf_counter()
    for item in items:
        runner(item)
    return time.perf_counter() - t0


def _worker_proc(middleware, target, n, port, cid, result_q):
    elapsed = _run_sequential(middleware, target, n, port, cid)
    result_q.put(elapsed)


def _run_concurrent(middleware, target, n, c, port):
    """Runs the program for c concurrent clients."""
    q     = MPQueue()
    procs = [
        Process(target=_worker_proc, args=(middleware, target, n, port, str(i), q))
        for i in range(c)
    ]
    wall_t0 = time.perf_counter()
    for p in procs:
        p.start()
    for p in procs:
        p.join()
    return time.perf_counter() - wall_t0


def _emit(middleware, target, n, c, elapsed):
    """Shows the results for the performed run."""
    total = n * c
    print(json.dumps({
        "middleware": middleware,
        "target":     target,
        "n":          total,
        "concurrent": c,
        "elapsed":    round(elapsed, 4),
        "throughput": round(total / elapsed, 2) if elapsed > 0 else 0.0,
    }), flush=True)


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("middleware", choices=["xmlrpc", "pyro", "redis", "rabbitmq"])
    parser.add_argument("target",     choices=["service", "filter"])
    parser.add_argument("n",          nargs="?", type=int, default=1000,
                        help="Requests per client (default 1000)")
    parser.add_argument("--all",      action="store_true",
                        help="Run n = 1, 10, 100, 1000 sequentially")
    parser.add_argument("--concurrent", metavar="C", type=int, default=1,
                        help="Number of parallel client processes (default 1)")
    parser.add_argument("--port", type=int, default=None,
                        help="Override the XML-RPC endpoint port "
                             "(e.g. target the orchestrator on 9000/9001)")
    args = parser.parse_args()

    sizes = LOAD_SIZES if args.all else [args.n]
    c     = args.concurrent

    for n in sizes:
        if c == 1:
            elapsed = _run_sequential(args.middleware, args.target, n, args.port,
                                      str(os.getpid()))
        else:
            elapsed = _run_concurrent(args.middleware, args.target, n, c, args.port)
        _emit(args.middleware, args.target, n, c, elapsed)


if __name__ == "__main__":
    main()
