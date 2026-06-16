"""
Dynamic-scaling orchestrator for the Redis InsultFilter.

Spawns and kills redis/insult_filter.py worker processes to maintain a target
response time using:

    N = ceil( (B + λ x Tr) / C )

  B  - current queue backlog       (Redis LLEN of the filter queue)
  λ  - message arrival rate        (msg/s, sliding-window estimate)
  Tr - target response time        (seconds, --Tr, default 1.0)
  C  - single-worker capacity      (msg/s, measured via --capacity or auto-calibrated)

Workflow
--------
  1. Start the Redis InsultService so workers can fetch insults:
       python redis/insult_service.py &

  2. Start the dynamic scaler (it calibrates C, then starts monitoring):
       python dynamic_scaler.py

  3. Run the stress-test client to inject load:
       python client.py redis filter --all

  4. Watch the scaler spawn or kill workers automatically.

Options
-------
  --Tr           Target response time in seconds (default 1.0)
  --capacity C   Known single-worker capacity in msg/s (skip auto-calibration)
  --max-workers  Upper bound on spawned workers (default 8)
  --poll         Polling interval in seconds (default 2.0)
"""

import math
import os
import subprocess
import sys
import time
import argparse
from collections import deque

FILTER_QUEUE = "filter_queue"   # must match redis/insult_filter.py
CALIB_BATCH  = 30


def calibrate(r, script):
    """
    Measure single-worker throughput by timing how fast one worker drains
    CALIB_BATCH messages from FILTER_QUEUE.

    Deletes FILTER_QUEUE before seeding it — call before any real load.
    """
    r.delete(FILTER_QUEUE)
    for i in range(CALIB_BATCH):
        r.rpush(FILTER_QUEUE, f"you idiot probe message {i}")

    proc = subprocess.Popen(
        [sys.executable, script],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    t0 = time.perf_counter()
    while r.llen(FILTER_QUEUE) > 0:
        time.sleep(0.05)
    C = CALIB_BATCH / max(time.perf_counter() - t0, 1e-6)
    proc.terminate()
    proc.wait()
    return C


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--Tr",          type=float, default=1.0,
                        help="Target response time in seconds (default 1.0)")
    parser.add_argument("--capacity",    type=float, default=None,
                        help="Single-worker capacity in msg/s (skips calibration)")
    parser.add_argument("--max-workers", type=int,   default=8,
                        help="Maximum number of worker processes (default 8)")
    parser.add_argument("--poll",        type=float, default=2.0,
                        help="Monitoring interval in seconds (default 2.0)")
    args = parser.parse_args()

    import redis as redis_lib
    r = redis_lib.Redis(decode_responses=True)

    base   = os.path.dirname(os.path.abspath(__file__))
    script = os.path.join(base, "redis", "insult_filter.py")

    if args.capacity is not None:
        C = args.capacity
        print(f"Using provided C = {C:.2f} msg/s")
    else:
        print(f"Calibrating single-worker capacity ({CALIB_BATCH} probe messages)...")
        C = calibrate(r, script)
        print(f"Calibrated C = {C:.2f} msg/s")

    workers = []           # list[subprocess.Popen]
    history = deque(maxlen=10)   # (timestamp, llen) for λ estimation

    print(f"\nDynamic scaler running — Tr={args.Tr}s  C={C:.2f} msg/s  max={args.max_workers}")
    print("Press Ctrl-C to stop.\n")
    print(f"{'ACTION':6s}  {'B':>6s}  {'λ (msg/s)':>10s}  {'N_target':>8s}  {'workers':>7s}")
    print("-" * 48)

    try:
        while True:
            B   = r.llen(FILTER_QUEUE)
            now = time.time()
            history.append((now, B))

            # prune dead workers
            workers = [w for w in workers if w.poll() is None]
            n_alive = len(workers)

            # estimate arrival rate λ over the observation window
            if len(history) >= 2:
                t0, b0 = history[0]
                t1, b1 = history[-1]
                dt = max(t1 - t0, 1e-6)
                # messages arrived = (backlog change) + (messages drained by workers)
                drained = min(float(b0), n_alive * C * dt)
                arrived = max(0.0, (b1 - b0) + drained)
                lam = arrived / dt
            else:
                lam = 0.0

            N = math.ceil((B + lam * args.Tr) / C) if C > 0 else 1
            N = max(1, min(N, args.max_workers))

            if n_alive < N:
                to_add = N - n_alive
                for _ in range(to_add):
                    p = subprocess.Popen(
                        [sys.executable, script],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                    workers.append(p)
                print(f"{'[SPAWN]':6s}  {B:6d}  {lam:10.1f}  {N:8d}  {len(workers):7d}  +{to_add}")
            elif n_alive > N:
                to_kill = n_alive - N
                for _ in range(to_kill):
                    w = workers.pop()
                    w.terminate()
                    w.wait()
                print(f"{'[KILL]':6s}  {B:6d}  {lam:10.1f}  {N:8d}  {len(workers):7d}  -{to_kill}")
            else:
                print(f"{'[OK]':6s}  {B:6d}  {lam:10.1f}  {N:8d}  {n_alive:7d}")

            time.sleep(args.poll)

    except KeyboardInterrupt:
        print("\nShutting down dynamic scaler...")
        for w in workers:
            w.terminate()
            w.wait()
        print(f"Stopped {len(workers)} remaining workers.")


if __name__ == "__main__":
    main()
