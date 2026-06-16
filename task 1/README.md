# Task 1 - Insult Service

Distributed insult service implemented using four different middleware technologies: XML-RPC, Pyro4, Redis, and RabbitMQ.

Each implementation provides the same functionality:
- **InsultService**: stores insults and broadcasts them periodically to subscribers
- **InsultFilter**: receives texts, censors known insults, and stores the results
- **Test client**: demonstrates add/get insults, filter text, and subscribe to broadcast

---

## Prerequisites

Python 3.8+ is required. Install dependencies based on which implementation you want to run:

```bash
# Pyro4
pip install Pyro4

# Redis
pip install redis
# Also requires a running Redis server on localhost:6379
# (or any Redis-compatible server such as Valkey): https://redis.io/docs/getting-started/

# RabbitMQ
pip install pika
# Also requires a running RabbitMQ server: https://www.rabbitmq.com/download.html
```

> All commands below assume the working directory is the **repository root**, unless stated otherwise.

---

## XML-RPC

Uses Python's built-in `xmlrpc` module — no extra dependencies needed.

Open **3 terminals** and run each command in order:

**Terminal 1 — Insult Service (port 8000):**
```bash
python "task 1/xmlrpc/insult_service.py"
```

**Terminal 2 — Insult Filter (port 8001):**
```bash
python "task 1/xmlrpc/insult_filter.py"
```

**Terminal 3 — Test Client:**
```bash
python "task 1/xmlrpc/test_client.py"
```

The client adds insults, filters a sentence, retrieves the insult list and filtered results, then subscribes to the broadcast (receives a random insult every 5 seconds). Stop with `Ctrl+C`.

---

## Pyro4

Uses the Pyro4 RPC library. Service URIs are exchanged via `task 1/pyro/settings.json`.

Open **3 terminals** in order:

**Terminal 1 — Insult Service:**
```bash
python "task 1/pyro/insult_service.py"
```

> Wait until the service is running before starting the filter; the service writes its URI to `settings.json` on startup.

**Terminal 2 — Insult Filter:**
```bash
python "task 1/pyro/insult_filter.py"
```

**Terminal 3 — Test Client:**
```bash
python "task 1/pyro/test_client.py"
```

The client reads both URIs from `settings.json`. Stop with `Ctrl+C` — the client cleans up `settings.json` on exit.

---

## Redis

Uses Redis data structures and pub/sub: insults are kept in a **set** (`insult_list`, deduplicated via `SADD`/`SMEMBERS`), the filter work queue and the filtered-results list are Redis **lists**, and broadcasting uses a **pub/sub** channel (`insult_pubsub`). Requires a running Redis-compatible server on `localhost:6379`.

Open **3 terminals** in order:

**Terminal 1 — Insult Service:**
```bash
python "task 1/redis/insult_service.py"
```

**Terminal 2 — Insult Filter:**
```bash
python "task 1/redis/insult_filter.py"
```

**Terminal 3 — Test Client:**

```bash
python "task 1/redis/test_client.py"
```

The client sends insults, triggers a filter job, retrieves the insult list and filtered results, and subscribes to the broadcast channel. Stop with `Ctrl+C`.

---

## RabbitMQ

Uses RabbitMQ with direct queues (RPC pattern) and a fanout exchange for broadcasting. Requires a running RabbitMQ server on `localhost:5672`.

Open **3 terminals** in order:

**Terminal 1 — Insult Service:**
```bash
python "task 1/rabbitmq/insult_service.py"
```

**Terminal 2 — Insult Filter:**
```bash
python "task 1/rabbitmq/insult_filter.py"
```

**Terminal 3 — Test Client:**
```bash
python "task 1/rabbitmq/test_client.py"
```

The client adds insults via RPC, submits a text to be filtered, retrieves filtered results, and then subscribes to the broadcast exchange. Stop with `Ctrl+C`.

---

## Performance Analysis

Performance testing uses three standalone tools, all run from the repository root:

| Tool | Purpose |
|------|---------|
| `client.py` | Generic load generator / stress client for all four middlewares |
| `orchestrator.py` | Round-robin front-end for several separate XML-RPC / Pyro4 backends (static scaling) |
| `dynamic_scaler.py` | Backlog-based autoscaler for the Redis filter (dynamic scaling) |

Start the relevant service/filter (and broker) before running the client.

### Generic stress client (`client.py`)

Sends `n` requests as fast as possible and prints one JSON summary per measurement, e.g.
`{"middleware":"xmlrpc","target":"service","n":1000,"concurrent":1,"elapsed":0.77,"throughput":1295.3}`.

```bash
python "task 1/client.py" <middleware> <target> [n] [--all] [--concurrent C]
```

- `middleware` — `xmlrpc | pyro | redis | rabbitmq`
- `target` — `service` (calls `add_insult`) or `filter` (submits a text). **Required.**
- `n` — requests per client (default `1000`)
- `--all` — sweep `n = 1, 10, 100, 1000`, printing one JSON line each
- `--concurrent C` — run `C` parallel client processes (default `1`)
- `--port P` — override the XML-RPC endpoint port (XML-RPC only; e.g. target the orchestrator on `9000`/`9001`)

```bash
# Examples (start the matching service / filter first)
python "task 1/client.py" xmlrpc service 1000
python "task 1/client.py" redis  filter  --all
python "task 1/client.py" rabbitmq filter 2000 --concurrent 4
```

> For the `filter` target, `submit_text` only enqueues the job and returns, so the reported throughput is the **enqueue** rate. To measure end-to-end processing throughput, time how long the filtered-results list/queue takes to reach `n` messages.

### Phase 1 — Single-node throughput

Start one service (and, for the filter, one filter worker) per middleware and sweep the load:

```bash
python "task 1/client.py" xmlrpc   service --all
python "task 1/client.py" pyro     service --all
python "task 1/client.py" redis    service --all
python "task 1/client.py" rabbitmq service --all
# then repeat each with: filter
```

### Phase 2 — Static multi-node scaling (1 / 2 / 3 workers)

**Redis / RabbitMQ** — the filter is a competing consumer on a shared queue, so simply start N worker
processes and the broker load-balances the work:

```bash
for i in 1 2 3; do python "task 1/redis/insult_filter.py"    & done   # 3 Redis workers
for i in 1 2 3; do python "task 1/rabbitmq/insult_filter.py" & done   # 3 RabbitMQ workers
```

**XML-RPC / Pyro4** — the filter keeps its queue in-process, so scale it by forking N internal worker
processes via the `INSULT_FILTER_WORKERS` env var (default `1`):

```bash
INSULT_FILTER_WORKERS=3 python "task 1/xmlrpc/insult_filter.py"
INSULT_FILTER_WORKERS=3 python "task 1/pyro/insult_filter.py"
```

Drive each configuration with the stress client and compute the speedup **S = T₁ / Tₙ** from the elapsed
times for 1, 2 and 3 workers.

Advanced: `orchestrator.py` can instead round-robin requests across several *separate* XML-RPC / Pyro4
backends. For XML-RPC, start the backends on `base + 2*k` ports, run the orchestrator (it listens on
`9000` for service / `9001` for filter), then point the client at it with `--port`:

```bash
XMLRPC_FILTER_PORT=8001 python "task 1/xmlrpc/insult_filter.py" &
XMLRPC_FILTER_PORT=8003 python "task 1/xmlrpc/insult_filter.py" &
XMLRPC_FILTER_PORT=8005 python "task 1/xmlrpc/insult_filter.py" &
python "task 1/orchestrator.py" xmlrpc filter --nodes 3        # proxy on :9001
python "task 1/client.py" xmlrpc filter --all --port 9001      # drive the orchestrator
```

For Pyro4 the orchestrator writes its proxy URI into `settings.json` and `client.py` picks it up
automatically. See the `orchestrator.py` docstring for the full conventions.

### Phase 3 — Dynamic scaling (Redis autoscaler)

`dynamic_scaler.py` spawns and kills `redis/insult_filter.py` workers to hold a target response time,
using **N = ⌈(B + λ·Tr) / C⌉**:

```bash
# Terminal 1 — start the broadcaster service and seed a few insults so the filter censors
python "task 1/redis/insult_service.py" &
python "task 1/client.py" redis service 5

# Terminal 2 — start the autoscaler (auto-calibrates C, then monitors)
python "task 1/dynamic_scaler.py" --Tr 1.0 --max-workers 8 --poll 1.0

# Terminal 3 — inject load and watch workers scale up / down
python "task 1/client.py" redis filter --all
```

Options: `--Tr` target response time in seconds (default `1.0`), `--capacity C` to skip calibration with
a known single-worker capacity, `--max-workers` (default `8`), `--poll` interval in seconds (default `2.0`).
The autoscaler manages its own worker pool — don't start `redis/insult_filter.py` yourself for this phase.
