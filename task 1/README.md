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
# Also requires a running Redis server: https://redis.io/docs/getting-started/

# RabbitMQ
pip install pika
# Also requires a running RabbitMQ server: https://www.rabbitmq.com/download.html
```

> All commands below assume the working directory is the **repository root** (`c:\URV\Distributed-Systems`), unless stated otherwise.

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

Uses Redis data structures (lists) and pub/sub for broadcasting. Requires a running Redis server on `localhost:6379`.

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
cd "task 1/redis"
python test_client.py
```

> The test client must be run from the `task 1/redis/` directory because it imports directly from `insult_service.py` and `insult_filter.py`.

The client sends insults, triggers a filter job, and subscribes to the broadcast channel. Stop with `Ctrl+C`.

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

The performance analysis is split into a load generator (`client.py`) and an orchestrator (`performance_analysis.py`) that implements the three phases from the spec.

### Generic stress client

`client.py` sends `num_requests` requests as fast as possible and prints a JSON summary `{elapsed, n, throughput}` on stdout. It is invoked directly by the orchestrator, but can also be run by hand:

```bash
# From the repository root
python "task 1/client.py" <middleware> [target] [num_requests] [client_id]

# Examples
python "task 1/client.py" xmlrpc service 1000
python "task 1/client.py" redis  filter  2000
```

- `middleware` — `xmlrpc | pyro | redis | rabbitmq`
- `target` — `service` (calls `add_insult`) or `filter` (submits a text). Default: `service`
- `num_requests` — default `1000`
- `client_id` — namespace prefix to avoid payload collisions between concurrent clients

### Orchestrator (3 phases)

The orchestrator auto-spawns every InsultService / InsultFilter it needs, waits for them to be ready, runs the client workers, and tears everything down between runs. The only external dependencies are the **Redis** broker on `:6379` and the **RabbitMQ** broker on `:5672` — make sure those are running before you start. Middlewares whose broker isn't reachable are skipped with a warning.

Run each phase from the repository root.

**Phase 1 — Single-node throughput** (compare the 4 middlewares):
```bash
python "task 1/performance_analysis.py" phase1 --target service
python "task 1/performance_analysis.py" phase1 --target filter
```

**Phase 2 — Multi-node static scaling** (speedup S = T₁/Tₙ for 1, 2, 3 worker nodes):
```bash
python "task 1/performance_analysis.py" phase2 --middleware redis    --target filter
python "task 1/performance_analysis.py" phase2 --middleware rabbitmq --target filter
python "task 1/performance_analysis.py" phase2 --middleware xmlrpc   --target filter
python "task 1/performance_analysis.py" phase2 --middleware pyro     --target filter
```
For Redis/RabbitMQ the orchestrator spawns N parallel filter worker processes (the broker load-balances). For XML-RPC/Pyro it sets `INSULT_FILTER_WORKERS=N` so the filter starts N internal worker processes. Service-target replication is only supported for Redis (the others keep state in memory).

**Phase 3 — Dynamic scaling** (Redis filter autoscaler using `N = ⌈(B + λ·Tr) / C⌉`):
```bash
python "task 1/performance_analysis.py" phase3 --capacity 200 --target-response 1 --duration 60
```
This spawns its own built-in load generator with a time-varying arrival rate (don't run `redis/insult_filter.py` yourself for this phase — the autoscaler manages its own worker pool) and plots how the worker count, backlog, and λ evolve over time.

### Filter worker count via env var

The XML-RPC and Pyro filters read `INSULT_FILTER_WORKERS` (default `1`) on startup, so you can launch them with any number of internal workers:
```bash
INSULT_FILTER_WORKERS=4 python "task 1/xmlrpc/insult_filter.py"
```
