# burst-scheduler

A small job queue that runs work on local machines and, when jobs wait too long, bursts them to
cloud workers (Kubernetes or AWS Batch). Inspired by Princeton's Orchestrated Burst Compute and the
HydroGEN stack: Docker, NATS, a REST API and a React UI.

> **Status:** part 1 of 3, the local queue. Cloud bursting with cost/speed rules and the dashboard come next.

## How it works

```
             REST (FastAPI)                       NATS JetStream
 client ───▶ controller ──── burst.dispatch.local ────▶ worker 1  (runs jobs as subprocesses)
             │  SQLite queue ◀─── burst.events.job ────  worker 2
             │  scheduler loop ◀── burst.workers.heartbeat (core NATS)
             └───────────────────── burst.control.cancel ─▶ all workers
```

- **The controller holds the queue.** Jobs wait in SQLite, not in NATS. Workers announce their free slots
  in heartbeats, and the scheduler only dispatches a job when a local slot is free. Because waiting jobs
  have not been handed to anyone yet, the scheduler can still decide to send them somewhere else, which
  is what the cloud burst (part 2) relies on.
- **At-least-once, but safe.** Dispatches and job events go through JetStream, so nothing is lost if the
  controller restarts. A worker acknowledges a job only when it finishes (and sends in-progress pings while
  it runs), so a job whose worker crashes is redelivered to another worker. Dispatches carry
  `Nats-Msg-Id = job id`, so JetStream drops a duplicate if the controller dies between publishing and
  recording the dispatch. Job state changes are idempotent: duplicate or late events are ignored.
- **Priorities:** higher `priority` first, then first come, first served.
- **Cancellation:** a queued job is removed from the queue; a running job's whole process group is killed.

## Run it

With Docker:

```bash
docker compose up --build              # NATS, controller on :8000, 2 workers with 2 slots each
docker compose up --scale worker=4     # more local capacity
```

Without Docker (needs `nats-server`, e.g. `brew install nats-server`):

```bash
python -m venv .venv && .venv/bin/pip install -e ".[dev]"
nats-server -js &
.venv/bin/burst-controller &
.venv/bin/burst-worker --slots 2 &
```

Submit and inspect jobs:

```bash
curl -X POST localhost:8000/jobs -H 'content-type: application/json' \
     -d '{"name": "hello", "command": ["python", "-c", "print(6 * 7)"], "priority": 5}'
curl localhost:8000/jobs/<id>          # state, worker, exit code, last 4 KB of output
curl localhost:8000/stats              # queue depth, oldest wait, p50/p95 wait, free slots, estimated wait
curl localhost:8000/workers
curl -X POST localhost:8000/jobs/<id>/cancel
```

Interactive API docs are at <http://localhost:8000/docs>.

## API

| method | path | |
|---|---|---|
| POST | `/jobs` | submit: `command` (argv list), `name`, `priority`, `timeout_s`, `est_runtime_s`, `deadline_s`, `image`, `cpu`, `memory_mb` |
| GET | `/jobs?state=&limit=` | list jobs, newest first |
| GET | `/jobs/{id}` | one job |
| POST | `/jobs/{id}/cancel` | cancel (409 if already finished) |
| GET | `/workers` | local workers and their free slots |
| GET | `/stats` | queue and wait-time statistics |
| GET | `/healthz` | controller health (no auth) |

Set `BURST_API_TOKEN` to require `Authorization: Bearer <token>`. Jobs run arbitrary commands on the
workers, so only expose the API on a trusted network.

## Configuration

| variable | default | |
|---|---|---|
| `BURST_NATS_URL` | `nats://127.0.0.1:4222` | controller and workers |
| `BURST_DB_PATH` | `burst.db` | controller's SQLite file |
| `BURST_API_TOKEN` | (none) | require a bearer token |
| `BURST_SCHEDULE_INTERVAL` | `0.5` | seconds between scheduling passes |
| `BURST_WORKER_TIMEOUT` | `10` | seconds without a heartbeat before a worker counts as gone |
| `BURST_WORKER_SLOTS` | CPU count | jobs a worker runs at once |
| `BURST_ACK_WAIT` | `30` | seconds before an unacknowledged job is redelivered |

## Tests

```bash
.venv/bin/pytest          # 26 tests, ~15 s
```

The end-to-end tests start a real `nats-server` (skipped if it is not installed) and check job outcomes,
priority order, cancellation, redelivery after `kill -9` of a worker, and that job events survive a
controller restart.
