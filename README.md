# burst-scheduler

[![CI](https://github.com/Rishavbhattarai/burst-scheduler/actions/workflows/ci.yml/badge.svg)](https://github.com/Rishavbhattarai/burst-scheduler/actions/workflows/ci.yml)

A small job queue that runs work on local machines and, when jobs wait too long, bursts them to
cloud workers (Kubernetes or AWS Batch), with rules that trade cost against speed and a live
dashboard. Inspired by Princeton's Orchestrated Burst Compute and the HydroGEN stack: Docker, NATS,
a REST API and a React UI.

![Dashboard: 4 local slots full, overflow running on the cloud backend, spend rate and job decisions](docs/dashboard.png)

**Try it in one minute:**

```bash
docker compose up --build            # NATS, controller + dashboard on http://localhost:8000, 2 workers
python3 scripts/load.py              # waves of jobs: watch them spill over to the (simulated) cloud
```

## How it works

```
 dashboard (React) ─┐
             REST (FastAPI)                       NATS JetStream
 client ───▶ controller ──── burst.dispatch.local ────▶ worker 1  (runs jobs as subprocesses)
             │  SQLite queue ◀─── burst.events.job ────  worker 2
             │  scheduler loop ◀── burst.workers.heartbeat (core NATS)
             │  policy engine  ──────────────────── burst.control.cancel ─▶ all workers
             │
             └── submit / poll / cancel ──▶ Kubernetes Jobs · AWS Batch · simulated cloud
```

- **The controller holds the queue.** Jobs wait in SQLite, not in NATS. Workers announce their free slots
  in heartbeats, and the scheduler only dispatches a job when a local slot is free. Because waiting jobs
  have not been handed to anyone yet, the scheduler can still decide to send them somewhere else, which
  is what the cloud burst relies on.
- **At-least-once, but safe.** Dispatches and job events go through JetStream, so nothing is lost if the
  controller restarts. A worker acknowledges a job only when it finishes (and sends in-progress pings while
  it runs), so a job whose worker crashes is redelivered to another worker. Dispatches carry
  `Nats-Msg-Id = job id`, so JetStream drops a duplicate if the controller dies between publishing and
  recording the dispatch. Job state changes are idempotent: duplicate or late events are ignored.
- **Priorities:** higher `priority` first, then first come, first served.
- **Cancellation:** a queued job is removed from the queue; a running job's whole process group is killed,
  and a cloud job is deleted/terminated on its backend.

## Bursting to the cloud

Every scheduling pass first fills free local slots. Each job that is still waiting then goes through
the policy (`burst/policy.py`), which either sends it to a cloud backend or records why it stays:

1. **Trigger:** expected total wait (time waited so far + predicted remaining local wait) reaches
   `burst_threshold_s`, or the job would miss its `deadline_s` locally. The remaining wait is predicted
   from the job's queue position, the number of local slots and the median recent run time.
2. **Guardrails:** a backend needs free capacity (`max_jobs`), must keep the cloud spend rate under
   `max_spend_per_hour`, and its cost estimate must fit in what is left of `daily_budget`. At most
   `max_cloud_jobs` run in the cloud at once.
3. **Deadlines first:** if the job misses its deadline locally, the cheapest backend that meets it wins.
4. **Cost vs speed:** otherwise waiting time saved is worth `value_per_hour` dollars per hour (doubled
   for every +10 priority), and the backend with the largest positive net benefit (value of time saved
   minus cost) is used. Modes: `cheapest` (never burst just for speed), `balanced` ($2/h),
   `fastest` (always burst once the threshold is reached).

Every job carries the reason in `decision`, e.g.
`expected wait 20s ≥ 15s: saves 20s for $0.0012 on kubernetes` or `cloud job limit reached (2)`.
Cost is estimated at dispatch and charged from the actual time between submission and the end.

| backend | how | notes |
|---|---|---|
| `kubernetes` | one `batch/v1` Job per burst job: CPU/memory requests and limits, `activeDeadlineSeconds` = timeout, no retries, TTL cleanup | exit code and log tail are read back from the pod |
| `aws_batch` | `submit_job` with command/vCPU/memory overrides, `describe_jobs`, `terminate_job` | job queue and job definition must exist; the image comes from the job definition |
| `simulated` | runs the job on the controller's machine after `startup_s` and bills it | for demos without a cloud account; results are labelled with the backend name |

Configure them in a TOML file (`BURST_CONFIG=burst.toml`, see [burst.example.toml](burst.example.toml)).
The policy can also be read and changed at runtime with `GET`/`PUT /policy`.

To try Kubernetes locally with [kind](https://kind.sigs.k8s.io/):

```bash
kind create cluster --name burst
cp burst.example.toml burst.toml     # set enabled = true under [backends.kubernetes]
BURST_CONFIG=burst.toml .venv/bin/burst-controller
```

## Dashboard

`dashboard/` is a React + TypeScript app (Vite). The controller serves the built app at `/`, so
`docker compose up` gives you everything on port 8000. It shows:

- **Summary tiles:** queued jobs and the oldest wait, p50/p95 wait over the last 5 minutes, free local
  slots with the estimated wait for a new job, cloud jobs per backend, and today's spend against the
  daily budget.
- **Charts** from `GET /stats/history` (sampled every 2 s, 30 minutes kept): jobs queued / running
  locally / running in the cloud, and the cloud spend rate. Hover (or focus and use the arrow keys) for
  every series at that moment; each chart also has a table view.
- **Policy controls** (`PUT /policy`): mode, threshold, cloud job limit, spend cap and daily budget,
  applied on the next scheduling pass.
- **Workers, backends and recent jobs** with where each job ran, its cost, and the scheduler's reason.

Light and dark mode follow the system setting. For development:

```bash
cd dashboard && npm install && npm run dev   # http://localhost:5173, proxies the API to :8000
```

![Dashboard in dark mode](docs/dashboard-dark.png)

## Run it

With Docker:

```bash
docker compose up --build              # NATS, controller on :8000, 2 workers with 2 slots each
docker compose up --scale worker=4     # more local capacity
```

The compose file loads `burst.example.toml`, so jobs that would wait more than 60 s locally are
sent to the simulated cloud. Point `BURST_CONFIG` at your own file to use Kubernetes or AWS Batch.

Without Docker (needs `nats-server`, e.g. `brew install nats-server`):

```bash
python -m venv .venv && .venv/bin/pip install -e ".[dev]"
nats-server -js &
.venv/bin/burst-controller &
.venv/bin/burst-worker --slots 2 &
```

Generate load (standard library only; `--help` for options):

```bash
python3 scripts/load.py --waves 6 --jobs 20 --min-s 10 --max-s 40
```

Submit and inspect jobs by hand:

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
| GET | `/stats` | queue and wait-time statistics, cloud jobs, spend rate and today's spend |
| GET | `/stats/history?since=` | samples every 2 s for the last 30 minutes (for the charts) |
| GET / PUT | `/policy` | read or change the burst policy (partial updates) |
| GET | `/backends` | configured cloud backends, their prices and active jobs |
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
| `BURST_CONFIG` | (none) | TOML file with `[policy]` and `[backends.*]`; without it everything runs locally |
| `BURST_CLOUD_POLL_INTERVAL` | `2` | seconds between cloud status checks |
| `BURST_DASHBOARD_DIR` | `dashboard/dist` | built dashboard served at `/` (skipped if missing) |

## Tests

```bash
.venv/bin/pytest                                  # ~75 tests, ~35 s
BURST_K8S_CONTEXT=kind-burst .venv/bin/pytest -m k8s   # against a real cluster
```

- Unit tests for the store, scheduler and every policy rule, the Kubernetes manifest and status mapping,
  and the AWS Batch calls (checked against the real API schema with botocore's `Stubber`).
- End-to-end tests start a real `nats-server` (skipped if it is not installed) and check job outcomes,
  priority order, cancellation, redelivery after `kill -9` of a worker, events surviving a controller
  restart, and bursting: threshold, job limit, deadlines, cheapest mode, cloud cancellation, a failing
  backend and runtime policy changes.
- `-m k8s` runs jobs on a real cluster (kind): success, exit codes, deadlines, cancellation, and the
  controller bursting into Kubernetes.

GitHub Actions runs four jobs on every push: lint + tests on Python 3.11 and 3.13 with a real
nats-server, the Kubernetes tests on a kind cluster, the dashboard type check and build, and a
`docker compose` smoke test (`scripts/smoke.sh`) that sends a wave of jobs and checks they finish on
both the local workers and the simulated cloud.

## Layout

```
burst/            controller (API + scheduler), worker, store, policy, NATS setup
burst/backends/   kubernetes, aws_batch, simulated
dashboard/        React + TypeScript dashboard (Vite)
scripts/          load generator, CI smoke test, nats-server installer
tests/            unit, end-to-end (real NATS) and Kubernetes tests
```
