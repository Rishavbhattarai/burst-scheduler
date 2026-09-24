"""Submit waves of jobs to a burst-scheduler controller, to watch jobs spill over to the cloud.

    python scripts/load.py                                  # 4 waves of 12 jobs, 30 s apart
    python scripts/load.py --url http://localhost:8000 --waves 6 --jobs 20 --min-s 10 --max-s 40

Each job sleeps for a random time. Some get a higher priority, and --deadline-share of them get a
deadline, so the dashboard shows every kind of burst decision. Standard library only.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
import urllib.error
import urllib.request


def post(url: str, path: str, body: dict, token: str) -> dict:
    headers = {"content-type": "application/json"}
    if token:
        headers["authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url + path, data=json.dumps(body).encode(), headers=headers, method="POST")
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def get(url: str, path: str, token: str) -> dict:
    headers = {"authorization": f"Bearer {token}"} if token else {}
    with urllib.request.urlopen(urllib.request.Request(url + path, headers=headers), timeout=10) as response:
        return json.load(response)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default=os.environ.get("BURST_URL", "http://localhost:8000"))
    parser.add_argument("--token", default=os.environ.get("BURST_API_TOKEN", ""))
    parser.add_argument("--waves", type=int, default=4)
    parser.add_argument("--jobs", type=int, default=12, help="jobs per wave")
    parser.add_argument("--interval", type=float, default=30, help="seconds between waves")
    parser.add_argument("--min-s", type=float, default=10, help="shortest job")
    parser.add_argument("--max-s", type=float, default=30, help="longest job")
    parser.add_argument("--deadline-share", type=float, default=0.15, help="fraction of jobs with a deadline")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()
    rng = random.Random(args.seed)
    url = args.url.rstrip("/")

    try:
        get(url, "/healthz", args.token)
    except (urllib.error.URLError, OSError) as exc:
        print(f"cannot reach {url}: {exc}", file=sys.stderr)
        return 1

    submitted = 0
    for wave in range(1, args.waves + 1):
        for i in range(args.jobs):
            runtime = rng.uniform(args.min_s, args.max_s)
            job = {
                "name": f"wave{wave}-{i + 1}",
                "command": ["sleep", f"{runtime:.1f}"],
                "est_runtime_s": round(runtime, 1),
                "priority": rng.choice([0, 0, 0, 5, 10]),
            }
            if rng.random() < args.deadline_share:
                job["deadline_s"] = round(runtime + rng.uniform(5, 20), 1)
            post(url, "/jobs", job, args.token)
            submitted += 1
        stats = get(url, "/stats", args.token)
        print(f"wave {wave}: {submitted} jobs submitted · queued {stats['queue_depth']} · "
              f"local free {stats['slots_free']}/{stats['slots_total']} · cloud {stats['cloud']['active']} · "
              f"spent today ${stats['cloud']['spent_today']:.4f}", flush=True)
        if wave < args.waves:
            time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    sys.exit(main())
