"""When to send a queued job to the cloud, and to which backend.

For each job still waiting after the free local slots are filled:

1. Trigger. The job is a candidate to burst when its expected total wait (time already waited plus
   predicted remaining local wait) reaches `burst_threshold_s`, or when it would miss its deadline
   if it stayed local.
2. Guardrails. A backend is only considered if it has capacity, adding the job keeps the cloud spend
   rate under `max_spend_per_hour`, and its estimated cost fits in what is left of `daily_budget`.
3. Deadline. If the job misses its deadline locally but a backend meets it, the cheapest such
   backend wins, regardless of the cost-vs-speed rule.
4. Cost vs speed. Otherwise the waiting time saved is worth `value_per_hour` dollars per hour
   (doubled for every +10 priority). The backend with the largest net benefit (value of time saved
   minus cost) is used if that benefit is positive. Presets: cheapest (never burst for speed, only
   for deadlines), balanced, fastest (always burst once triggered).

Every decision carries a human-readable reason, shown in the API and dashboard.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from .models import Job

MODES = {"cheapest": 0.0, "balanced": 2.0, "fastest": math.inf}


@dataclass
class PolicyConfig:
    enabled: bool = True
    burst_threshold_s: float = 60.0
    mode: str = "balanced"
    value_per_hour: float | None = None   # overrides the mode's value when set
    max_cloud_jobs: int = 10
    max_spend_per_hour: float = 5.0
    daily_budget: float = 50.0
    default_runtime_s: float = 60.0

    def __post_init__(self) -> None:
        if self.mode not in MODES:
            raise ValueError(f"mode must be one of {sorted(MODES)}")

    @property
    def time_value_per_hour(self) -> float:
        return self.value_per_hour if self.value_per_hour is not None else MODES[self.mode]


@dataclass
class Offer:
    """What a cloud backend can do for a job right now."""

    backend: str
    price_per_hour: float   # $ per hour for this job's cpu/memory
    startup_s: float        # typical time from submit to running
    capacity: int           # how many more jobs it accepts now


@dataclass
class BudgetState:
    active_cloud_jobs: int = 0
    spend_rate_per_hour: float = 0.0   # sum of price_per_hour of running cloud jobs
    spent_today: float = 0.0           # finished jobs' cost + running jobs' estimates


@dataclass
class Decision:
    backend: str | None
    reason: str
    cost_estimate: float = 0.0
    price_per_hour: float = 0.0
    expected_local_wait_s: float | None = None


@dataclass
class _Candidate:
    offer: Offer
    cost: float
    meets_deadline: bool
    time_saved_s: float
    net_benefit: float = field(default=0.0)


def _fmt_s(seconds: float) -> str:
    return "∞" if math.isinf(seconds) else f"{seconds:.0f}s"


def decide(job: Job, now: float, predicted_remaining_local_s: float | None, runtime_s: float,
           offers: list[Offer], budget: BudgetState, config: PolicyConfig) -> Decision:
    """Decide whether `job` should leave the local queue for a cloud backend."""
    remaining = math.inf if predicted_remaining_local_s is None else predicted_remaining_local_s
    expected_wait = job.wait_s(now) + remaining
    base = {"expected_local_wait_s": None if math.isinf(expected_wait) else expected_wait}

    if not config.enabled:
        return Decision(None, "bursting is disabled", **base)

    misses_deadline = False
    if job.deadline_s is not None:
        misses_deadline = job.submitted_at + expected_wait + runtime_s > job.submitted_at + job.deadline_s

    if expected_wait < config.burst_threshold_s and not misses_deadline:
        return Decision(None, f"expected wait {_fmt_s(expected_wait)} is under the "
                              f"{_fmt_s(config.burst_threshold_s)} threshold", **base)

    if budget.active_cloud_jobs >= config.max_cloud_jobs:
        return Decision(None, f"cloud job limit reached ({config.max_cloud_jobs})", **base)

    candidates: list[_Candidate] = []
    excluded: list[str] = []
    for offer in offers:
        cost = offer.price_per_hour * (offer.startup_s + runtime_s) / 3600
        if offer.capacity <= 0:
            excluded.append(f"{offer.backend}: no capacity")
        elif budget.spend_rate_per_hour + offer.price_per_hour > config.max_spend_per_hour:
            excluded.append(f"{offer.backend}: would exceed ${config.max_spend_per_hour:g}/h spend cap")
        elif budget.spent_today + cost > config.daily_budget:
            excluded.append(f"{offer.backend}: daily budget ${config.daily_budget:g} used up")
        else:
            finish = now + offer.startup_s + runtime_s
            meets = job.deadline_s is None or finish <= job.submitted_at + job.deadline_s
            candidates.append(_Candidate(offer, cost, meets, time_saved_s=remaining - offer.startup_s))

    if not candidates:
        detail = "; ".join(excluded) if excluded else "no cloud backends configured"
        return Decision(None, f"cannot burst ({detail})", **base)

    def result(c: _Candidate, reason: str) -> Decision:
        return Decision(c.offer.backend, reason, cost_estimate=c.cost, price_per_hour=c.offer.price_per_hour, **base)

    # a deadline that local execution misses beats the cost-vs-speed rule
    if misses_deadline:
        meeting = [c for c in candidates if c.meets_deadline]
        if meeting:
            best = min(meeting, key=lambda c: (c.cost, c.offer.startup_s))
            return result(best, f"would miss its {_fmt_s(job.deadline_s)} deadline locally "
                                f"(expected wait {_fmt_s(expected_wait)}); ${best.cost:.4f} on {best.offer.backend}")

    faster = [c for c in candidates if c.time_saved_s > 0]
    if not faster:
        return Decision(None, f"cloud would not start sooner than the local queue ({_fmt_s(remaining)})", **base)

    rate = config.time_value_per_hour * 2 ** (job.priority / 10)
    if math.isinf(rate):
        best = min(faster, key=lambda c: (c.offer.startup_s, c.cost))
        return result(best, f"expected wait {_fmt_s(expected_wait)} ≥ threshold, fastest mode: "
                            f"${best.cost:.4f} on {best.offer.backend}")

    def value_of(saved_s: float) -> float:
        # a time value of 0 makes any saving worthless, even an infinite one (avoid inf * 0 = nan)
        return 0.0 if rate == 0 else saved_s / 3600 * rate

    for c in faster:
        c.net_benefit = value_of(c.time_saved_s) - c.cost
    best = max(faster, key=lambda c: (c.net_benefit, -c.cost))
    if not best.net_benefit > 0:
        value = value_of(best.time_saved_s)
        return Decision(None, f"saving {_fmt_s(best.time_saved_s)} is worth ${value:.4f}, less than "
                              f"${best.cost:.4f} on {best.offer.backend} ({config.mode} mode)", **base)
    return result(best, f"expected wait {_fmt_s(expected_wait)} ≥ {_fmt_s(config.burst_threshold_s)}: "
                        f"saves {_fmt_s(best.time_saved_s)} for ${best.cost:.4f} on {best.offer.backend}")
