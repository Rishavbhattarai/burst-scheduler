import math

import pytest

from burst.models import Job
from burst.policy import BudgetState, Offer, PolicyConfig, decide

NOW = 1000.0


def job(waited: float = 0.0, **kw) -> Job:
    return Job(command=["true"], submitted_at=NOW - waited, **kw)


CHEAP_SLOW = Offer("batch", price_per_hour=0.10, startup_s=60, capacity=10)
PRICEY_FAST = Offer("k8s", price_per_hour=0.40, startup_s=10, capacity=10)


def run(j, remaining, offers=(CHEAP_SLOW, PRICEY_FAST), budget=None, runtime=60, **config):
    return decide(j, NOW, remaining, runtime, list(offers), budget or BudgetState(), PolicyConfig(**config))


def test_below_threshold_stays_local():
    d = run(job(waited=10), remaining=30, burst_threshold_s=60)
    assert d.backend is None
    assert "under the 60s threshold" in d.reason
    assert d.expected_local_wait_s == 40


def test_waiting_time_so_far_counts_towards_the_threshold():
    d = run(job(waited=50), remaining=20, burst_threshold_s=60, mode="fastest")
    assert d.backend == "k8s"


def test_disabled():
    assert run(job(), remaining=1e6, enabled=False).reason == "bursting is disabled"


def test_no_local_workers_means_infinite_wait():
    d = run(job(), remaining=None, mode="fastest")
    assert d.backend == "k8s" and d.expected_local_wait_s is None
    assert "∞" in d.reason


def test_balanced_picks_largest_net_benefit():
    # remaining local wait 1 h. value $2/h:
    #   batch: saves 3540 s -> $1.967, costs 0.10 * 120/3600 = $0.0033 -> net 1.963
    #   k8s:   saves 3590 s -> $1.994, costs 0.40 *  70/3600 = $0.0078 -> net 1.987
    d = run(job(), remaining=3600, mode="balanced")
    assert d.backend == "k8s"
    assert d.cost_estimate == pytest.approx(0.40 * 70 / 3600)
    assert d.price_per_hour == 0.40
    assert "saves 3590s" in d.reason


def test_balanced_stays_local_when_saving_is_not_worth_the_cost():
    # 30 s saved at $2/h = $0.0167 < cost of a 10 h job on k8s ($4)
    d = run(job(), remaining=70, runtime=36_000, burst_threshold_s=60, mode="balanced")
    assert d.backend is None
    assert "less than" in d.reason and "balanced mode" in d.reason


def test_priority_raises_the_value_of_time():
    # same job as above but priority 100: value x 1024 -> $17 > $4
    d = run(job(priority=100), remaining=70, runtime=36_000, burst_threshold_s=60, mode="balanced")
    assert d.backend is not None


def test_cheapest_mode_never_bursts_for_speed():
    d = run(job(), remaining=10_000, mode="cheapest")
    assert d.backend is None


def test_cheapest_mode_without_local_workers():
    # infinite local wait x $0/h must not become nan (which slipped past the "worth it" check)
    d = run(job(), remaining=None, mode="cheapest")
    assert d.backend is None and "cheapest mode" in d.reason


def test_fastest_mode_picks_quickest_start():
    assert run(job(), remaining=100, mode="fastest").backend == "k8s"


def test_cloud_must_actually_be_faster():
    # local slot expected in 65 s, threshold 60: batch (60 s startup) saves 5 s, k8s saves 55 s
    assert run(job(), remaining=65, mode="fastest").backend == "k8s"
    d = run(job(), remaining=65, offers=[Offer("slow", 0.1, startup_s=120, capacity=5)], mode="fastest")
    assert d.backend is None and "would not start sooner" in d.reason


def test_deadline_overrides_cheapest_mode():
    # 5 min deadline, 10 min expected local wait: cheapest backend that still meets it
    j = job(deadline_s=300)
    d = run(j, remaining=600, mode="cheapest")
    assert d.backend == "batch"   # 60 + 60 s < 300 s, and cheaper than k8s
    assert "deadline" in d.reason


def test_deadline_only_fast_backend_meets_it():
    j = job(deadline_s=100)
    assert run(j, remaining=600, mode="cheapest").backend == "k8s"   # batch would finish at 120 s


def test_deadline_nobody_meets_falls_back_to_value_rule():
    j = job(deadline_s=30)
    assert run(j, remaining=600, mode="cheapest").backend is None
    assert run(j, remaining=600, mode="fastest").backend == "k8s"


def test_job_limit():
    d = run(job(), remaining=1e4, budget=BudgetState(active_cloud_jobs=3), max_cloud_jobs=3, mode="fastest")
    assert d.backend is None and "limit reached (3)" in d.reason


def test_spend_rate_cap_excludes_expensive_backend():
    budget = BudgetState(spend_rate_per_hour=0.8)
    d = run(job(), remaining=1e4, budget=budget, max_spend_per_hour=1.0, mode="fastest")
    assert d.backend == "batch"   # 0.8 + 0.4 > 1.0, 0.8 + 0.1 <= 1.0
    d = run(job(), remaining=1e4, budget=BudgetState(spend_rate_per_hour=0.95), max_spend_per_hour=1.0)
    assert d.backend is None and "spend cap" in d.reason


def test_daily_budget():
    d = run(job(), remaining=1e4, budget=BudgetState(spent_today=50.0), daily_budget=50.0, mode="fastest")
    assert d.backend is None and "daily budget" in d.reason


def test_capacity_and_no_backends():
    full = [Offer("k8s", 0.4, 10, capacity=0)]
    assert "no capacity" in run(job(), remaining=1e4, offers=full).reason
    assert "no cloud backends" in run(job(), remaining=1e4, offers=[]).reason


def test_value_per_hour_overrides_mode():
    config = PolicyConfig(mode="cheapest", value_per_hour=100.0)
    assert config.time_value_per_hour == 100.0
    assert PolicyConfig(mode="fastest").time_value_per_hour == math.inf
    with pytest.raises(ValueError):
        PolicyConfig(mode="yolo")
    with pytest.raises(ValueError):
        PolicyConfig(colour="red")   # typos in burst.toml are errors, not silently ignored


def test_budget_day_starts_at_utc_midnight():
    from burst.controller import start_of_day
    assert start_of_day(1_790_000_000.0) == 1_789_948_800.0   # 2026-09-21 14:13:20 UTC
