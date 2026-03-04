"""Unit tests for menu.utils shared helpers."""

from types import SimpleNamespace

import pytest

from menu.utils import (
    estimate_dbu,
    estimate_warehouse_dbu,
    format_uptime,
    quartz_to_standard_cron,
    resolve_display_state,
    run_uses_cluster,
)


# ── quartz_to_standard_cron ──────────────────────────────────────


class TestQuartzToStandardCron:
    def test_valid_6_field(self):
        assert quartz_to_standard_cron("0 30 10 * * ?") == "30 10 * * *"

    def test_valid_7_field_with_year(self):
        assert quartz_to_standard_cron("0 0 12 ? * MON-FRI 2025") == "0 12 * * MON-FRI"

    def test_question_mark_replaced(self):
        result = quartz_to_standard_cron("0 0 0 ? * ? *")
        assert "?" not in result
        assert result == "0 0 * * *"

    def test_too_few_fields_returns_none(self):
        assert quartz_to_standard_cron("0 30 10") is None

    def test_five_fields_returns_none(self):
        assert quartz_to_standard_cron("0 30 10 * *") is None

    def test_whitespace_stripped(self):
        assert quartz_to_standard_cron("  0 15 6 * * ?  ") == "15 6 * * *"


# ── estimate_warehouse_dbu ───────────────────────────────────────


class TestEstimateWarehouseDbu:
    def test_known_size_small(self):
        assert estimate_warehouse_dbu("Small", 1, 1) == (12, 12)

    def test_known_size_xlarge(self):
        assert estimate_warehouse_dbu("X-Large", 1, 1) == (80, 80)

    def test_unknown_size_returns_zero(self):
        assert estimate_warehouse_dbu("Mega", 1, 1) == (0, 0)

    def test_min_neq_max_clusters(self):
        assert estimate_warehouse_dbu("Medium", 1, 3) == (24, 72)

    def test_none_clusters_default_to_one(self):
        assert estimate_warehouse_dbu("Small", None, None) == (12, 12)


# ── estimate_dbu ─────────────────────────────────────────────────


class TestEstimateDbu:
    NODE_TYPES = {
        "Standard_D4_v2": 8,
        "Standard_D8_v2": 16,
    }

    def test_known_node_types(self):
        # driver 8 cores -> 2 DBU, worker 16 cores * 2 -> 8 DBU
        assert estimate_dbu("Standard_D4_v2", "Standard_D8_v2", 2, 2, self.NODE_TYPES) == (10, 10)

    def test_unknown_node_types_return_zero(self):
        assert estimate_dbu("unknown_d", "unknown_w", 4, 4, {}) == (0, 0)

    def test_autoscale_range(self):
        min_dbu, max_dbu = estimate_dbu("Standard_D4_v2", "Standard_D8_v2", 1, 4, self.NODE_TYPES)
        assert min_dbu == 6   # 2 + 1*4
        assert max_dbu == 18  # 2 + 4*4

    def test_zero_workers(self):
        assert estimate_dbu("Standard_D4_v2", "Standard_D8_v2", 0, 0, self.NODE_TYPES) == (2, 2)


# ── run_uses_cluster ─────────────────────────────────────────────


def _make_run(cluster_id=None, spec_cluster_id=None, tasks=None):
    """Helper to build a minimal run-like namespace."""
    cluster_instance = SimpleNamespace(cluster_id=cluster_id) if cluster_id else None
    cluster_spec = SimpleNamespace(existing_cluster_id=spec_cluster_id) if spec_cluster_id else None
    return SimpleNamespace(
        cluster_instance=cluster_instance,
        cluster_spec=cluster_spec,
        tasks=tasks,
    )


class TestRunUsesCluster:
    def test_match_on_run_level_cluster_instance(self):
        run = _make_run(cluster_id="abc-123")
        assert run_uses_cluster(run, "abc-123") is True

    def test_match_on_run_level_cluster_spec(self):
        run = _make_run(spec_cluster_id="abc-123")
        assert run_uses_cluster(run, "abc-123") is True

    def test_match_on_task_level_cluster(self):
        task = SimpleNamespace(
            cluster_instance=SimpleNamespace(cluster_id="abc-123"),
            existing_cluster_id=None,
        )
        run = _make_run(tasks=[task])
        assert run_uses_cluster(run, "abc-123") is True

    def test_no_match(self):
        run = _make_run(cluster_id="other-id")
        assert run_uses_cluster(run, "abc-123") is False

    def test_no_tasks(self):
        run = _make_run()
        assert run_uses_cluster(run, "abc-123") is False


# ── resolve_display_state ────────────────────────────────────────


class TestResolveDisplayState:
    def test_running(self):
        assert resolve_display_state("RUNNING", None) == "RUNNING"

    def test_terminated_success(self):
        assert resolve_display_state("TERMINATED", "SUCCESS") == "SUCCESS"

    def test_terminated_failed(self):
        assert resolve_display_state("TERMINATED", "FAILED") == "FAILED"

    def test_pending(self):
        assert resolve_display_state("PENDING", None) == "PENDING"

    def test_queued(self):
        assert resolve_display_state("QUEUED", None) == "PENDING"

    def test_internal_error(self):
        assert resolve_display_state("INTERNAL_ERROR", None) == "FAILED"

    def test_terminated_timedout(self):
        assert resolve_display_state("TERMINATED", "TIMEDOUT") == "TIMEDOUT"

    def test_terminated_canceled(self):
        assert resolve_display_state("TERMINATED", "CANCELED") == "CANCELED"

    def test_terminated_no_result(self):
        assert resolve_display_state("TERMINATED", None) == "TERMINATED"

    def test_none_states(self):
        assert resolve_display_state(None, None) == "FAILED"

    def test_skipped(self):
        assert resolve_display_state("SKIPPED", None) == "FAILED"

    def test_terminating(self):
        assert resolve_display_state("TERMINATING", None) == "TERMINATING"


# ── format_uptime ────────────────────────────────────────────────


class TestFormatUptime:
    def test_zero(self):
        assert format_uptime(0) == "0d 0h 0m"

    def test_minutes_only(self):
        assert format_uptime(300) == "0d 0h 5m"

    def test_hours_and_minutes(self):
        assert format_uptime(3661) == "0d 1h 1m"

    def test_multi_day(self):
        assert format_uptime(90061) == "1d 1h 1m"

    def test_exact_day(self):
        assert format_uptime(86400) == "1d 0h 0m"
