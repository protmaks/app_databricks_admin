"""Shared pure-logic helpers used by multiple menu pages."""
import os

from databricks.sdk import WorkspaceClient


def make_workspace_client() -> WorkspaceClient:
    """Return a WorkspaceClient using SP credentials in Databricks Apps,
    falling back to profile='DEFAULT' for local development."""
    if os.getenv("DATABRICKS_CLIENT_ID"):
        # OAuth SP credentials available — explicitly clear token to avoid PAT conflict
        return WorkspaceClient(token="")
    if os.getenv("DATABRICKS_TOKEN"):
        # Only PAT available
        return WorkspaceClient()
    return WorkspaceClient(profile="DEFAULT")

# SQL Warehouse DBU rates per cluster (single cluster unit)
WAREHOUSE_SIZE_DBU = {
    "2X-Small": 4,
    "X-Small": 6,
    "Small": 12,
    "Medium": 24,
    "Large": 40,
    "X-Large": 80,
    "2X-Large": 144,
    "3X-Large": 272,
    "4X-Large": 528,
}


def quartz_to_standard_cron(quartz_expr: str) -> str | None:
    """Convert Quartz cron (sec min hr dom month dow [year]) to standard 5-field cron."""
    parts = quartz_expr.strip().split()
    if len(parts) < 6:
        return None
    # Drop seconds (field 0) and year (field 6) if present
    parts = parts[1:6]
    # Replace ? with *
    parts = [p.replace("?", "*") for p in parts]
    return " ".join(parts)


def estimate_warehouse_dbu(cluster_size, min_clusters, max_clusters):
    """Estimate DBU/hour range for a SQL warehouse."""
    base = WAREHOUSE_SIZE_DBU.get(cluster_size, 0)
    min_dbu = base * (min_clusters or 1)
    max_dbu = base * (max_clusters or 1)
    return min_dbu, max_dbu


def estimate_dbu(driver_type, worker_type, min_workers, max_workers, node_types):
    """Estimate DBU/hour range. All-purpose ≈ 1 DBU per 4 vCPUs."""
    driver_cores = node_types.get(driver_type, 0)
    worker_cores = node_types.get(worker_type, 0)
    driver_dbu = driver_cores // 4
    min_dbu = driver_dbu + min_workers * (worker_cores // 4)
    max_dbu = driver_dbu + max_workers * (worker_cores // 4)
    return min_dbu, max_dbu


def run_uses_cluster(run, cluster_id):
    """Check if a run used the given cluster (run-level or task-level)."""
    if run.cluster_instance and run.cluster_instance.cluster_id == cluster_id:
        return True
    if (
        run.cluster_spec
        and getattr(run.cluster_spec, "existing_cluster_id", None) == cluster_id
    ):
        return True
    if run.tasks:
        for task in run.tasks:
            if task.cluster_instance and task.cluster_instance.cluster_id == cluster_id:
                return True
            if getattr(task, "existing_cluster_id", None) == cluster_id:
                return True
    return False


def resolve_display_state(life_cycle_state, result_state):
    """Map lifecycle/result state pair to a display state for charts."""
    lcs = life_cycle_state
    rs = result_state

    if lcs == "RUNNING":
        return "RUNNING"
    elif lcs in ("PENDING", "QUEUED", "BLOCKED"):
        return "PENDING"
    elif lcs == "TERMINATING":
        return "TERMINATING"
    elif lcs == "INTERNAL_ERROR" or lcs == "SKIPPED":
        return "FAILED"
    elif lcs == "TERMINATED":
        state_map = {
            "SUCCESS": "SUCCESS",
            "FAILED": "FAILED",
            "TIMEDOUT": "TIMEDOUT",
            "CANCELED": "CANCELED",
            "INTERNAL_ERROR": "FAILED",
            "EXCLUDED": "CANCELED",
        }
        return state_map.get(rs, "FAILED" if rs else lcs)
    else:
        return lcs or "FAILED"


def format_uptime(total_seconds):
    """Format seconds into 'Xd Yh Zm' string."""
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    mins = rem // 60
    return f"{days}d {hours}h {mins}m"
