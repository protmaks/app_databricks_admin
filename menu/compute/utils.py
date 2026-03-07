"""Shared pure-logic helpers used by multiple menu pages."""
import os

from databricks.sdk import WorkspaceClient

COMMON_TZ = [
    "UTC",
    "US/Eastern",
    "US/Central",
    "US/Pacific",
    "Europe/London",
    "Europe/Berlin",
    "Europe/Moscow",
    "Asia/Tokyo",
    "Asia/Shanghai",
    "Australia/Sydney",
]

# Maximum number of events to fetch per cluster from the Databricks Events API
MAX_CLUSTER_EVENTS = 500


def make_workspace_client(user_token: str | None = None) -> WorkspaceClient:
    """Return a WorkspaceClient for Databricks Apps or local development.

    In Databricks Apps pass the forwarded user token (X-Forwarded-Access-Token)
    to act on behalf of the logged-in user.  Falls back to SP OAuth or a local
    DEFAULT profile when no token is supplied.
    """
    host = os.getenv("DATABRICKS_HOST")
    if user_token and host:
        # Use the end-user's forwarded access token.
        # Pop OAuth env vars so the SDK doesn't see conflicting auth methods.
        _saved_id = os.environ.pop("DATABRICKS_CLIENT_ID", None)
        _saved_secret = os.environ.pop("DATABRICKS_CLIENT_SECRET", None)
        try:
            client = WorkspaceClient(host=host, token=user_token)
        finally:
            if _saved_id is not None:
                os.environ["DATABRICKS_CLIENT_ID"] = _saved_id
            if _saved_secret is not None:
                os.environ["DATABRICKS_CLIENT_SECRET"] = _saved_secret
        return client
    if host and os.getenv("DATABRICKS_CLIENT_ID"):
        # SP OAuth — pop DATABRICKS_TOKEN so the SDK doesn't see a conflicting PAT
        _saved = os.environ.pop("DATABRICKS_TOKEN", None)
        try:
            client = WorkspaceClient()
        finally:
            if _saved is not None:
                os.environ["DATABRICKS_TOKEN"] = _saved
        return client
    if os.getenv("DATABRICKS_TOKEN"):
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


def match_team_rules(job_name: str, creator: str, teams_config: list, tags: dict | None = None) -> list[str]:
    """Return list of team names whose rules match the given job_name / creator / tags.

    Conditions are evaluated left-to-right. Each condition (from the second onwards)
    carries its own 'logic' key ("AND" or "OR") that connects it to the accumulated
    result. The team-level 'logic' field is used as fallback for conditions that lack
    their own key (backward compatibility).
    """
    job_tags = tags or {}
    matched = []
    for team in teams_config:
        conditions = team.get("conditions", [])
        fallback_logic = team.get("logic", "OR").upper()
        if not conditions:
            continue

        def _eval(cond):
            field = cond.get("field")
            op = cond.get("operator", "")
            if field == "tags":
                tag_key = cond.get("tag_key", "")
                if op == "has_key":
                    return tag_key.lower() in {k.lower() for k in job_tags}
                tag_val = next((v for k, v in job_tags.items() if k.lower() == tag_key.lower()), None)
                if tag_val is None:
                    return False
                s, v = tag_val.lower(), cond.get("value", "").lower()
            else:
                subject = job_name if field == "job_name" else creator
                s, v = subject.lower(), cond.get("value", "").lower()
            if op == "starts_with":
                return s.startswith(v)
            elif op == "ends_with":
                return s.endswith(v)
            elif op == "contains":
                return v in s
            elif op == "equals":
                return s == v
            return False

        result = _eval(conditions[0])
        for cond in conditions[1:]:
            logic = cond.get("logic", fallback_logic).upper()
            hit = _eval(cond)
            result = (result and hit) if logic == "AND" else (result or hit)

        if result:
            matched.append(team["name"])
    return matched
