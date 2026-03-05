import datetime as dt

import pytz
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterSource, State as ClusterState

LIFECYCLE_COLORS = {
    "RUNNING": "🟢",
    "PENDING": "🟡",
    "QUEUED": "🟡",
    "BLOCKED": "🟡",
    "WAITING": "🟡",
    "TERMINATING": "🟠",
}

ACTIVE_STATES = {"RUNNING", "PENDING", "QUEUED", "BLOCKED", "WAITING", "TERMINATING"}

# Cluster states that mean the cluster is not yet ready
_CLUSTER_PENDING = {ClusterState.PENDING, ClusterState.RESIZING, ClusterState.RESTARTING}


def get_lifecycle_str(run, cluster_states: dict) -> str:
    """Return lifecycle state string.

    When the run lifecycle is RUNNING but its cluster is still PENDING/RESIZING,
    returns PENDING — matching the Databricks UI behaviour.
    """
    # New API (v2.1+): run.status.state is RunLifecycleStateV2State
    if run.status and run.status.state:
        lcs = run.status.state.value
    elif run.state and run.state.life_cycle_state:
        lcs = run.state.life_cycle_state.value
    else:
        return "—"

    # If lifecycle says RUNNING but the cluster isn't ready yet → PENDING
    if lcs == "RUNNING":
        cluster_id = run.cluster_instance.cluster_id if run.cluster_instance else None
        if cluster_id is None:
            return "PENDING"
        c_state = cluster_states.get(cluster_id)
        if c_state in _CLUSTER_PENDING:
            return "PENDING"

    return lcs


def build_cluster_states(all_clusters) -> dict:
    """Build cluster_id → ClusterState map from an already-fetched cluster list."""
    return {
        c.cluster_id: c.state
        for c in all_clusters
        if c.cluster_source == ClusterSource.JOB and c.cluster_id
    }


def render(w, active_runs, cluster_states, tz, selected_tz, key_prefix="jobs_runs"):
    """Render the Active Job Runs table. Can be called from other pages."""
    total = len(active_runs)
    running = sum(1 for r in active_runs if get_lifecycle_str(r, cluster_states) == "RUNNING")
    pending = sum(
        1 for r in active_runs
        if get_lifecycle_str(r, cluster_states) in ("PENDING", "QUEUED", "BLOCKED", "WAITING")
    )

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Active", total)
    col2.metric("Running", running)
    col3.metric("Pending / Queued", pending)

    st.divider()

    if not active_runs:
        st.info("No active job runs found.")
        return

    # Show action result from previous rerun
    if "run_action_result" in st.session_state:
        result = st.session_state.pop("run_action_result")
        if result["success"]:
            st.success(result["message"])
        else:
            st.error(result["message"])

    # Table header
    header_cols = st.columns([0.15, 1.8, 0.7, 0.7, 0.9, 1.2, 0.5])
    for col, h in zip(
        header_cols,
        [None, "Run Name", "Job ID", "Run ID", "State", f"Start Time ({selected_tz})", None],
    ):
        if h:
            col.markdown(f"**{h}**")

    st.divider()

    for i, run in enumerate(active_runs):
        lcs_str = get_lifecycle_str(run, cluster_states)
        indicator = LIFECYCLE_COLORS.get(lcs_str, "⚪")

        run_name = run.run_name or f"run-{run.run_id}"

        if run.start_time:
            start_dt = dt.datetime.fromtimestamp(run.start_time / 1000, tz=pytz.utc).astimezone(tz)
            start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            start_str = "—"

        row_cols = st.columns([0.15, 1.8, 0.7, 0.7, 0.9, 1.2, 0.5])
        row_cols[0].write(indicator)

        if run.run_page_url:
            row_cols[1].markdown(f"[{run_name}]({run.run_page_url})")
        else:
            row_cols[1].write(run_name)

        row_cols[2].write(str(run.job_id) if run.job_id else "—")
        row_cols[3].write(str(run.run_id) if run.run_id else "—")

        state_msg = run.state.state_message if run.state and run.state.state_message else ""
        if state_msg:
            row_cols[4].markdown(
                f"{lcs_str}<br><span style='color:gray; font-size:0.82em'>{state_msg}</span>",
                unsafe_allow_html=True,
            )
        else:
            row_cols[4].write(lcs_str)

        row_cols[5].write(start_str)

        can_cancel = lcs_str in ACTIVE_STATES

        if row_cols[6].button(
            "⏹",
            key=f"{key_prefix}_cancel_{i}",
            disabled=not can_cancel,
            use_container_width=True,
            help="Cancel run",
        ):
            try:
                w.jobs.cancel_run(run_id=run.run_id)
                st.session_state["run_action_result"] = {
                    "success": True,
                    "message": f"Run '{run_name}' (run_id={run.run_id}) cancellation requested.",
                }
            except Exception as e:
                st.session_state["run_action_result"] = {
                    "success": False,
                    "message": f"Failed to cancel run '{run_name}': {e}",
                }
            st.rerun()

        st.divider()


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

st.header("Active Job Runs")

selected_tz = st.selectbox("Timezone", options=COMMON_TZ, index=0, key="jobs_runs_tz")
tz = pytz.timezone(selected_tz)

w = WorkspaceClient(profile="DEFAULT")

with st.spinner("Fetching active job runs..."):
    try:
        active_runs = list(w.jobs.list_runs(active_only=True, expand_tasks=False))
    except Exception as e:
        st.error(f"Failed to fetch active runs: {e}")
        st.stop()

try:
    cluster_states = build_cluster_states(list(w.clusters.list()))
except Exception:
    cluster_states = {}

render(w, active_runs, cluster_states, tz, selected_tz, key_prefix="jobs_runs_page")
