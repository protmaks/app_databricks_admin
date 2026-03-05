import pytz
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterSource, State as ClusterState
from databricks.sdk.service.sql import State as WarehouseState
from databricks.sdk.service.apps import ApplicationState, ComputeState as AppComputeState
from databricks.sdk.service.database import DatabaseInstanceState

st.header("Active Compute")

w = WorkspaceClient(profile="DEFAULT")

# ── Fetch all data ─────────────────────────────────────────────────────────────
with st.spinner("Loading compute data..."):
    all_clusters = list(w.clusters.list())
    warehouses   = list(w.warehouses.list())
    apps         = list(w.apps.list())
    try:
        lb_instances = list(w.database.list_database_instances())
    except Exception:
        lb_instances = []
    try:
        active_runs = list(w.jobs.list_runs(active_only=True, expand_tasks=False))
    except Exception:
        active_runs = []

# Active = running + starting/pending states
ALLPURP_ACTIVE = (ClusterState.RUNNING, ClusterState.PENDING, ClusterState.RESIZING, ClusterState.RESTARTING)
JOBS_ACTIVE    = (ClusterState.RUNNING, ClusterState.PENDING)
WH_ACTIVE      = (WarehouseState.RUNNING, WarehouseState.STARTING)
LB_ACTIVE      = (DatabaseInstanceState.AVAILABLE, DatabaseInstanceState.STARTING, DatabaseInstanceState.UPDATING)

def app_is_active(a):
    app_st = a.app_status.state if a.app_status else None
    comp_st = a.compute_status.state if a.compute_status else None
    return (
        app_st in (ApplicationState.RUNNING, ApplicationState.DEPLOYING)
        or comp_st in (AppComputeState.ACTIVE, AppComputeState.STARTING, AppComputeState.UPDATING)
    )

allpurp_active = [
    c for c in all_clusters
    if c.cluster_source not in (ClusterSource.JOB, ClusterSource.PIPELINE, ClusterSource.PIPELINE_MAINTENANCE)
    and c.state in ALLPURP_ACTIVE
]
jobs_active = [
    c for c in all_clusters
    if c.cluster_source == ClusterSource.JOB
    and c.state in JOBS_ACTIVE
]
wh_active   = [wh for wh in warehouses   if wh.state in WH_ACTIVE]
apps_active  = [a  for a  in apps         if app_is_active(a)]
lb_active    = [i  for i  in lb_instances if i.state in LB_ACTIVE]




# ── Tabs ──────────────────────────────────────────────────────────────────────
def _fmt(n): return f"**{n}**" if n else str(n)

tab_ap, tab_wh, tab_job, tab_app, tab_lb = st.tabs([
    f":material/desktop_windows: All-Purpose ({_fmt(len(allpurp_active))})",
    f":material/cloud: SQL Warehouses ({_fmt(len(wh_active))})",
    f":material/check_circle: Jobs Compute ({_fmt(len(active_runs))})",
    f":material/apps: Apps ({_fmt(len(apps_active))})",
    f":material/apps: Lakebase ({_fmt(len(lb_active))})",
])


# ── All-Purpose ───────────────────────────────────────────────────────────────
with tab_ap:
    from menu.compute.compute_allpurp import render as render_allpurp
    allpurp_all = [
        c for c in all_clusters
        if c.cluster_source not in (ClusterSource.JOB, ClusterSource.PIPELINE, ClusterSource.PIPELINE_MAINTENANCE)
    ]
    render_allpurp(w, allpurp_all, tz, selected_tz, key_prefix="compute_all_ap")


# ── SQL Warehouses ────────────────────────────────────────────────────────────
with tab_wh:
    from menu.compute.compute_sqlwh import render as render_wh
    render_wh(w, warehouses, all_clusters, tz, selected_tz, key_prefix="compute_all_wh")


# ── Jobs Compute ──────────────────────────────────────────────────────────────
with tab_job:
    from menu.compute.compute_jobs_runs import build_cluster_states, render as render_runs
    cluster_states = build_cluster_states(all_clusters)
    render_runs(w, active_runs, cluster_states, tz, selected_tz, key_prefix="compute_all_runs")


# ── Apps ──────────────────────────────────────────────────────────────────────
with tab_app:
    from menu.compute.compute_apps import render as render_apps
    render_apps(w, apps, tz, selected_tz, key_prefix="compute_all_apps")


# ── Lakebase ──────────────────────────────────────────────────────────────────
with tab_lb:
    from menu.compute.compute_lakebase import render as render_lb
    render_lb(w, lb_instances, tz, selected_tz, key_prefix="compute_all_lb")


