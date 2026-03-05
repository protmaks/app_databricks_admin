import datetime as dt
import time
import pytz
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterSource, State as ClusterState
from databricks.sdk.service.sql import State as WarehouseState
from databricks.sdk.service.apps import ApplicationState, ComputeState as AppComputeState
from databricks.sdk.service.database import DatabaseInstanceState

from menu.utils import format_uptime

st.header("Main Statistics — Active Compute")

COMMON_TZ = [
    "UTC", "US/Eastern", "US/Central", "US/Pacific",
    "Europe/London", "Europe/Berlin", "Europe/Moscow",
    "Asia/Tokyo", "Asia/Shanghai", "Australia/Sydney",
]
selected_tz = st.selectbox("Timezone", options=COMMON_TZ, index=0, key="main_tz")
tz = pytz.timezone(selected_tz)

now_ms = int(time.time() * 1000)

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

# ── Summary row ───────────────────────────────────────────────────────────────
cols = st.columns(5)
cols[0].metric("All-Purpose", len(allpurp_active))
cols[1].metric("SQL Warehouses", len(wh_active))
cols[2].metric("Jobs Compute", len(jobs_active))
cols[3].metric("Apps", len(apps_active))
cols[4].metric("Lakebase", len(lb_active))

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_ap, tab_wh, tab_job, tab_app, tab_lb = st.tabs([
    f"🖥 All-Purpose ({len(allpurp_active)})",
    f"☁ SQL Warehouses ({len(wh_active)})",
    f"⚙ Jobs Compute ({len(jobs_active)})",
    f"📦 Apps ({len(apps_active)})",
    f"🐘 Lakebase ({len(lb_active)})",
])


def uptime_from_ms(start_ms):
    if not start_ms:
        return "—"
    secs = (now_ms - start_ms) // 1000
    return format_uptime(secs)


def fmt_epoch_ms(epoch_ms):
    if not epoch_ms:
        return "—"
    return dt.datetime.fromtimestamp(epoch_ms / 1000, tz=pytz.utc).astimezone(tz).strftime("%Y-%m-%d %H:%M")


# ── All-Purpose ───────────────────────────────────────────────────────────────
with tab_ap:
    from menu.compute_allpurp import render as render_allpurp
    allpurp_all = [
        c for c in all_clusters
        if c.cluster_source not in (ClusterSource.JOB, ClusterSource.PIPELINE, ClusterSource.PIPELINE_MAINTENANCE)
    ]
    render_allpurp(w, allpurp_all, tz, selected_tz, key_prefix="compute_all_ap")


# ── SQL Warehouses ────────────────────────────────────────────────────────────
with tab_wh:
    from menu.compute_sqlwh import render as render_wh
    render_wh(w, warehouses, all_clusters, tz, selected_tz, key_prefix="compute_all_wh")


# ── Jobs Compute ──────────────────────────────────────────────────────────────
with tab_job:
    if not jobs_active:
        st.info("No active Jobs Compute clusters.")
    else:
        h = st.columns([0.15, 0.7, 1.5, 1.2, 0.6, 1.1, 0.8])
        for col, label in zip(h, ["", "State", "Cluster Name", "Creator", "Workers", f"Start ({selected_tz})", "Uptime"]):
            col.markdown(f"**{label}**")
        st.divider()
        for c in jobs_active:
            workers = (
                f"{c.autoscale.min_workers}–{c.autoscale.max_workers} auto"
                if c.autoscale else str(c.num_workers or 0)
            )
            start_ms = c.last_state_loss_time if c.state == ClusterState.RUNNING else None
            row = st.columns([0.15, 0.7, 1.5, 1.2, 0.6, 1.1, 0.8])
            row[0].write("🟢" if c.state == ClusterState.RUNNING else "🟡")
            row[1].write(c.state.value if c.state else "—")
            row[2].markdown(f"{c.cluster_name}<br><span style='color:gray;font-size:0.85em'>{c.cluster_id}</span>", unsafe_allow_html=True)
            row[3].write(c.creator_user_name or "—")
            row[4].write(workers)
            row[5].write(fmt_epoch_ms(start_ms))
            row[6].write(uptime_from_ms(start_ms))
            st.divider()


# ── Apps ──────────────────────────────────────────────────────────────────────
with tab_app:
    from menu.compute_apps import render as render_apps
    render_apps(w, apps, tz, selected_tz, key_prefix="compute_all_apps")


# ── Lakebase ──────────────────────────────────────────────────────────────────
with tab_lb:
    from menu.compute_lakebase import render as render_lb
    render_lb(w, lb_instances, tz, selected_tz, key_prefix="compute_all_lb")
