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

def app_indicator(a):
    app_st = a.app_status.state if a.app_status else None
    comp_st = a.compute_status.state if a.compute_status else None
    if app_st == ApplicationState.RUNNING or comp_st == AppComputeState.ACTIVE:
        return "🟢"
    return "🟡"

def cluster_indicator(c):
    return "🟢" if c.state == ClusterState.RUNNING else "🟡"

def wh_indicator(wh):
    return "🟢" if wh.state == WarehouseState.RUNNING else "🟡"

def lb_indicator(inst):
    return "🟢" if inst.state == DatabaseInstanceState.AVAILABLE else "🟡"

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


def fmt_iso(ts_str):
    if not ts_str:
        return "—"
    try:
        utc = dt.datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
        return utc.astimezone(tz).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(ts_str)


# ── All-Purpose ───────────────────────────────────────────────────────────────
with tab_ap:
    if not allpurp_active:
        st.info("No active All-Purpose clusters.")
    else:
        h = st.columns([0.15, 0.7, 1.5, 1.2, 0.6, 1.1, 0.8])
        for col, label in zip(h, ["", "State", "Cluster Name", "Creator", "Workers", f"Start ({selected_tz})", "Uptime"]):
            col.markdown(f"**{label}**")
        st.divider()
        for c in allpurp_active:
            workers = (
                f"{c.autoscale.min_workers}–{c.autoscale.max_workers} auto"
                if c.autoscale else str(c.num_workers or 0)
            )
            start_ms = c.last_state_loss_time if c.state == ClusterState.RUNNING else None
            row = st.columns([0.15, 0.7, 1.5, 1.2, 0.6, 1.1, 0.8])
            row[0].write(cluster_indicator(c))
            row[1].write(c.state.value if c.state else "—")
            row[2].markdown(f"{c.cluster_name}<br><span style='color:gray;font-size:0.85em'>{c.cluster_id}</span>", unsafe_allow_html=True)
            row[3].write(c.creator_user_name or "—")
            row[4].write(workers)
            row[5].write(fmt_epoch_ms(start_ms))
            row[6].write(uptime_from_ms(start_ms))
            st.divider()


# ── SQL Warehouses ────────────────────────────────────────────────────────────
with tab_wh:
    if not wh_active:
        st.info("No active SQL Warehouses.")
    else:
        wh_start: dict[str, int] = {}
        for wh in wh_active:
            if wh.state != WarehouseState.RUNNING:
                continue
            for c in all_clusters:
                if (c.cluster_name and wh.id in c.cluster_name) or (c.custom_tags and wh.id in str(c.custom_tags)):
                    if c.last_state_loss_time:
                        wh_start[wh.id] = c.last_state_loss_time
                    break

        h = st.columns([0.15, 0.7, 1.5, 1.2, 0.6, 0.7, 1.1, 0.8])
        for col, label in zip(h, ["", "State", "Warehouse", "Creator", "Size", "Clusters", f"Start ({selected_tz})", "Uptime"]):
            col.markdown(f"**{label}**")
        st.divider()
        for wh in wh_active:
            start_ms = wh_start.get(wh.id)
            min_max = f"{wh.min_num_clusters or '?'}/{wh.max_num_clusters or '?'}"
            row = st.columns([0.15, 0.7, 1.5, 1.2, 0.6, 0.7, 1.1, 0.8])
            row[0].write(wh_indicator(wh))
            row[1].write(wh.state.value if wh.state else "—")
            row[2].markdown(f"{wh.name}<br><span style='color:gray;font-size:0.85em'>{wh.id}</span>", unsafe_allow_html=True)
            row[3].write(wh.creator_name or "—")
            row[4].write(wh.cluster_size or "—")
            row[5].write(min_max)
            row[6].write(fmt_epoch_ms(start_ms))
            row[7].write(uptime_from_ms(start_ms))
            st.divider()


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
            row[0].write(cluster_indicator(c))
            row[1].write(c.state.value if c.state else "—")
            row[2].markdown(f"{c.cluster_name}<br><span style='color:gray;font-size:0.85em'>{c.cluster_id}</span>", unsafe_allow_html=True)
            row[3].write(c.creator_user_name or "—")
            row[4].write(workers)
            row[5].write(fmt_epoch_ms(start_ms))
            row[6].write(uptime_from_ms(start_ms))
            st.divider()


# ── Apps ──────────────────────────────────────────────────────────────────────
with tab_app:
    if not apps_active:
        st.info("No active Apps.")
    else:
        h = st.columns([0.15, 0.9, 1.5, 0.9, 2.0, 1.1])
        for col, label in zip(h, ["", "App State", "App Name", "Compute", "URL", f"Updated ({selected_tz})"]):
            col.markdown(f"**{label}**")
        st.divider()
        for a in apps_active:
            app_st_str = a.app_status.state.value if (a.app_status and a.app_status.state) else "—"
            compute_str = a.compute_status.state.value if (a.compute_status and a.compute_status.state) else "—"
            url = a.url or "—"
            row = st.columns([0.15, 0.9, 1.5, 0.9, 2.0, 1.1])
            row[0].write(app_indicator(a))
            row[1].write(app_st_str)
            row[2].markdown(f"{a.name}<br><span style='color:gray;font-size:0.85em'>{a.description or ''}</span>", unsafe_allow_html=True)
            row[3].write(compute_str)
            row[4].markdown(f"[{url}]({url})" if url != "—" else "—")
            row[5].write(fmt_iso(a.update_time))
            st.divider()


# ── Lakebase ──────────────────────────────────────────────────────────────────
with tab_lb:
    if not lb_active:
        st.info("No active Lakebase instances." if lb_instances else "Lakebase not available in this workspace.")
    else:
        h = st.columns([0.15, 0.7, 1.2, 0.6, 0.6, 1.8, 1.1])
        for col, label in zip(h, ["", "State", "Instance", "PG Ver", "Capacity", "Read/Write DNS", f"Created ({selected_tz})"]):
            col.markdown(f"**{label}**")
        st.divider()
        for inst in lb_active:
            row = st.columns([0.15, 0.7, 1.2, 0.6, 0.6, 1.8, 1.1])
            row[0].write(lb_indicator(inst))
            row[1].write(inst.state.value if inst.state else "—")
            row[2].write(inst.name)
            row[3].write(inst.pg_version or "—")
            row[4].write(inst.effective_capacity or inst.capacity or "—")
            dns = inst.read_write_dns or "—"
            row[5].code(dns, language=None) if dns != "—" else row[5].write("—")
            row[6].write(fmt_iso(inst.creation_time))
            st.divider()
