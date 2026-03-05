import datetime as dt
import pytz
import streamlit as st
from databricks.sdk import WorkspaceClient
from menu.compute.utils import make_workspace_client
from databricks.sdk.service.database import DatabaseInstance, DatabaseInstanceState
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

STATE_COLORS = {
    DatabaseInstanceState.AVAILABLE: "🟢",
    DatabaseInstanceState.STARTING: "🟡",
    DatabaseInstanceState.UPDATING: "🟡",
    DatabaseInstanceState.FAILING_OVER: "🟠",
    DatabaseInstanceState.STOPPED: "🔘",
    DatabaseInstanceState.DELETING: "⚫",
}


def get_indicator(inst):
    return STATE_COLORS.get(inst.state, "⚪")


def can_start(inst):
    return inst.state == DatabaseInstanceState.STOPPED


def can_stop(inst):
    return inst.state in (
        DatabaseInstanceState.AVAILABLE,
        DatabaseInstanceState.STARTING,
        DatabaseInstanceState.UPDATING,
    )


def fmt_time(ts_str, tz):
    if not ts_str:
        return "—"
    try:
        utc_dt = dt.datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
        return utc_dt.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts_str)


def render(w, instances, tz, selected_tz, key_prefix="lb"):
    """Render the Lakebase table. Can be called from other pages."""
    total = len(instances)
    available = sum(1 for i in instances if i.state == DatabaseInstanceState.AVAILABLE)
    stopped = sum(1 for i in instances if i.state == DatabaseInstanceState.STOPPED)
    starting = sum(1 for i in instances if i.state == DatabaseInstanceState.STARTING)
    error = sum(1 for i in instances if i.state == DatabaseInstanceState.FAILING_OVER)

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total", total)
    col2.metric("Available", available)
    col3.metric("Stopped", stopped)
    col4.metric("Starting", starting)
    col5.metric("Failing Over", error)

    st.divider()

    if not instances:
        st.info("No Lakebase instances found.")
        return

    if "lb_action_result" in st.session_state:
        result = st.session_state.pop("lb_action_result")
        if result["success"]:
            st.success(result["message"])
        else:
            st.error(result["message"])

    header_cols = st.columns([0.15, 1.0, 0.7, 0.5, 0.5, 1.8, 1.1, 1.1, 0.4])
    for col, h in zip(
        header_cols,
        [None, "Name", "State", "PG Ver", "Capacity", "Read/Write DNS", f"Created ({selected_tz})", "Creator", None],
    ):
        if h:
            col.markdown(f"**{h}**")

    st.divider()

    for i, inst in enumerate(instances):
        indicator = get_indicator(inst)
        state_str = inst.state.value if inst.state else "—"
        pg_ver = inst.pg_version or "—"
        capacity = inst.effective_capacity or inst.capacity or "—"
        dns = inst.read_write_dns or "—"
        creator = inst.creator or "—"
        created_str = fmt_time(inst.creation_time, tz)

        row_cols = st.columns([0.15, 1.0, 0.7, 0.5, 0.5, 1.8, 1.1, 1.1, 0.4])
        row_cols[0].write(indicator)
        row_cols[1].write(inst.name)
        row_cols[2].write(state_str)
        row_cols[3].write(pg_ver)
        row_cols[4].write(capacity)
        if dns != "—":
            row_cols[5].code(dns, language=None)
        else:
            row_cols[5].write("—")
        row_cols[6].write(created_str)
        row_cols[7].write(creator)

        if can_start(inst):
            btn_label, btn_help, btn_disabled = "▶", "Start", False
        elif can_stop(inst):
            btn_label, btn_help, btn_disabled = "⏹", "Stop", False
        else:
            btn_label, btn_help, btn_disabled = "—", "", True

        if row_cols[8].button(btn_label, key=f"{key_prefix}_action_{i}", disabled=btn_disabled, use_container_width=True, help=btn_help):
            if can_start(inst):
                try:
                    w.database.update_database_instance(
                        inst.name,
                        DatabaseInstance(name=inst.name, stopped=False),
                        update_mask="stopped",
                    )
                    st.session_state["lb_action_result"] = {"success": True, "message": f"Instance '{inst.name}' is starting."}
                except Exception as e:
                    st.session_state["lb_action_result"] = {"success": False, "message": f"Failed to start '{inst.name}': {e}"}
            else:
                try:
                    w.database.update_database_instance(
                        inst.name,
                        DatabaseInstance(name=inst.name, stopped=True),
                        update_mask="stopped",
                    )
                    st.session_state["lb_action_result"] = {"success": True, "message": f"Instance '{inst.name}' is stopping."}
                except Exception as e:
                    st.session_state["lb_action_result"] = {"success": False, "message": f"Failed to stop '{inst.name}': {e}"}
            st.rerun()

        st.divider()


st.header("Lakebase (Managed PostgreSQL)")
selected_tz = st.selectbox("Timezone", options=COMMON_TZ, index=0, key="lakebase_tz")
tz = pytz.timezone(selected_tz)

w = make_workspace_client()
instances = list(w.database.list_database_instances())

render(w, instances, tz, selected_tz, key_prefix="lb_page")
