import datetime as dt
import os
import time
import pytz
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import ApplicationState, ComputeState

APP_NAME = os.getenv("DATABRICKS_APP_NAME")

APP_STATE_COLORS = {
    ApplicationState.RUNNING: "🟢",
    ApplicationState.DEPLOYING: "🟡",
    ApplicationState.CRASHED: "🔴",
    ApplicationState.UNAVAILABLE: "⚫",
}

COMPUTE_STATE_COLORS = {
    ComputeState.ACTIVE: "🟢",
    ComputeState.STARTING: "🟡",
    ComputeState.STOPPED: "🔘",
    ComputeState.ERROR: "🔴",
    ComputeState.DELETING: "🟠",
    ComputeState.UPDATING: "🟡",
}


def get_indicator(app):
    if app.app_status and app.app_status.state:
        return APP_STATE_COLORS.get(app.app_status.state, "⚪")
    if app.compute_status and app.compute_status.state:
        return COMPUTE_STATE_COLORS.get(app.compute_status.state, "⚪")
    return "⚪"


def can_start(app):
    compute = app.compute_status.state if app.compute_status else None
    app_st = app.app_status.state if app.app_status else None
    return compute == ComputeState.STOPPED or app_st == ApplicationState.CRASHED


def can_stop(app):
    compute = app.compute_status.state if app.compute_status else None
    app_st = app.app_status.state if app.app_status else None
    return compute in (ComputeState.ACTIVE, ComputeState.STARTING, ComputeState.UPDATING) \
        or app_st in (ApplicationState.RUNNING, ApplicationState.DEPLOYING)


def render(w, apps, tz, selected_tz, key_prefix="apps"):
    """Render the Apps table. Can be called from other pages."""
    total = len(apps)
    running = sum(1 for a in apps if a.app_status and a.app_status.state == ApplicationState.RUNNING)
    stopped = sum(1 for a in apps if a.compute_status and a.compute_status.state == ComputeState.STOPPED)
    starting = sum(
        1
        for a in apps
        if a.compute_status and a.compute_status.state == ComputeState.STARTING
        or a.app_status and a.app_status.state == ApplicationState.DEPLOYING
    )
    crashed = sum(1 for a in apps if a.app_status and a.app_status.state == ApplicationState.CRASHED)

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total", total)
    col2.metric("Running", running)
    col3.metric("Stopped", stopped)
    col4.metric("Starting", starting)
    col5.metric("Crashed", crashed)

    st.divider()

    if not apps:
        st.info("No Databricks Apps found.")
        return

    # Show action result from previous rerun
    if "app_action_result" in st.session_state:
        result = st.session_state.pop("app_action_result")
        if result["success"]:
            st.success(result["message"])
        else:
            st.error(result["message"])

    # Table header
    header_cols = st.columns([0.2, 1.5, 0.9, 0.9, 2.0, 1.2, 0.5, 0.5])
    for col, h in zip(
        header_cols,
        [None, "Name", "App State", "Compute", "URL", f"Update Time ({selected_tz})", None, None],
    ):
        if h:
            col.markdown(f"**{h}**")

    st.divider()

    for i, app in enumerate(apps):
        indicator = get_indicator(app)
        app_state_str = app.app_status.state.value if (app.app_status and app.app_status.state) else "—"
        compute_str = app.compute_status.state.value if (app.compute_status and app.compute_status.state) else "—"
        app_url = app.url or "—"

        # Update time
        if app.update_time:
            try:
                update_utc = dt.datetime.fromisoformat(str(app.update_time).replace("Z", "+00:00"))
                update_str = update_utc.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                update_str = str(app.update_time)
        else:
            update_str = "—"

        row_cols = st.columns([0.2, 1.5, 0.9, 0.9, 2.0, 1.2, 0.5, 0.5])
        row_cols[0].write(indicator)
        row_cols[1].markdown(
            f"{app.name}<br><span style='color:gray; font-size:0.85em'>{app.description or ''}</span>",
            unsafe_allow_html=True,
        )
        row_cols[2].write(app_state_str)
        row_cols[3].write(compute_str)
        if app_url != "—":
            row_cols[4].markdown(f"[{app_url}]({app_url})")
        else:
            row_cols[4].write("—")
        row_cols[5].write(update_str)

        start_disabled = not can_start(app)
        stop_disabled = not can_stop(app)

        if row_cols[6].button("▶", key=f"{key_prefix}_start_{i}", disabled=start_disabled, use_container_width=True, help="Start"):
            try:
                w.apps.start(app.name)
                st.session_state["app_action_result"] = {
                    "success": True,
                    "message": f"App '{app.name}' is starting.",
                }
            except Exception as e:
                st.session_state["app_action_result"] = {
                    "success": False,
                    "message": f"Failed to start '{app.name}': {e}",
                }
            st.rerun()

        if row_cols[7].button("⏹", key=f"{key_prefix}_stop_{i}", disabled=stop_disabled, use_container_width=True, help="Stop"):
            try:
                w.apps.stop(app.name)
                st.session_state["app_action_result"] = {
                    "success": True,
                    "message": f"App '{app.name}' is stopping.",
                }
            except Exception as e:
                st.session_state["app_action_result"] = {
                    "success": False,
                    "message": f"Failed to stop '{app.name}': {e}",
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

st.header("Databricks Apps")
selected_tz = st.selectbox("Timezone", options=COMMON_TZ, index=0, key="apps_tz")
tz = pytz.timezone(selected_tz)

w = WorkspaceClient(profile="DEFAULT")
apps = list(w.apps.list())

render(w, apps, tz, selected_tz, key_prefix="apps_page")
