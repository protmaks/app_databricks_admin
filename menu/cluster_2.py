import os
import datetime as dt
import pytz
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import State as WarehouseState

from menu.utils import estimate_warehouse_dbu

APP_NAME = os.getenv("DATABRICKS_APP_NAME")

st.header("SQL Warehouses")

COMMON_TZ = ["UTC", "US/Eastern", "US/Central", "US/Pacific", "Europe/London", "Europe/Berlin",
             "Europe/Moscow", "Asia/Tokyo", "Asia/Shanghai", "Australia/Sydney"]
selected_tz = st.selectbox("Timezone", options=COMMON_TZ, index=0, key="warehouse_tz")
tz = pytz.timezone(selected_tz)

w = WorkspaceClient()
warehouses = list(w.warehouses.list())

# Summary metrics
total = len(warehouses)
running = sum(1 for wh in warehouses if wh.state == WarehouseState.RUNNING)
stopped = sum(1 for wh in warehouses if wh.state == WarehouseState.STOPPED)
starting = sum(1 for wh in warehouses if wh.state == WarehouseState.STARTING)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total", total)
col2.metric("Running", running)
col3.metric("Stopped", stopped)
col4.metric("Starting", starting)

st.divider()

STATE_COLORS = {
    WarehouseState.RUNNING: "🟢",
    WarehouseState.STARTING: "🟡",
    WarehouseState.STOPPING: "🟠",
    WarehouseState.STOPPED: "🔴",
    WarehouseState.DELETING: "🔴",
    WarehouseState.DELETED: "⚫",
}


if not warehouses:
    st.info("No SQL warehouses found.")
else:
    # Helper to apply auto-stop edit
    def apply_auto_stop(wh_id, new_minutes):
        wh_info = w.warehouses.get(wh_id)
        edit_kwargs = dict(
            id=wh_id,
            name=wh_info.name,
            cluster_size=wh_info.cluster_size,
            auto_stop_mins=int(new_minutes),
            min_num_clusters=wh_info.min_num_clusters,
            max_num_clusters=wh_info.max_num_clusters,
            enable_photon=wh_info.enable_photon,
            enable_serverless_compute=wh_info.enable_serverless_compute,
            spot_instance_policy=wh_info.spot_instance_policy,
            warehouse_type=wh_info.warehouse_type,
        )
        if wh_info.tags:
            edit_kwargs["tags"] = wh_info.tags
        if wh_info.channel:
            edit_kwargs["channel"] = wh_info.channel
        edit_kwargs = {k: v for k, v in edit_kwargs.items() if v is not None}
        return w.warehouses.edit(**edit_kwargs)

    # Table header
    header_cols = st.columns([0.3, 1.5, 1, 0.7, 0.8, 0.9, 0.7, 0.6, 0.5])
    for col, h in zip(header_cols, [None, "Name", "Creator", "Size", "Min/Max", "DBU/hr (min-max)", "Auto-Stop", "New (min)", None]):
        if h:
            col.markdown(f"**{h}**")

    st.divider()

    for i, wh in enumerate(warehouses):
        # Auto-stop
        if wh.auto_stop_mins and wh.auto_stop_mins > 0:
            auto_stop = f"{wh.auto_stop_mins} min"
        else:
            auto_stop = "Disabled"

        indicator = STATE_COLORS.get(wh.state, "⚪")
        current_val = wh.auto_stop_mins or 0
        min_max = f"{wh.min_num_clusters or '-'} / {wh.max_num_clusters or '-'}"

        min_dbu, max_dbu = estimate_warehouse_dbu(wh.cluster_size, wh.min_num_clusters, wh.max_num_clusters)
        dbu_str = f"{min_dbu} - {max_dbu}" if min_dbu != max_dbu else f"{min_dbu}"

        with st.form(key=f"as_form_{i}"):
            row_cols = st.columns([0.3, 1.5, 1, 0.7, 0.8, 0.9, 0.7, 0.6, 0.5])
            row_cols[0].write(indicator)
            row_cols[1].markdown(f"{wh.name}<br><span style='color:gray'>({wh.id})</span>", unsafe_allow_html=True)
            row_cols[2].write(wh.creator_name or "—")
            row_cols[3].write(wh.cluster_size or "—")
            row_cols[4].write(min_max)
            row_cols[5].write(dbu_str)
            row_cols[6].write(auto_stop)
            new_val = row_cols[7].number_input("min", min_value=0, max_value=1440, value=current_val, step=10, key=f"as_{i}", label_visibility="collapsed")
            submitted = row_cols[8].form_submit_button("Apply")
        if submitted:
            try:
                result = apply_auto_stop(wh.id, new_val)
                st.success(f"Auto-stop updated to {new_val} min for {wh.name}.")
                st.info(f"API response: {result}")
            except Exception as e:
                st.error(f"Failed to update {wh.name}: {e}")
