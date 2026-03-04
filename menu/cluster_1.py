import os
import datetime as dt
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterSource, State

APP_NAME = os.getenv("DATABRICKS_APP_NAME")

st.header("All-Purpose Clusters")

w = WorkspaceClient()
clusters = [c for c in w.clusters.list()
            if c.cluster_source not in (ClusterSource.JOB, ClusterSource.PIPELINE, ClusterSource.PIPELINE_MAINTENANCE)]

# Summary metrics
total = len(clusters)
running = sum(1 for c in clusters if c.state == State.RUNNING)
terminated = sum(1 for c in clusters if c.state == State.TERMINATED)
errors = sum(1 for c in clusters if c.state == State.ERROR)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total", total)
col2.metric("Running", running)
col3.metric("Terminated", terminated)
col4.metric("Errors", errors)

st.divider()

STATE_COLORS = {
    State.RUNNING: "🟢",
    State.PENDING: "🟡",
    State.RESIZING: "🟡",
    State.RESTARTING: "🟡",
    State.TERMINATED: "🔴",
    State.TERMINATING: "🟠",
    State.ERROR: "🔴",
    State.UNKNOWN: "⚪",
}

import time
now_epoch_ms = int(time.time() * 1000)

if not clusters:
    st.info("No clusters found.")
else:
    # Helper to apply auto-termination edit
    def apply_auto_termination(cluster_id, new_minutes):
        cluster_info = w.clusters.get(cluster_id)
        edit_kwargs = dict(
            cluster_id=cluster_id,
            cluster_name=cluster_info.cluster_name,
            spark_version=cluster_info.spark_version,
            node_type_id=cluster_info.node_type_id,
            driver_node_type_id=cluster_info.driver_node_type_id,
            autotermination_minutes=int(new_minutes),
            spark_conf=cluster_info.spark_conf,
            spark_env_vars=cluster_info.spark_env_vars,
            custom_tags=cluster_info.custom_tags,
            ssh_public_keys=cluster_info.ssh_public_keys,
            init_scripts=cluster_info.init_scripts,
            enable_elastic_disk=cluster_info.enable_elastic_disk,
            enable_local_disk_encryption=cluster_info.enable_local_disk_encryption,
            runtime_engine=cluster_info.runtime_engine,
        )
        if cluster_info.autoscale:
            edit_kwargs["autoscale"] = cluster_info.autoscale
        elif cluster_info.num_workers is not None:
            edit_kwargs["num_workers"] = cluster_info.num_workers
        if cluster_info.aws_attributes:
            edit_kwargs["aws_attributes"] = cluster_info.aws_attributes
        if cluster_info.azure_attributes:
            edit_kwargs["azure_attributes"] = cluster_info.azure_attributes
        if cluster_info.gcp_attributes:
            edit_kwargs["gcp_attributes"] = cluster_info.gcp_attributes
        if cluster_info.cluster_log_conf:
            edit_kwargs["cluster_log_conf"] = cluster_info.cluster_log_conf
        if cluster_info.docker_image:
            edit_kwargs["docker_image"] = cluster_info.docker_image
        if cluster_info.data_security_mode:
            edit_kwargs["data_security_mode"] = cluster_info.data_security_mode
        if cluster_info.single_user_name:
            edit_kwargs["single_user_name"] = cluster_info.single_user_name
        edit_kwargs = {k: v for k, v in edit_kwargs.items() if v is not None}
        return w.clusters.edit(**edit_kwargs)

    # Table header
    header_cols = st.columns([0.3, 1.5, 1, 1, 0.7, 0.8, 1.2, 0.6, 0.8, 0.5])
    for col, h in zip(header_cols, [None, "Cluster Name", "Creator", "Workers", "Auto-Term", "Start Time (UTC)", "Uptime", "New (min)", None]):
        if h:
            col.markdown(f"**{h}**")

    st.divider()

    for i, c in enumerate(clusters):
        # Workers display
        if c.autoscale:
            workers = f"{c.autoscale.min_workers}-{c.autoscale.max_workers} (auto)"
        else:
            workers = str(c.num_workers) if c.num_workers is not None else "N/A"

        # Auto-termination
        if c.autotermination_minutes and c.autotermination_minutes > 0:
            auto_term = f"{c.autotermination_minutes} min"
        else:
            auto_term = "Disabled"

        # Start time & uptime
        if c.state == State.RUNNING and c.last_state_loss_time:
            start_str = dt.datetime.utcfromtimestamp(c.last_state_loss_time / 1000).strftime("%Y-%m-%d %H:%M:%S")
        else:
            start_str = "N/A"

        if c.state == State.RUNNING and c.last_state_loss_time:
            total_secs = (now_epoch_ms - c.last_state_loss_time) // 1000
            days, rem = divmod(total_secs, 86400)
            hours, rem = divmod(rem, 3600)
            mins = rem // 60
            uptime = f"{days}d {hours}h {mins}m"
        else:
            uptime = "N/A"

        indicator = STATE_COLORS.get(c.state, "⚪")
        current_val = c.autotermination_minutes or 0

        with st.form(key=f"at_form_{i}"):
            row_cols = st.columns([0.3, 1.5, 1, 1, 0.7, 0.8, 1.2, 0.6, 0.8, 0.5])
            row_cols[0].write(indicator)
            row_cols[1].write(c.cluster_name)
            row_cols[2].write(c.creator_user_name or "N/A")
            row_cols[3].write(workers)
            row_cols[4].write(auto_term)
            row_cols[5].write(start_str)
            row_cols[6].write(uptime)
            new_val = row_cols[7].number_input("min", min_value=0, max_value=1440, value=current_val, step=10, key=f"at_{i}", label_visibility="collapsed")
            submitted = row_cols[8].form_submit_button("Apply")
        if submitted:
            try:
                result = apply_auto_termination(c.cluster_id, new_val)
                st.success(f"Auto-termination updated to {new_val} min for {c.cluster_name}.")
                st.info(f"API response: {result}")
            except Exception as e:
                st.error(f"Failed to update {c.cluster_name}: {e}")
