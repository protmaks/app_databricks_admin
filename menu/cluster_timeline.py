import datetime as dt

import altair as alt
import pandas as pd
import pytz
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import (
    ClusterSource,
    EventType,
    GetEventsOrder,
)

st.header("Cluster State Timeline")

COMMON_TZ = [
    "UTC", "US/Eastern", "US/Central", "US/Pacific", "Europe/London",
    "Europe/Berlin", "Europe/Moscow", "Asia/Tokyo", "Asia/Shanghai",
    "Australia/Sydney",
]

col_date, col_tz = st.columns(2)
selected_date = col_date.date_input("Date", value=dt.date.today())
selected_tz = col_tz.selectbox("Timezone", options=COMMON_TZ, index=0, key="timeline_tz")
tz = pytz.timezone(selected_tz)

# Day boundaries in epoch ms (in selected timezone)
day_start_local = tz.localize(dt.datetime.combine(selected_date, dt.time.min))
day_end_local = tz.localize(dt.datetime.combine(selected_date, dt.time.max))
now_local = dt.datetime.now(tz)
effective_end = min(day_end_local, now_local)
start_ms = int(day_start_local.timestamp() * 1000)
end_ms = int(effective_end.timestamp() * 1000)

w = WorkspaceClient()
clusters = [
    c for c in w.clusters.list()
    if c.cluster_source not in (
        ClusterSource.JOB, ClusterSource.PIPELINE, ClusterSource.PIPELINE_MAINTENANCE,
    )
]

if not clusters:
    st.info("No all-purpose clusters found.")
    st.stop()

cluster_names = sorted(set(c.cluster_name for c in clusters))
selected_clusters = st.multiselect(
    "Clusters", options=cluster_names, default=cluster_names,
)
if not selected_clusters:
    st.warning("Select at least one cluster.")
    st.stop()

selected_set = set(selected_clusters)
filtered = [c for c in clusters if c.cluster_name in selected_set]

# State derived from event type
EVENT_TO_STATE = {
    EventType.CREATING: "STARTING",
    EventType.STARTING: "STARTING",
    EventType.RUNNING: "RUNNING",
    EventType.RESTARTING: "RESTARTING",
    EventType.TERMINATING: "TERMINATING",
    EventType.EDITED: None,          # keep previous state
    EventType.RESIZING: None,
}

STATE_COLORS = {
    "STARTING":    "#FFD54F",   # bright yellow
    "RUNNING":     "#4CAF50",   # bright green
    "RESTARTING":  "#FF9800",   # bright orange
    "TERMINATING": "#B0BEC5",   # light blue-gray
    "TERMINATED":  "#78909C",   # medium blue-gray
    "ERROR":       "#EF5350",   # bright red
    "UNKNOWN":     "#CE93D8",   # light purple
}

segments = []

with st.spinner("Fetching cluster events…"):
    for c in filtered:
        try:
            resp = w.clusters.events(
                cluster_id=c.cluster_id,
                start_time=start_ms,
                end_time=end_ms,
                order=GetEventsOrder.ASC,
                limit=500,
            )
            events = list(resp) if resp else []
        except Exception:
            events = []

        if not events:
            continue

        cur_state = None
        cur_start = None

        for ev in events:
            ts = dt.datetime.fromtimestamp(ev.timestamp / 1000, tz=pytz.utc).astimezone(tz)

            new_state = None
            ev_type = ev.type
            if ev_type in EVENT_TO_STATE:
                new_state = EVENT_TO_STATE[ev_type]
            elif ev_type in (EventType.PINNED, EventType.UNPINNED):
                new_state = None
            else:
                # For any other event type, use the type name as the state
                new_state = ev_type.value if hasattr(ev_type, "value") else str(ev_type)

            if new_state is None:
                continue

            if cur_state is not None and cur_start is not None:
                segments.append({
                    "cluster": c.cluster_name,
                    "state": cur_state,
                    "start": cur_start,
                    "end": ts,
                })

            cur_state = new_state
            cur_start = ts

        # Close the last segment at current time (today) or end of day (past)
        if cur_state is not None and cur_start is not None:
            now_local = dt.datetime.now(tz)
            seg_end = min(now_local, day_end_local)
            # Only override transitional states with the live state
            # e.g. TERMINATING -> TERMINATED, but don't change RUNNING
            final_state = cur_state
            transitional = {"TERMINATING", "STARTING", "RESTARTING"}
            if cur_state in transitional and c.state is not None:
                live = c.state.value
                if live in STATE_COLORS:
                    final_state = live
            segments.append({
                "cluster": c.cluster_name,
                "state": final_state,
                "start": cur_start,
                "end": seg_end,
            })

if not segments:
    st.info("No events found for the selected date and clusters.")
    st.stop()

df = pd.DataFrame(segments)

domain = list(STATE_COLORS.keys())
range_ = list(STATE_COLORS.values())

chart = (
    alt.Chart(df)
    .mark_bar()
    .encode(
        x=alt.X("start:T", title="Time"),
        x2="end:T",
        y=alt.Y("cluster:N", title="", sort=alt.SortField("cluster")),
        color=alt.Color(
            "state:N",
            scale=alt.Scale(domain=domain, range=range_),
            legend=alt.Legend(title="State"),
        ),
        tooltip=["cluster", "state", "start:T", "end:T"],
    )
    .properties(width="container", height=max(len(selected_clusters) * 40, 120))
)

st.altair_chart(chart, use_container_width=True)
