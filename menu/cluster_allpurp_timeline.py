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
MAX_CLUSTERS = 500
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

col_date, col_tz, col_teams = st.columns([0.15, 0.10, 0.75])
selected_date = col_date.date_input("Date", value=dt.date.today())
selected_tz = col_tz.selectbox(
    "Timezone", options=COMMON_TZ, index=0, key="timeline_tz"
)
col_teams.multiselect(
    "Teams", options=[], default=[], disabled=True, help="Coming soon"
)
tz = pytz.timezone(selected_tz)

# Day boundaries in epoch ms (in selected timezone)
day_start_local = tz.localize(dt.datetime.combine(selected_date, dt.time.min))
day_end_local = tz.localize(dt.datetime.combine(selected_date, dt.time.max))
now_local = dt.datetime.now(tz)
effective_end = min(day_end_local, now_local)
start_ms = int(day_start_local.timestamp() * 1000)
end_ms = int(effective_end.timestamp() * 1000)

w = WorkspaceClient(profile="DEFAULT")
clusters = [
    c
    for c in w.clusters.list()
    if c.cluster_source
    not in (
        ClusterSource.JOB,
        ClusterSource.PIPELINE,
        ClusterSource.PIPELINE_MAINTENANCE,
    )
]

if not clusters:
    st.info("No all-purpose clusters found.")
    st.stop()

cluster_names = sorted(set(c.cluster_name for c in clusters))
selected_clusters = cluster_names

selected_set = set(selected_clusters)
filtered = [c for c in clusters if c.cluster_name in selected_set]

# State derived from event type
EVENT_TO_STATE = {
    EventType.CREATING: "STARTING",
    EventType.STARTING: "STARTING",
    EventType.RUNNING: "RUNNING",
    EventType.RESTARTING: "RESTARTING",
    EventType.TERMINATING: "TERMINATING",
    EventType.EDITED: None,  # keep previous state
    EventType.RESIZING: None,
    EventType.DRIVER_HEALTHY: None,
}

STATE_COLORS = {
    "STARTING": "#FFD54F",  # bright yellow
    "RUNNING": "#4CAF50",  # bright green
    "RESTARTING": "#FF9800",  # bright orange
    "INACTIVITY": "#EF5350",  # bright red
    "ERROR": "#EF5350",  # bright red
    "UNKNOWN": "#CE93D8",  # light purple
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
                limit=MAX_CLUSTERS,
            )
            events = list(resp) if resp else []
        except Exception:
            events = []

        if not events:
            continue

        cur_state = None
        cur_start = None

        for ev in events:
            ts = dt.datetime.fromtimestamp(ev.timestamp / 1000, tz=pytz.utc).astimezone(
                tz
            )

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
                # For TERMINATING events, extract inactivity duration and insert
                # an INACTIVITY segment (red) before the termination point
                if ev_type == EventType.TERMINATING:
                    inactivity_min = 0
                    if ev.details:
                        reason = getattr(ev.details, 'reason', None)
                        if reason:
                            params = getattr(reason, 'parameters', None) or {}
                            try:
                                inactivity_min = int(params.get('inactivity_duration_min', 0))
                            except (ValueError, TypeError):
                                inactivity_min = 0
                    if inactivity_min > 0:
                        inactivity_start = ts - dt.timedelta(minutes=inactivity_min)
                        if inactivity_start > cur_start:
                            segments.append({"cluster": c.cluster_name, "state": cur_state, "start": cur_start, "end": inactivity_start})
                        else:
                            inactivity_start = cur_start
                        segments.append({"cluster": c.cluster_name, "state": "INACTIVITY", "start": inactivity_start, "end": ts})
                    else:
                        segments.append({"cluster": c.cluster_name, "state": cur_state, "start": cur_start, "end": ts})
                else:
                    segments.append(
                        {
                            "cluster": c.cluster_name,
                            "state": cur_state,
                            "start": cur_start,
                            "end": ts,
                        }
                    )

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
            segments.append(
                {
                    "cluster": c.cluster_name,
                    "state": final_state,
                    "start": cur_start,
                    "end": seg_end,
                }
            )

_hidden = {"TERMINATED", "TERMINATING"}
segments = [s for s in segments if s["state"] not in _hidden]

chart = None
if segments:
    df = pd.DataFrame(segments)
    df["start"] = df["start"].apply(lambda x: x.replace(tzinfo=None))
    df["end"] = df["end"].apply(lambda x: x.replace(tzinfo=None))

    day_start_naive = day_start_local.replace(tzinfo=None)
    day_end_naive = day_start_naive + dt.timedelta(days=1)
    anchor_cluster = df["cluster"].iloc[0]
    anchors = pd.DataFrame(
        [
            {"cluster": anchor_cluster, "state": "UNKNOWN", "start": day_start_naive, "end": day_start_naive},
            {"cluster": anchor_cluster, "state": "UNKNOWN", "start": day_end_naive, "end": day_end_naive},
        ]
    )
    df = pd.concat([df, anchors], ignore_index=True)
    df["_opacity"] = df["state"].apply(lambda s: 0.0 if s == "UNKNOWN" else 1.0)
    df["duration_min"] = ((df["end"] - df["start"]).dt.total_seconds() / 60).round(1)

    domain = list(STATE_COLORS.keys())
    range_ = list(STATE_COLORS.values())

    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("start:T", title="Time", axis=alt.Axis(format="%H:%M", labelAngle=-45)),
            x2="end:T",
            y=alt.Y("cluster:N", title="", sort=alt.SortField("cluster")),
            color=alt.Color(
                "state:N",
                scale=alt.Scale(domain=domain, range=range_),
                legend=alt.Legend(title="State"),
            ),
            opacity=alt.Opacity("_opacity:Q", legend=None, scale=None),
            tooltip=[
                "cluster",
                "state",
                alt.Tooltip("start:T", title="Start", format="%H:%M:%S"),
                alt.Tooltip("end:T", title="End", format="%H:%M:%S"),
                alt.Tooltip("duration_min:Q", title="Duration (min)"),
            ],
        )
        .properties(width="container", height=max(len(selected_clusters) * 40, 120))
    )

# --- Daily cluster runtime (last 90 days) ---
st.subheader("Daily Cluster Runtime (last 90 days)")

today = dt.date.today()
thirty_days_ago = today - dt.timedelta(days=90)
range_start = tz.localize(dt.datetime.combine(thirty_days_ago, dt.time.min))
range_end = min(
    tz.localize(dt.datetime.combine(today, dt.time.max)), dt.datetime.now(tz)
)
range_start_ms = int(range_start.timestamp() * 1000)
range_end_ms = int(range_end.timestamp() * 1000)

daily_running = {}  # date -> total running seconds

with st.spinner("Fetching 30-day cluster events…"):
    for c in filtered:
        try:
            resp = w.clusters.events(
                cluster_id=c.cluster_id,
                start_time=range_start_ms,
                end_time=range_end_ms,
                order=GetEventsOrder.ASC,
                limit=MAX_CLUSTERS,
            )
            events = list(resp) if resp else []
        except Exception:
            events = []

        if not events:
            continue

        cur_state = None
        cur_start = None

        for ev in events:
            ts = dt.datetime.fromtimestamp(ev.timestamp / 1000, tz=pytz.utc).astimezone(
                tz
            )
            ev_type = ev.type
            if ev_type in EVENT_TO_STATE:
                new_state = EVENT_TO_STATE[ev_type]
            elif ev_type in (EventType.PINNED, EventType.UNPINNED):
                new_state = None
            else:
                new_state = ev_type.value if hasattr(ev_type, "value") else str(ev_type)

            if new_state is None:
                continue

            # Close previous RUNNING segment
            if cur_state == "RUNNING" and cur_start is not None:
                seg_start = cur_start
                seg_end = ts
                # Split across day boundaries
                d = seg_start.date()
                while d <= seg_end.date():
                    day_begin = max(
                        seg_start, tz.localize(dt.datetime.combine(d, dt.time.min))
                    )
                    day_finish = min(
                        seg_end, tz.localize(dt.datetime.combine(d, dt.time.max))
                    )
                    secs = (day_finish - day_begin).total_seconds()
                    if secs > 0:
                        daily_running[d] = daily_running.get(d, 0) + secs
                    d += dt.timedelta(days=1)

            cur_state = new_state
            cur_start = ts

        # Close last RUNNING segment
        if cur_state == "RUNNING" and cur_start is not None:
            seg_start = cur_start
            seg_end = min(dt.datetime.now(tz), range_end)
            d = seg_start.date()
            while d <= seg_end.date():
                day_begin = max(
                    seg_start, tz.localize(dt.datetime.combine(d, dt.time.min))
                )
                day_finish = min(
                    seg_end, tz.localize(dt.datetime.combine(d, dt.time.max))
                )
                secs = (day_finish - day_begin).total_seconds()
                if secs > 0:
                    daily_running[d] = daily_running.get(d, 0) + secs
                d += dt.timedelta(days=1)

# Build full 30-day range with zeros for missing days
daily_rows = []
d = thirty_days_ago
while d <= today:
    hours = daily_running.get(d, 0) / 3600
    daily_rows.append({"date": d, "runtime_hours": round(hours, 2)})
    d += dt.timedelta(days=1)

daily_df = pd.DataFrame(daily_rows)
daily_df["date"] = pd.to_datetime(daily_df["date"])

daily_chart = (
    alt.Chart(daily_df)
    .mark_bar()
    .encode(
        x=alt.X("date:T", title="Date", axis=alt.Axis(format="%b %d", labelAngle=-45)),
        y=alt.Y("runtime_hours:Q", title="Runtime (hours)"),
        tooltip=[
            alt.Tooltip("date:T", title="Date", format="%Y-%m-%d"),
            alt.Tooltip("runtime_hours:Q", title="Hours", format=".1f"),
        ],
    )
    .properties(width="container", height=250)
)

st.altair_chart(daily_chart, use_container_width=True)

st.subheader("Cluster State Timeline")
if not segments:
    st.info("No events found for the selected date.")
else:
    st.altair_chart(chart, use_container_width=True)
