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

from menu.compute.utils import run_uses_cluster, resolve_display_state, make_workspace_client, COMMON_TZ, MAX_CLUSTER_EVENTS
st.header("Cluster Jobs")

col_date, col_tz, col_cluster = st.columns([0.15, 0.10, 0.75])
selected_date = col_date.date_input("Date", value=dt.date.today())
selected_tz = col_tz.selectbox(
    "Timezone", options=COMMON_TZ, index=0, key="cluster_jobs_tz"
)
tz = pytz.timezone(selected_tz)

w = make_workspace_client()

clusters = [
    c for c in w.clusters.list()
    if c.cluster_source not in (
        ClusterSource.JOB, ClusterSource.PIPELINE, ClusterSource.PIPELINE_MAINTENANCE,
    )
]

if not clusters:
    st.info("No all-purpose clusters found.")
    st.stop()

cluster_map = {c.cluster_name: c for c in clusters}
selected_name = col_cluster.selectbox(
    "Cluster", options=sorted(cluster_map.keys()), key="cluster_jobs_cluster"
)
selected_cluster = cluster_map[selected_name]

# Day boundaries in epoch ms (in selected timezone)
day_start_local = tz.localize(dt.datetime.combine(selected_date, dt.time.min))
day_end_local = tz.localize(dt.datetime.combine(selected_date, dt.time.max))
now_local = dt.datetime.now(tz)
effective_end = min(day_end_local, now_local)
start_ms = int(day_start_local.timestamp() * 1000)
end_ms = int(effective_end.timestamp() * 1000)

# Fixed x-axis domain: full day 00:00–24:00
day_start_naive = day_start_local.replace(tzinfo=None)
day_end_naive = day_start_naive + dt.timedelta(days=1)


# ═══════════════════════════════════════════════════════════════════
# Chart 1 — Cluster State Timeline (from cluster_timeline.py)
# ═══════════════════════════════════════════════════════════════════

EVENT_TO_STATE = {
    EventType.CREATING: "STARTING",
    EventType.STARTING: "STARTING",
    EventType.RUNNING: "RUNNING",
    EventType.RESTARTING: "RESTARTING",
    EventType.TERMINATING: "TERMINATING",
    EventType.EDITED: None,
    EventType.RESIZING: None,
    EventType.DRIVER_HEALTHY: None,
}

CLUSTER_STATE_COLORS = {
    "STARTING":    "#FFD54F",
    "RUNNING":     "#4CAF50",
    "RESTARTING":  "#FF9800",
    "INACTIVITY":  "#EF5350",
    "ERROR":       "#EF5350",
    "UNKNOWN":     "#CE93D8",
}

cluster_segments = []

with st.spinner("Fetching cluster events…"):
    try:
        resp = w.clusters.events(
            cluster_id=selected_cluster.cluster_id,
            start_time=start_ms,
            end_time=end_ms,
            order=GetEventsOrder.ASC,
            limit=MAX_CLUSTER_EVENTS,
        )
        events = list(resp) if resp else []
    except Exception:
        events = []

    if events:
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
                new_state = ev_type.value if hasattr(ev_type, "value") else str(ev_type)

            if new_state is None:
                continue

            if cur_state is not None and cur_start is not None:
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
                            cluster_segments.append({"cluster": selected_name, "state": cur_state, "start": cur_start, "end": inactivity_start})
                        else:
                            inactivity_start = cur_start
                        cluster_segments.append({"cluster": selected_name, "state": "INACTIVITY", "start": inactivity_start, "end": ts})
                    else:
                        cluster_segments.append({"cluster": selected_name, "state": cur_state, "start": cur_start, "end": ts})
                else:
                    cluster_segments.append({
                        "cluster": selected_name,
                        "state": cur_state,
                        "start": cur_start,
                        "end": ts,
                    })

            cur_state = new_state
            cur_start = ts

        if cur_state is not None and cur_start is not None:
            seg_end = min(now_local, day_end_local)
            final_state = cur_state
            transitional = {"TERMINATING", "STARTING", "RESTARTING"}
            if cur_state in transitional and selected_cluster.state is not None:
                live = selected_cluster.state.value
                if live in CLUSTER_STATE_COLORS:
                    final_state = live
            cluster_segments.append({
                "cluster": selected_name,
                "state": final_state,
                "start": cur_start,
                "end": seg_end,
            })

_hidden = {"TERMINATED", "TERMINATING"}
cluster_segments = [s for s in cluster_segments if s["state"] not in _hidden]

cluster_chart = None

if cluster_segments:
    cdf = pd.DataFrame(cluster_segments)
    cdf["start"] = cdf["start"].apply(lambda x: x.replace(tzinfo=None))
    cdf["end"] = cdf["end"].apply(lambda x: x.replace(tzinfo=None))

    anchors = pd.DataFrame([
        {"cluster": selected_name, "state": "UNKNOWN", "start": day_start_naive, "end": day_start_naive},
        {"cluster": selected_name, "state": "UNKNOWN", "start": day_end_naive, "end": day_end_naive},
    ])
    cdf = pd.concat([cdf, anchors], ignore_index=True)
    cdf["_opacity"] = cdf["state"].apply(lambda s: 0.0 if s == "UNKNOWN" else 1.0)
    cdf["duration_min"] = ((cdf["end"] - cdf["start"]).dt.total_seconds() / 60).round(1)

    c_domain = list(CLUSTER_STATE_COLORS.keys())
    c_range = list(CLUSTER_STATE_COLORS.values())

    cluster_chart = (
        alt.Chart(cdf)
        .mark_bar()
        .encode(
            x=alt.X("start:T", title="Time", axis=alt.Axis(format="%H:%M", labelAngle=-45)),
            x2="end:T",
            y=alt.Y("cluster:N", title="", sort=alt.SortField("cluster")),
            color=alt.Color(
                "state:N",
                scale=alt.Scale(domain=c_domain, range=c_range),
                legend=None,
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
        .properties(height=120)
    )


# ═══════════════════════════════════════════════════════════════════
# Charts 2 & 3 — Jobs Execution Timeline + Concurrency (from jobs_timeline.py)
# ═══════════════════════════════════════════════════════════════════

STATE_COLORS = {
    "PENDING": "#FFD54F",  # yellow
    "RUNNING": "#4CAF50",  # green
    "SUCCESS": "#66BB6A",  # light green
    "FAILED": "#EF5350",  # red
    "TIMEDOUT": "#FF9800",  # orange
    "CANCELED": "#B0BEC5",  # gray
    "TERMINATING": "#CE93D8",  # purple
}


with st.spinner("Fetching job runs…"):
    try:
        runs = list(
            w.jobs.list_runs(
                start_time_from=start_ms,
                start_time_to=end_ms,
                expand_tasks=True,
            )
        )
    except Exception as e:
        st.error(f"Failed to fetch runs: {e}")
        st.stop()

# Filter to selected cluster
runs = [r for r in runs if run_uses_cluster(r, selected_cluster.cluster_id)]

# Build actual run segments
segments = []
for run in runs:
    if not run.start_time:
        continue

    run_start = dt.datetime.fromtimestamp(
        run.start_time / 1000, tz=pytz.utc
    ).astimezone(tz)

    if run.end_time and run.end_time > 0:
        run_end = dt.datetime.fromtimestamp(
            run.end_time / 1000, tz=pytz.utc
        ).astimezone(tz)
    else:
        run_end = min(now_local, day_end_local)

    # Ensure minimum display width of 5 minutes so short runs are visible
    min_end = run_start + dt.timedelta(minutes=5)
    if run_end < min_end:
        run_end = min_end

    # Derive display state
    lcs = (
        run.state.life_cycle_state.value
        if run.state and run.state.life_cycle_state
        else None
    )
    rs = run.state.result_state.value if run.state and run.state.result_state else None
    display_state = resolve_display_state(lcs, rs)

    name = run.run_name or f"job-{run.job_id}"

    segments.append(
        {
            "job": name,
            "run_id": run.run_id,
            "state": display_state,
            "start": run_start,
            "end": run_end,
        }
    )

if not segments:
    st.info("No job runs found on this cluster for the selected date.")
    st.stop()

df = pd.DataFrame(segments)

job_names = sorted(df["job"].unique())
selected_jobs = job_names

# Strip timezone for Altair compatibility
df["start"] = df["start"].apply(lambda x: x.replace(tzinfo=None))
df["end"] = df["end"].apply(lambda x: x.replace(tzinfo=None))


domain = list(STATE_COLORS.keys())
range_ = list(STATE_COLORS.values())

# Add invisible anchor rows so the x-axis always spans the full day
anchor_jobs = df["job"].iloc[0]
anchors = pd.DataFrame(
    [
        {
            "job": anchor_jobs,
            "state": "TERMINATED",
            "start": day_start_naive,
            "end": day_start_naive,
            "_opacity": 0,
            "run_id": None,
        },
        {
            "job": anchor_jobs,
            "state": "TERMINATED",
            "start": day_end_naive,
            "end": day_end_naive,
            "_opacity": 0,
            "run_id": None,
        },
    ]
)
df["_opacity"] = 1.0
df = pd.concat([df, anchors], ignore_index=True)

timeline_chart = (
    alt.Chart(df)
    .mark_bar()
    .encode(
        x=alt.X("start:T", title="Time", axis=alt.Axis(format="%H:%M", labelAngle=-45)),
        x2=alt.X2("end:T"),
        y=alt.Y(
            "job:N", title="", sort=alt.SortField("job"), axis=alt.Axis(labelLimit=300)
        ),
        color=alt.Color(
            "state:N",
            scale=alt.Scale(domain=domain, range=range_),
            legend=None,
        ),
        opacity=alt.Opacity("_opacity:Q", legend=None, scale=None),
        tooltip=[
            "job",
            "state",
            alt.Tooltip("start:T", format="%H:%M:%S"),
            alt.Tooltip("end:T", format="%H:%M:%S"),
        ],
    )
    .properties(height=max(len(selected_jobs) * 40, 100))
)

# --- Parallel jobs concurrency chart (5-min windows) ---

runs_df = df[df["_opacity"] > 0].copy()  # exclude invisible anchors

charts = []
if cluster_chart is not None:
    charts.append(cluster_chart)
charts.append(timeline_chart)

if not runs_df.empty:
    # Build 5-minute buckets spanning the full day (00:00 to 24:00)
    counts = []
    t = day_start_naive
    while t <= day_end_naive:
        bucket_end = t + dt.timedelta(minutes=5)
        n = ((runs_df["start"] < bucket_end) & (runs_df["end"] > t)).sum()
        counts.append({"time": t, "parallel_jobs": int(n)})
        t = bucket_end

    concurrency_df = pd.DataFrame(counts)

    concurrency_chart = (
        alt.Chart(concurrency_df)
        .mark_line(point=False, interpolate="step-after")
        .encode(
            x=alt.X(
                "time:T", title="Time", axis=alt.Axis(format="%H:%M", labelAngle=-45)
            ),
            y=alt.Y(
                "parallel_jobs:Q",
                title="Parallel Jobs",
                axis=alt.Axis(tickMinStep=1, format="d"),
            ),
            tooltip=[
                alt.Tooltip("time:T", title="Window", format="%H:%M"),
                alt.Tooltip("parallel_jobs:Q", title="Jobs"),
            ],
        )
        .properties(height=150)
    )
    charts.append(concurrency_chart)

combined = alt.vconcat(*charts).resolve_scale(x="shared")

st.altair_chart(combined, use_container_width=True)
