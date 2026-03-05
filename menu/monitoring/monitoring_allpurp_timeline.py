import datetime as dt

import altair as alt
import pandas as pd
import pytz
import streamlit as st
from databricks.sdk import WorkspaceClient
from menu.compute.utils import make_workspace_client
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
_pending = st.session_state.pop("_timeline_date_pending", None)
if "timeline_date" not in st.session_state:
    st.session_state["timeline_date"] = dt.date.today()
if _pending is not None:
    st.session_state["timeline_date"] = _pending
selected_date = col_date.date_input("Date", key="timeline_date")
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

w = make_workspace_client()
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
    "STARTING": "#FFD54F",    # bright yellow
    "RUNNING": "#4CAF50",     # bright green
    "RESTARTING": "#FF9800",  # bright orange
    "INACTIVITY": "#EF5350",  # bright red
    "TERMINATING": "#EF9A9A", # light red
    "ERROR": "#B71C1C",       # dark red
    "UNKNOWN": "#CE93D8",     # light purple
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

daily_running = {}  # date -> {state -> seconds}

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

            # Close previous segment; for TERMINATING split off INACTIVITY portion
            if cur_state is not None and cur_state not in {"TERMINATED", "TERMINATING"} and cur_start is not None:
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
                        if inactivity_start < cur_start:
                            inactivity_start = cur_start
                        # Accumulate cur_state up to inactivity_start
                        for seg_start, seg_end, seg_state in [
                            (cur_start, inactivity_start, cur_state),
                            (inactivity_start, ts, "INACTIVITY"),
                        ]:
                            d = seg_start.date()
                            while d <= seg_end.date():
                                day_begin = max(seg_start, tz.localize(dt.datetime.combine(d, dt.time.min)))
                                day_finish = min(seg_end, tz.localize(dt.datetime.combine(d, dt.time.max)))
                                secs = (day_finish - day_begin).total_seconds()
                                if secs > 0:
                                    day_dict = daily_running.setdefault(d, {})
                                    day_dict[seg_state] = day_dict.get(seg_state, 0) + secs
                                d += dt.timedelta(days=1)
                    else:
                        seg_start = cur_start
                        seg_end = ts
                        d = seg_start.date()
                        while d <= seg_end.date():
                            day_begin = max(seg_start, tz.localize(dt.datetime.combine(d, dt.time.min)))
                            day_finish = min(seg_end, tz.localize(dt.datetime.combine(d, dt.time.max)))
                            secs = (day_finish - day_begin).total_seconds()
                            if secs > 0:
                                day_dict = daily_running.setdefault(d, {})
                                day_dict[cur_state] = day_dict.get(cur_state, 0) + secs
                            d += dt.timedelta(days=1)
                else:
                    seg_start = cur_start
                    seg_end = ts
                    d = seg_start.date()
                    while d <= seg_end.date():
                        day_begin = max(seg_start, tz.localize(dt.datetime.combine(d, dt.time.min)))
                        day_finish = min(seg_end, tz.localize(dt.datetime.combine(d, dt.time.max)))
                        secs = (day_finish - day_begin).total_seconds()
                        if secs > 0:
                            day_dict = daily_running.setdefault(d, {})
                            day_dict[cur_state] = day_dict.get(cur_state, 0) + secs
                        d += dt.timedelta(days=1)

            cur_state = new_state
            cur_start = ts

        # Close last segment (any active state, skip TERMINATING to avoid runaway accumulation)
        if cur_state is not None and cur_state not in {"TERMINATED", "TERMINATING"} and cur_start is not None:
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
                    day_dict = daily_running.setdefault(d, {})
                    day_dict[cur_state] = day_dict.get(cur_state, 0) + secs
                d += dt.timedelta(days=1)

# Build full 90-day range with zeros for missing days, one row per (date, state)
_active_states = [s for s in STATE_COLORS if s != "UNKNOWN"]
daily_rows = []
d = thirty_days_ago
while d <= today:
    day_dict = daily_running.get(d, {})
    for state in _active_states:
        secs = day_dict.get(state, 0)
        daily_rows.append({"date": d, "state": state, "runtime_hours": round(secs / 3600, 2)})
    d += dt.timedelta(days=1)

daily_df = pd.DataFrame(daily_rows)
daily_df["date"] = pd.to_datetime(daily_df["date"])

# Wide format for combined per-day tooltip
daily_wide = daily_df.pivot_table(
    index="date", columns="state", values="runtime_hours", fill_value=0
).reset_index()
daily_wide.columns.name = None
_present_states = [s for s in _active_states if s in daily_wide.columns]
daily_wide["total_hours"] = daily_wide[_present_states].sum(axis=1)
daily_wide["date_str"] = daily_wide["date"].dt.strftime("%Y-%m-%d")

_STATE_EMOJI = {
    "STARTING":    "🟡",
    "RUNNING":     "🟢",
    "RESTARTING":  "🟠",
    "INACTIVITY":  "🔴",
    "TERMINATING": "🔴",
    "ERROR":       "🔴",
}

_tooltip = [
    alt.Tooltip("date:T", title="📅 Date", format="%Y-%m-%d"),
    alt.Tooltip("total_hours:Q", title="⬜ Total, h", format=".2f"),
] + [
    alt.Tooltip(f"{s}:Q", title=f"{_STATE_EMOJI.get(s, '⬜')} {s}, h", format=".2f")
    for s in _present_states
]

_date_sel = alt.selection_point(fields=["date_str"], on="click", name="date_sel", clear="dblclick")

daily_chart = (
    alt.Chart(daily_wide)
    .transform_fold(_present_states, as_=["state", "runtime_hours"])
    .mark_bar()
    .encode(
        x=alt.X("date:T", title="Date", axis=alt.Axis(format="%b %d", labelAngle=-45)),
        y=alt.Y("runtime_hours:Q", title="Runtime (hours)", stack="zero"),
        color=alt.Color(
            "state:N",
            scale=alt.Scale(
                domain=_active_states,
                range=[STATE_COLORS[s] for s in _active_states],
            ),
            legend=alt.Legend(title="State"),
        ),
        tooltip=_tooltip,
    )
    .add_params(_date_sel)
    .properties(width="container", height=250)
)

_event = st.altair_chart(daily_chart, use_container_width=True, on_select="rerun")

_sel_data = (_event.selection or {}).get("date_sel", [])
_date_str_val = None
if isinstance(_sel_data, list) and _sel_data:
    _first = _sel_data[0]
    _date_str_val = _first.get("date_str") if isinstance(_first, dict) else None
elif isinstance(_sel_data, dict):
    _pts = _sel_data.get("date_str", [])
    _date_str_val = _pts[0] if _pts else None

if _date_str_val:
    try:
        _clicked_date = dt.date.fromisoformat(_date_str_val)
        if _clicked_date != selected_date:
            st.session_state["_timeline_date_pending"] = _clicked_date
            st.rerun()
    except Exception:
        pass

st.subheader("Cluster State Timeline")
if not segments:
    st.info("No events found for the selected date.")
else:
    st.altair_chart(chart, use_container_width=True)
