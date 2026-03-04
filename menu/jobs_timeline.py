import datetime as dt

import altair as alt
import pandas as pd
import pytz
import streamlit as st
from databricks.sdk import WorkspaceClient

st.header("Jobs Execution Timeline")

COMMON_TZ = [
    "UTC", "US/Eastern", "US/Central", "US/Pacific", "Europe/London",
    "Europe/Berlin", "Europe/Moscow", "Asia/Tokyo", "Asia/Shanghai",
    "Australia/Sydney",
]

col_date, col_tz = st.columns(2)
selected_date = col_date.date_input("Date", value=dt.date.today())
selected_tz = col_tz.selectbox("Timezone", options=COMMON_TZ, index=0, key="jobs_timeline_tz")
tz = pytz.timezone(selected_tz)

# Day boundaries in epoch ms (in selected timezone)
day_start_local = tz.localize(dt.datetime.combine(selected_date, dt.time.min))
day_end_local = tz.localize(dt.datetime.combine(selected_date, dt.time.max))
now_local = dt.datetime.now(tz)
effective_end = min(day_end_local, now_local)
start_ms = int(day_start_local.timestamp() * 1000)
end_ms = int(effective_end.timestamp() * 1000)

STATE_COLORS = {
    "PENDING":     "#FFD54F",   # yellow
    "RUNNING":     "#4CAF50",   # green
    "SUCCESS":     "#66BB6A",   # light green
    "FAILED":      "#EF5350",   # red
    "TIMEDOUT":    "#FF9800",   # orange
    "CANCELED":    "#B0BEC5",   # gray
    "TERMINATING": "#CE93D8",   # purple
}

w = WorkspaceClient()

with st.spinner("Fetching job runs…"):
    try:
        runs = list(w.jobs.list_runs(
            start_time_from=start_ms,
            start_time_to=end_ms,
            expand_tasks=False,
        ))
    except Exception as e:
        st.error(f"Failed to fetch runs: {e}")
        st.stop()

if not runs:
    st.info("No job runs found for the selected date.")
    st.stop()

# Build segments
segments = []
for run in runs:
    if not run.start_time:
        continue

    run_start = dt.datetime.fromtimestamp(run.start_time / 1000, tz=pytz.utc).astimezone(tz)

    if run.end_time and run.end_time > 0:
        run_end = dt.datetime.fromtimestamp(run.end_time / 1000, tz=pytz.utc).astimezone(tz)
    else:
        run_end = min(now_local, day_end_local)

    # Derive display state
    lcs = run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else None
    rs = run.state.result_state.value if run.state and run.state.result_state else None

    if lcs == "RUNNING":
        display_state = "RUNNING"
    elif lcs in ("PENDING", "QUEUED", "BLOCKED"):
        display_state = "PENDING"
    elif lcs == "TERMINATING":
        display_state = "TERMINATING"
    elif lcs == "TERMINATED":
        state_map = {
            "SUCCESS": "SUCCESS",
            "FAILED": "FAILED",
            "TIMEDOUT": "TIMEDOUT",
            "CANCELED": "CANCELED",
        }
        display_state = state_map.get(rs, rs or lcs)
    else:
        display_state = lcs or "UNKNOWN"

    name = run.run_name or f"job-{run.job_id}"

    segments.append({
        "job": name,
        "run_id": run.run_id,
        "state": display_state,
        "start": run_start,
        "end": run_end,
    })

if not segments:
    st.info("No displayable runs for the selected date.")
    st.stop()

df = pd.DataFrame(segments)

# Job filter
job_names = sorted(df["job"].unique())
selected_jobs = st.multiselect("Jobs", options=job_names, default=job_names)
if not selected_jobs:
    st.warning("Select at least one job.")
    st.stop()

df = df[df["job"].isin(selected_jobs)]

domain = list(STATE_COLORS.keys())
range_ = list(STATE_COLORS.values())

chart = (
    alt.Chart(df)
    .mark_bar()
    .encode(
        x=alt.X("start:T", title="Time"),
        x2="end:T",
        y=alt.Y("job:N", title="", sort=alt.SortField("job")),
        color=alt.Color(
            "state:N",
            scale=alt.Scale(domain=domain, range=range_),
            legend=alt.Legend(title="State"),
        ),
        tooltip=["job", "state", "run_id:Q", "start:T", "end:T"],
    )
    .properties(width="container", height=max(len(selected_jobs) * 40, 120))
)

st.altair_chart(chart, use_container_width=True)
