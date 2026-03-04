import datetime as dt

import altair as alt
import pandas as pd
import pytz
import streamlit as st
from databricks.sdk import WorkspaceClient

st.header("Job Runs History")

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

col_tz, col_days = st.columns([0.15, 0.85])
selected_tz = col_tz.selectbox(
    "Timezone", options=COMMON_TZ, index=0, key="last_run_tz"
)
lookback_days = col_days.slider("Lookback days", min_value=1, max_value=90, value=30)

tz = pytz.timezone(selected_tz)
now_local = dt.datetime.now(tz)

start_ms = int((now_local - dt.timedelta(days=lookback_days)).timestamp() * 1000)
end_ms = int(now_local.timestamp() * 1000)

w = WorkspaceClient(profile="DEFAULT")

with st.spinner("Fetching completed job runs…"):
    try:
        completed_runs = list(
            w.jobs.list_runs(
                start_time_from=start_ms,
                start_time_to=end_ms,
                completed_only=True,
                expand_tasks=False,
            )
        )
    except Exception as e:
        st.error(f"Failed to fetch runs: {e}")
        st.stop()

# Build records for all completed runs
records = []
for run in completed_runs:
    rs = run.state.result_state.value if run.state and run.state.result_state else None
    if not rs or not run.start_time:
        continue

    name = run.run_name or f"job-{run.job_id}"
    run_start = dt.datetime.fromtimestamp(
        run.start_time / 1000, tz=pytz.utc
    ).astimezone(tz)
    run_end = (
        dt.datetime.fromtimestamp(run.end_time / 1000, tz=pytz.utc).astimezone(tz)
        if run.end_time and run.end_time > 0
        else run_start
    )
    duration_min = (run_end - run_start).total_seconds() / 60

    # Simplify to SUCCESS / FAILED
    status = "SUCCESS" if rs == "SUCCESS" else "FAILED"

    records.append(
        {
            "job": name,
            "job_id": run.job_id,
            "run_time": run_start,
            "duration_min": round(duration_min, 1),
            "status": status,
        }
    )

if not records:
    st.info(f"No completed job runs found in the last {lookback_days} days.")
    st.stop()

df = pd.DataFrame(records)

# Strip tz for Altair compatibility
df["run_time"] = df["run_time"].apply(lambda x: x.replace(tzinfo=None))

status_colors = {
    "SUCCESS": "#66BB6A",
    "FAILED": "#EF5350",
}

job_names = sorted(df["job"].unique())

chart = (
    alt.Chart(df)
    .mark_point(size=80, filled=True)
    .encode(
        x=alt.X(
            "run_time:T",
            title="Run Date",
            axis=alt.Axis(format="%Y-%m-%d", labelAngle=-45, grid=True),
        ),
        y=alt.Y(
            "job:N",
            title="",
            sort=job_names,
            axis=alt.Axis(labelLimit=300, grid=True),
        ),
        color=alt.Color(
            "status:N",
            scale=alt.Scale(
                domain=list(status_colors.keys()),
                range=list(status_colors.values()),
            ),
            legend=alt.Legend(title="Status"),
        ),
        tooltip=[
            "job",
            "status",
            alt.Tooltip("run_time:T", title="Run Time", format="%Y-%m-%d %H:%M"),
            alt.Tooltip("duration_min:Q", title="Duration (min)"),
        ],
    )
    .properties(height=max(len(job_names) * 25, 200))
)

st.altair_chart(chart, use_container_width=True)
