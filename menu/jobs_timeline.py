import datetime as dt

import altair as alt
import pandas as pd
import pytz
import streamlit as st
from croniter import croniter
from databricks.sdk import WorkspaceClient

st.header("Jobs Execution Timeline")

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

col_date, col_tz = st.columns(2)
selected_date = col_date.date_input("Date", value=dt.date.today())
selected_tz = col_tz.selectbox(
    "Timezone", options=COMMON_TZ, index=0, key="jobs_timeline_tz"
)
tz = pytz.timezone(selected_tz)

# Day boundaries in epoch ms (in selected timezone)
day_start_local = tz.localize(dt.datetime.combine(selected_date, dt.time.min))
day_end_local = tz.localize(dt.datetime.combine(selected_date, dt.time.max))
now_local = dt.datetime.now(tz)
effective_end = min(day_end_local, now_local)
start_ms = int(day_start_local.timestamp() * 1000)
end_ms = int(effective_end.timestamp() * 1000)

STATE_COLORS = {
    "SCHEDULED": "#E0E0E0",  # gray
    "PENDING": "#FFD54F",  # yellow
    "RUNNING": "#4CAF50",  # green
    "SUCCESS": "#66BB6A",  # light green
    "FAILED": "#EF5350",  # red
    "TIMEDOUT": "#FF9800",  # orange
    "CANCELED": "#B0BEC5",  # gray
    "TERMINATING": "#CE93D8",  # purple
}

w = WorkspaceClient()


def quartz_to_standard_cron(quartz_expr: str) -> str | None:
    """Convert Quartz cron (sec min hr dom month dow [year]) to standard 5-field cron."""
    parts = quartz_expr.strip().split()
    if len(parts) < 6:
        return None
    # Drop seconds (field 0) and year (field 6) if present
    parts = parts[1:6]
    # Replace ? with *
    parts = [p.replace("?", "*") for p in parts]
    return " ".join(parts)


with st.spinner("Fetching job runs…"):
    try:
        runs = list(
            w.jobs.list_runs(
                start_time_from=start_ms,
                start_time_to=end_ms,
                expand_tasks=False,
            )
        )
    except Exception as e:
        st.error(f"Failed to fetch runs: {e}")
        st.stop()

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

    if lcs == "RUNNING":
        display_state = "RUNNING"
    elif lcs in ("PENDING", "QUEUED", "BLOCKED"):
        display_state = "PENDING"
    elif lcs == "TERMINATING":
        display_state = "TERMINATING"
    elif lcs == "INTERNAL_ERROR" or lcs == "SKIPPED":
        display_state = "FAILED"
    elif lcs == "TERMINATED":
        state_map = {
            "SUCCESS": "SUCCESS",
            "FAILED": "FAILED",
            "TIMEDOUT": "TIMEDOUT",
            "CANCELED": "CANCELED",
            "INTERNAL_ERROR": "FAILED",
            "EXCLUDED": "CANCELED",
        }
        display_state = state_map.get(rs, "FAILED" if rs else lcs)
    else:
        display_state = lcs or "FAILED"

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

# Fetch scheduled jobs and compute expected execution bars
with st.spinner("Fetching scheduled jobs…"):
    try:
        all_jobs = list(w.jobs.list(expand_tasks=False))
    except Exception:
        all_jobs = []

    scheduled_segments = []
    for job in all_jobs:
        schedule = job.settings.schedule if job.settings else None
        if not schedule or not schedule.quartz_cron_expression:
            continue
        if (
            hasattr(schedule, "pause_status")
            and schedule.pause_status
            and schedule.pause_status.value == "PAUSED"
        ):
            continue

        job_name = job.settings.name or f"job-{job.job_id}"
        std_cron = quartz_to_standard_cron(schedule.quartz_cron_expression)
        if not std_cron:
            continue

        # Determine schedule timezone
        sched_tz = pytz.timezone(schedule.timezone_id) if schedule.timezone_id else tz

        # Get last completed run duration for this job
        last_duration_s = 600  # default 10 min if no history
        try:
            last_runs = list(
                w.jobs.list_runs(
                    job_id=job.job_id,
                    limit=1,
                    completed_only=True,
                )
            )
            if last_runs and last_runs[0].run_duration:
                last_duration_s = max(last_runs[0].run_duration / 1000, 300)
        except Exception:
            pass

        # Find fire times within the selected day
        try:
            iter_start = day_start_local.astimezone(sched_tz).replace(tzinfo=None)
            iter_end = day_end_local.astimezone(sched_tz).replace(tzinfo=None)
            cron = croniter(std_cron, iter_start - dt.timedelta(seconds=1))
            while True:
                fire_naive = cron.get_next(dt.datetime)
                if fire_naive > iter_end:
                    break
                fire_local = sched_tz.localize(fire_naive).astimezone(tz)
                fire_end = fire_local + dt.timedelta(seconds=last_duration_s)
                scheduled_segments.append(
                    {
                        "job": job_name,
                        "run_id": None,
                        "state": "SCHEDULED",
                        "start": fire_local,
                        "end": fire_end,
                    }
                )
        except Exception:
            continue

all_segments = segments + scheduled_segments

if not all_segments:
    st.info("No job runs or scheduled jobs found for the selected date.")
    st.stop()

df = pd.DataFrame(all_segments)

# Job filter
job_names = sorted(df["job"].unique())
selected_jobs = st.multiselect("Jobs", options=job_names, default=job_names)
if not selected_jobs:
    st.warning("Select at least one job.")
    st.stop()

df = df[df["job"].isin(selected_jobs)]

# Strip timezone for Altair compatibility
df["start"] = df["start"].apply(lambda x: x.replace(tzinfo=None))
df["end"] = df["end"].apply(lambda x: x.replace(tzinfo=None))


domain = list(STATE_COLORS.keys())
range_ = list(STATE_COLORS.values())

# Add opacity column: scheduled bars are semi-transparent
df["_opacity"] = df["state"].apply(lambda s: 0.35 if s == "SCHEDULED" else 1.0)

# Fixed x-axis domain: full day 00:00–24:00
day_start_naive = day_start_local.replace(tzinfo=None)
day_end_naive = day_start_naive + dt.timedelta(days=1)

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
            legend=alt.Legend(title="State"),
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

# Include all runs (actual + scheduled) for concurrency
runs_df = df[df["_opacity"] > 0].copy()  # exclude invisible anchors

if runs_df.empty:
    combined = timeline_chart.properties(title="Jobs Execution Timeline")
else:
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
                "parallel_jobs:Q", title="Parallel Jobs", axis=alt.Axis(tickMinStep=1, format="d")
            ),
            tooltip=[
                alt.Tooltip("time:T", title="Window", format="%H:%M"),
                alt.Tooltip("parallel_jobs:Q", title="Jobs"),
            ],
        )
        .properties(height=200)
    )

    combined = alt.vconcat(timeline_chart, concurrency_chart).resolve_scale(x="shared")

st.altair_chart(combined, use_container_width=True)
