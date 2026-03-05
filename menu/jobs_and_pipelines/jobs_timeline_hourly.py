import datetime as dt

import altair as alt
import pandas as pd
import pytz
import streamlit as st
from croniter import croniter
from databricks.sdk import WorkspaceClient

from menu.compute.utils import quartz_to_standard_cron, resolve_display_state

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

col_date, col_tz, col_teams = st.columns([0.15, 0.10, 0.75])
selected_date = col_date.date_input("Date", value=dt.date.today())
selected_tz = col_tz.selectbox(
    "Timezone", options=COMMON_TZ, index=0, key="jobs_timeline_tz"
)
col_teams.multiselect("Teams", options=[], default=[], disabled=True, help="Coming soon")
tz = pytz.timezone(selected_tz)

if selected_date == dt.date.today():
    st.markdown("""
    <style>
    [data-testid="stDateInput"] input {
        border-color: #1976D2 !important;
        color: #1976D2 !important;
        font-weight: 600 !important;
    }
    </style>
    """, unsafe_allow_html=True)

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
    "RUNNING": "#2196F3",  # blue
    "SUCCESS": "#66BB6A",  # light green
    "FAILED": "#EF5350",  # red
    "TIMEDOUT": "#FF9800",  # orange
    "CANCELED": "#B0BEC5",  # gray
    "TERMINATING": "#CE93D8",  # purple
}

w = WorkspaceClient(profile="DEFAULT")


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
    display_state = resolve_display_state(lcs, rs)

    name = run.run_name or f"job-{run.job_id}"

    segments.append(
        {
            "job": name,
            "job_id": run.job_id,
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
                        "job_id": job.job_id,
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

job_names = sorted(df["job"].unique())
selected_jobs = job_names

# Build lookup: job name → job_id (before anchors are added)
job_to_id = df.drop_duplicates("job").set_index("job")["job_id"].to_dict()

# Build lookup: job name → run_id for currently RUNNING jobs
running_df = df[(df["state"] == "RUNNING") & (df["run_id"].notna())]
job_to_running_run_id = (
    running_df.drop_duplicates("job").set_index("job")["run_id"].to_dict()
)

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
            "job_id": None,
            "state": "TERMINATED",
            "start": day_start_naive,
            "end": day_start_naive,
            "_opacity": 0,
            "run_id": None,
        },
        {
            "job": anchor_jobs,
            "job_id": None,
            "state": "TERMINATED",
            "start": day_end_naive,
            "end": day_end_naive,
            "_opacity": 0,
            "run_id": None,
        },
    ]
)
df = pd.concat([df, anchors], ignore_index=True)

# Alternating row background stripes
sorted_jobs = sorted(df["job"].unique())
stripe_df = pd.DataFrame([
    {"job": jname, "start": day_start_naive, "end": day_end_naive}
    for i, jname in enumerate(sorted_jobs)
    if i % 2 == 0
])
bg_chart = (
    alt.Chart(stripe_df)
    .mark_rect(color="#F8F8F8", opacity=1.0)
    .encode(
        x=alt.X("start:T"),
        x2=alt.X2("end:T"),
        y=alt.Y("job:N", sort=alt.SortField("job")),
    )
)

bars_chart = (
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
)

timeline_chart = (
    alt.layer(bg_chart, bars_chart)
    .properties(height=alt.Step(25))
    .resolve_scale(color="independent")
)

# --- Parallel jobs concurrency chart (5-min windows) ---

# Include all runs (actual + scheduled) for concurrency
runs_df = df[df["_opacity"] > 0].copy()  # exclude invisible anchors

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
                "time:T",
                title="Time",
                axis=alt.Axis(format="%H:%M", labelAngle=-45),
                scale=alt.Scale(domain=[day_start_naive, day_end_naive]),
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

st.markdown("""
<style>
.st-emotion-cache-13tburv {
    min-height: 0 !important;
}
div[data-testid="stVerticalBlock"] {
    gap: 0 !important;
    overflow: visible !important;
}
div[data-testid="column"],
div[data-testid="stHorizontalBlock"] {
    overflow: visible !important;
}
div[data-testid="element-container"]:has(.stButton) {
    margin: 0 !important;
    padding: 0 !important;
    line-height: 0 !important;
}
button[data-testid="stBaseButton-secondary"] {
    height: 25px !important;
    min-height: 25px !important;
    padding: 0 4px !important;
    margin: 0 !important;
    border: none !important;
    background: transparent !important;
    box-shadow: none !important;
    font-size: 10px !important;
    line-height: 25px !important;
}
button[data-testid="stBaseButton-secondary"]:hover {
    background: rgba(49,51,63,0.08) !important;
    border: none !important;
}
div[data-testid="stButton"] {
    margin: 0 !important;
    padding: 0 !important;
    line-height: 1 !important;
    width: 100% !important;
}
div[data-testid="stButton"] > div,
div[data-testid="stButton"] > div > div,
div.stTooltipIcon,
div[data-testid="stTooltipHoverTarget"] {
    width: 100% !important;
    padding: 0 !important;
    margin: 0 !important;
}
div[data-testid="stTooltipHoverTarget"] {
    justify-content: center !important;
}
div[data-testid="stButton"] > button,
div[data-testid="stButton"] > div button {
    width: 100% !important;
    padding: 0 !important;
}
button[data-testid="stBaseButton-secondary"] p {
    margin: 0 !important;
    padding: 0 !important;
    line-height: 1 !important;
}
[data-testid="stMarkdownContainer"] p {
    font-size: 0.6rem !important;
}
</style>
""", unsafe_allow_html=True)

# Layout: narrow button column to the left, charts to the right
col_btn, col_chart = st.columns([0.02, 0.98])

triggered_job = None

with col_chart:
    if not runs_df.empty:
        combined_chart = (
            alt.vconcat(concurrency_chart, timeline_chart, spacing=50)
            .resolve_scale(x="shared")
            .properties(padding={"top": 50, "bottom": 5, "left": 0, "right": 0})
        )
        st.altair_chart(combined_chart, use_container_width=True)
    else:
        st.altair_chart(timeline_chart, use_container_width=True)

with col_btn:
    if not runs_df.empty:
        # Spacer to push buttons past the concurrency chart area
        concurrency_height_px = 50 + 150 + 55 + 90  # top padding + chart + x-axis labels + spacing
        st.markdown(
            f'<div style="height:{concurrency_height_px}px"></div>',
            unsafe_allow_html=True,
        )
    if selected_date == dt.date.today():
        for jname in job_names:
            jid = job_to_id.get(jname)
            running_run_id = job_to_running_run_id.get(jname)
            if running_run_id:
                if st.button("■", key=f"stop_{running_run_id}", use_container_width=True):
                    triggered_job = ("stop", jname, int(running_run_id))
            elif jid:
                if st.button("▶", key=f"run_{jid}", use_container_width=True):
                    triggered_job = ("run", jname, int(jid))

if triggered_job:
    action, jname, id_ = triggered_job
    if action == "run":
        try:
            run_result = w.jobs.run_now(job_id=id_)
            st.success(f"Job **{jname}** started — run ID: {run_result.run_id}")
        except Exception as e:
            st.error(f"Failed to start **{jname}**: {e}")
    else:
        try:
            w.jobs.cancel_run(run_id=id_)
            st.success(f"Job **{jname}** stop requested — run ID: {id_}")
        except Exception as e:
            st.error(f"Failed to stop **{jname}**: {e}")
