import datetime as dt

import altair as alt
import pandas as pd
import pytz
import streamlit as st
from databricks.sdk.service.jobs import RunType
from menu.compute.utils import make_workspace_client

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

col_tz, col_days, col_teams = st.columns([0.12, 0.63, 0.25])
selected_tz = col_tz.selectbox(
    "Timezone", options=COMMON_TZ, index=0, key="last_run_tz"
)
lookback_days = col_days.slider("Lookback days", min_value=1, max_value=60, value=30)
col_teams.multiselect("Teams", options=[], default=[], disabled=True, help="Coming soon")

tz = pytz.timezone(selected_tz)
now_local = dt.datetime.now(tz)

start_ms = int((now_local - dt.timedelta(days=lookback_days)).timestamp() * 1000)
end_ms = int(now_local.timestamp() * 1000)

w = make_workspace_client()
user_w = w

with st.spinner("Fetching data…"):
    try:
        completed_runs = list(
            w.jobs.list_runs(
                start_time_from=start_ms,
                start_time_to=end_ms,
                completed_only=True,
                expand_tasks=False,
                run_type=RunType.JOB_RUN,
            )
        )
    except Exception as e:
        st.error(f"Failed to fetch runs: {e}")
        st.stop()

    try:
        all_jobs = list(w.jobs.list(expand_tasks=True))
    except Exception as e:
        st.error(f"Failed to fetch job list: {e}")
        st.stop()

    try:
        active_runs = list(w.jobs.list_runs(active_only=True, expand_tasks=False, run_type=RunType.JOB_RUN))
    except Exception:
        active_runs = []

# Exclude pipeline jobs (jobs whose tasks include a pipeline_task)
pipeline_job_ids = {
    j.job_id
    for j in all_jobs
    if j.job_id and j.settings and j.settings.tasks
    and any(t.pipeline_task is not None for t in j.settings.tasks)
}

# Registry: job_id → canonical name from job settings (jobs only, no pipelines)
registry_id_to_name = {
    j.job_id: (j.settings.name or f"job-{j.job_id}")
    for j in all_jobs if j.job_id and j.job_id not in pipeline_job_ids
}
job_to_id = {name: jid for jid, name in registry_id_to_name.items()}

# Map job name → active run_id (jobs only)
job_to_running_run_id = {}
for run in active_runs:
    if not run.run_id or run.job_id not in registry_id_to_name:
        continue
    name = registry_id_to_name.get(run.job_id)
    if name not in job_to_running_run_id:
        job_to_running_run_id[name] = run.run_id

# Build records for completed runs using canonical names
records = []
for run in completed_runs:
    rs = run.state.result_state.value if run.state and run.state.result_state else None
    if not rs or not run.start_time or run.job_id not in registry_id_to_name:
        continue

    name = registry_id_to_name[run.job_id]
    run_start = dt.datetime.fromtimestamp(
        run.start_time / 1000, tz=pytz.utc
    ).astimezone(tz)
    run_end = (
        dt.datetime.fromtimestamp(run.end_time / 1000, tz=pytz.utc).astimezone(tz)
        if run.end_time and run.end_time > 0
        else run_start
    )
    duration_min = (run_end - run_start).total_seconds() / 60

    if rs == "SUCCESS":
        status = "SUCCESS"
    elif rs == "CANCELED":
        status = "CANCELED"
    else:
        status = "FAILED"

    records.append(
        {
            "job": name,
            "job_id": run.job_id,
            "run_time": run_start,
            "duration_min": round(duration_min, 1),
            "status": status,
        }
    )

# Add currently running jobs (so today's cell shows RUNNING)
for run in active_runs:
    if not run.start_time or run.job_id not in registry_id_to_name:
        continue
    name = registry_id_to_name[run.job_id]
    run_start = dt.datetime.fromtimestamp(
        run.start_time / 1000, tz=pytz.utc
    ).astimezone(tz)
    elapsed_min = (now_local - run_start).total_seconds() / 60
    records.append(
        {
            "job": name,
            "job_id": run.job_id,
            "run_time": run_start,
            "duration_min": round(elapsed_min, 1),
            "status": "RUNNING",
        }
    )

if not records:
    st.info(f"No job runs found in the last {lookback_days} days.")
    st.stop()

df = pd.DataFrame(records)
df["run_time"] = df["run_time"].apply(lambda x: x.replace(tzinfo=None))
df["date"] = df["run_time"].dt.normalize()
df_last = (
    df.sort_values("run_time")
    .groupby(["job", "date"], as_index=False)
    .last()
)

# Only jobs that have runs in the period
job_names = sorted(df_last["job"].unique())

# Full grid: all jobs (from registry) × all days in lookback period
all_dates = pd.date_range(
    end=dt.datetime(now_local.year, now_local.month, now_local.day),
    periods=lookback_days,
    freq="D",
)
full_grid = pd.DataFrame(
    [(job, date) for job in job_names for date in all_dates],
    columns=["job", "date"],
)
df_last_dedup = df_last.drop_duplicates(["job", "date"])

df_grid = full_grid.merge(
    df_last_dedup[["job", "date", "status", "run_time", "duration_min"]],
    on=["job", "date"],
    how="left",
).drop_duplicates(["job", "date"])
df_grid["status"] = df_grid["status"].fillna("NO RUN")

status_colors = {
    "SUCCESS": "#66BB6A",
    "FAILED": "#EF5350",
    "CANCELED": "#707070",
    "RUNNING": "#EFC550",
    "NO RUN": "#EEEEEE",
}

# Worst status per job for label coloring (FAILED > CANCELED > RUNNING > SUCCESS > NO RUN)
_priority = {"FAILED": 0, "CANCELED": 1, "RUNNING": 2, "SUCCESS": 3, "NO RUN": 4}
df_worst = (
    df_grid.groupby("job")["status"]
    .agg(lambda s: min(s, key=lambda x: _priority.get(x, 9)))
    .reset_index()
    .rename(columns={"status": "worst_status"})
)

label_colors = {
    "FAILED":  "#EF5350",
    "CANCELED": "#707070",
    "RUNNING":  "#EFC550",
    "SUCCESS":  "#31333F",
    "NO RUN":   "#AAAAAA",
}

LABEL_W = 200
label_chart = (
    alt.Chart(df_worst)
    .mark_text(align="right", baseline="middle", fontSize=11, limit=LABEL_W - 5)
    .encode(
        y=alt.Y("job:N", sort=job_names, axis=None),
        x=alt.value(LABEL_W),
        text=alt.Text("job:N"),
        color=alt.Color(
            "worst_status:N",
            scale=alt.Scale(
                domain=list(label_colors.keys()),
                range=list(label_colors.values()),
            ),
            legend=None,
        ),
    )
    .properties(width=LABEL_W, height=alt.Step(25))
)

heatmap = (
    alt.Chart(df_grid)
    .mark_rect(stroke="white", strokeWidth=2)
    .encode(
        x=alt.X(
            "yearmonthdate(date):O",
            title="Date",
            axis=alt.Axis(labelAngle=-45, format="%m-%d"),
        ),
        y=alt.Y(
            "job:N",
            title="",
            sort=job_names,
            axis=alt.Axis(labels=False, ticks=False, domain=False),
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
            alt.Tooltip("run_time:T", title="Last Run Time", format="%Y-%m-%d %H:%M"),
            alt.Tooltip("duration_min:Q", title="Duration (min)"),
        ],
    )
    .properties(height=alt.Step(25))
)

chart = (
    alt.hconcat(label_chart, heatmap, spacing=0)
    .resolve_scale(y="shared", color="independent")
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
    display: flex !important;
    flex-direction: column !important;
    justify-content: flex-end !important;
    align-items: center !important;
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

col_btn, col_chart = st.columns([0.02, 0.98])
triggered_job = None

with col_chart:
    st.altair_chart(chart, use_container_width=True)

with col_btn:
    for jname in job_names:
        jid = job_to_id.get(jname)
        running_run_id = job_to_running_run_id.get(jname)
        if running_run_id:
            if st.button("■", key=f"stop_{jname}_{running_run_id}", use_container_width=True):
                triggered_job = ("stop", jname, int(running_run_id))
        elif jid:
            if st.button("▶", key=f"run_{jname}_{jid}", use_container_width=True):
                triggered_job = ("run", jname, int(jid))

if triggered_job:
    action, jname, id_ = triggered_job
    if action == "run":
        try:
            run_result = user_w.jobs.run_now(job_id=id_)
            st.success(f"Job **{jname}** started — run ID: {run_result.run_id}")
        except Exception as e:
            st.error(f"Failed to start **{jname}**: {e}")
    else:
        try:
            user_w.jobs.cancel_run(run_id=id_)
            st.success(f"Job **{jname}** stop requested — run ID: {id_}")
        except Exception as e:
            st.error(f"Failed to stop **{jname}**: {e}")
