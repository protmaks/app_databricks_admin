import datetime as dt

import altair as alt
import pandas as pd
import pytz
import streamlit as st
from databricks.sdk import WorkspaceClient
from menu.compute.utils import make_workspace_client, COMMON_TZ, match_team_rules
from menu.settings.storage import get_cached_settings

st.header("Job Fails Details")

_w_settings = make_workspace_client()
_settings = get_cached_settings(_w_settings)
_global_tz = _settings["timezone"]
_teams_cfg = _settings["teams"]
_team_names = [t["name"] for t in _teams_cfg]

col_tz, col_days, col_teams = st.columns([0.12, 0.63, 0.25])
_tz_from_url = st.query_params.get("tz", _global_tz)
_tz_index = COMMON_TZ.index(_tz_from_url) if _tz_from_url in COMMON_TZ else COMMON_TZ.index(_global_tz) if _global_tz in COMMON_TZ else 0
selected_tz = col_tz.selectbox("Timezone", options=COMMON_TZ, index=_tz_index, key="fails_tz")
_days_from_url = int(st.query_params.get("days", 30))
_days_default = max(1, min(60, _days_from_url))
lookback_days = col_days.slider("Lookback days", min_value=1, max_value=60, value=_days_default)
if "fails_teams" not in st.session_state:
    _default_team_ids = _settings.get("default_teams", [])
    _id_to_name = {t["id"]: t["name"] for t in _teams_cfg}
    _default_team_names = [_id_to_name[tid] for tid in _default_team_ids if tid in _id_to_name]
    st.session_state["fails_teams"] = [n for n in _default_team_names if n in _team_names]
selected_teams = col_teams.multiselect(
    "Teams", options=_team_names, default=st.session_state["fails_teams"],
    placeholder="All teams", key="fails_teams",
)
st.query_params["tz"] = selected_tz
st.query_params["days"] = str(lookback_days)

tz = pytz.timezone(selected_tz)
now_local = dt.datetime.now(tz)
start_ms = int((now_local - dt.timedelta(days=lookback_days)).timestamp() * 1000)
end_ms = int(now_local.timestamp() * 1000)

w = make_workspace_client()

with st.spinner("Fetching failed runs…"):
    try:
        all_jobs = list(w.jobs.list(expand_tasks=False))
    except Exception as e:
        st.error(f"Failed to fetch job list: {e}")
        st.stop()

    try:
        failed_runs = list(
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

registry_id_to_name = {
    j.job_id: (j.settings.name or f"job-{j.job_id}")
    for j in all_jobs if j.job_id
}

records = []
for run in failed_runs:
    if not run.start_time:
        continue
    rs = run.state.result_state.value if run.state and run.state.result_state else None
    if not rs:
        continue

    name = registry_id_to_name.get(run.job_id) or run.run_name or f"job-{run.job_id}"
    run_start = dt.datetime.fromtimestamp(run.start_time / 1000, tz=pytz.utc).astimezone(tz)
    run_end = (
        dt.datetime.fromtimestamp(run.end_time / 1000, tz=pytz.utc).astimezone(tz)
        if run.end_time and run.end_time > 0
        else run_start
    )
    duration_min = round((run_end - run_start).total_seconds() / 60, 1)
    error_msg = (run.state.state_message or "").strip() if run.state else ""

    if rs == "SUCCESS":
        status = "SUCCESS"
    elif rs == "CANCELED":
        status = "CANCELED"
    elif rs == "TIMEDOUT":
        status = "TIMEDOUT"
    else:
        status = "FAILED"

    records.append({
        "job":          name,
        "job_id":       run.job_id,
        "run_id":       run.run_id,
        "status":       status,
        "run_time":     run_start.replace(tzinfo=None),
        "duration_min": duration_min,
        "error":        error_msg,
    })

if selected_teams:
    matched_ids = {
        j.job_id for j in all_jobs
        if j.job_id and any(
            m in selected_teams
            for m in match_team_rules(
                j.settings.name or f"job-{j.job_id}" if j.settings else f"job-{j.job_id}",
                getattr(j, "creator_user_name", None) or "unknown",
                _teams_cfg,
            )
        )
    }
    records = [r for r in records if r.get("job_id") in matched_ids]

if not records:
    st.info(f"No completed runs in the last {lookback_days} days.")
    st.stop()

df_all = pd.DataFrame(records)
df_all["date"] = df_all["run_time"].dt.normalize()

# Last run per (job, day)
df_last = df_all.sort_values("run_time").groupby(["job", "date"], as_index=False).last()

# failures-only subset: days where last run of the day was a failure
df = df_last[df_last["status"].isin(["FAILED", "TIMEDOUT"])].copy()

# ── Summary metrics ──────────────────────────────────────────────────────────
total_jobs = df_last["job"].nunique()
total_fails = df["date"].nunique()
unique_jobs = df["job"].nunique()

c1, c2, c3 = st.columns(3)
c1.metric("Total jobs", total_jobs)
c2.metric("Days ended with failure", total_fails)
c3.metric("Affected jobs", unique_jobs)

st.divider()

# ── Runs per day (stacked by status, based on last run per job per day) ───────
daily_counts = (
    df_last.groupby(["date", "status"])
    .agg(jobs=("job", "count"))
    .reset_index()
)

status_colors = {
    "SUCCESS":  "#66BB6A",
    "FAILED":   "#EF5350",
    "TIMEDOUT": "#FFA726",
    "CANCELED": "#B0BEC5",
}

daily_chart = (
    alt.Chart(daily_counts)
    .mark_bar()
    .encode(
        x=alt.X("date:T", title="Date", axis=alt.Axis(format="%m-%d", labelAngle=-45)),
        y=alt.Y("jobs:Q", title="Jobs", axis=alt.Axis(tickMinStep=1, format="d")),
        color=alt.Color(
            "status:N",
            scale=alt.Scale(
                domain=list(status_colors.keys()),
                range=list(status_colors.values()),
            ),
            legend=alt.Legend(title="Status"),
        ),
        order=alt.Order("status:N"),
        tooltip=[
            alt.Tooltip("date:T", title="Date", format="%Y-%m-%d"),
            alt.Tooltip("status:N", title="Status"),
            alt.Tooltip("jobs:Q", title="Jobs"),
        ],
    )
    .properties(height=200, title="Last run per job per day")
)
st.altair_chart(daily_chart, use_container_width=True)

st.divider()

# ── Detailed table ────────────────────────────────────────────────────────────
st.subheader("Jobs whose last run of the day failed")

col_search, col_status = st.columns([0.7, 0.3])
search = col_search.text_input("Filter by job name", placeholder="type to filter…")
_status_options = ["FAILED", "TIMEDOUT"]
_status_from_url = st.query_params.get("status", "FAILED,TIMEDOUT").split(",")
_status_default = [s for s in _status_from_url if s in _status_options] or _status_options
status_filter = col_status.multiselect(
    "Status", options=_status_options, default=_status_default
)
st.query_params["status"] = ",".join(status_filter) if status_filter else ""

df_view = df.copy()
if search:
    df_view = df_view[df_view["job"].str.contains(search, case=False, na=False)]
if status_filter:
    df_view = df_view[df_view["status"].isin(status_filter)]

df_view = df_view.sort_values("run_time", ascending=False)

workspace_host = w.config.host.rstrip("/")

STATUS_BADGE = {
    "FAILED":   ("<span style='color:#EF5350;font-weight:600'>FAILED</span>"),
    "TIMEDOUT": ("<span style='color:#FFA726;font-weight:600'>TIMEDOUT</span>"),
}

rows_html = ""
for _, row in df_view.iterrows():
    job_url = f"{workspace_host}/jobs/{row['job_id']}"
    run_time = row["run_time"].strftime("%Y-%m-%d %H:%M") if pd.notna(row["run_time"]) else ""
    duration = row["duration_min"] if pd.notna(row["duration_min"]) else ""
    error = str(row["error"]) if pd.notna(row["error"]) else ""
    badge = STATUS_BADGE.get(row["status"], row["status"])
    run_url = f"{workspace_host}/jobs/{row['job_id']}/runs/{row['run_id']}"
    rows_html += (
        f"<tr>"
        f"<td><a href='{job_url}' target='_blank'>{row['job']}</a></td>"
        f"<td>{badge}</td>"
        f"<td><a href='{run_url}' target='_blank'>{run_time}</a></td>"
        f"<td style='text-align:right'>{duration}</td>"
        f"<td style='color:#888;font-size:0.85em'>{error}</td>"
        f"</tr>"
    )

st.markdown(
    f"""
    <style>
    .fails-table {{width:100%;border-collapse:collapse;font-size:0.9rem}}
    .fails-table th {{text-align:left;padding:6px 8px;border-bottom:2px solid #ddd;white-space:nowrap}}
    .fails-table td {{padding:5px 8px;border-bottom:1px solid #eee;vertical-align:top}}
    .fails-table a {{text-decoration:none;color:#1976D2}}
    .fails-table a:hover {{text-decoration:underline}}
    </style>
    <table class='fails-table'>
      <thead><tr>
        <th>Job</th><th>Status</th><th>Run Time</th><th>Duration (min)</th><th>Error Message</th>
      </tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
    """,
    unsafe_allow_html=True,
)
