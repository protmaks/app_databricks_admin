import streamlit as st
from concurrent.futures import ThreadPoolExecutor, as_completed
from databricks.sdk.service.jobs import RunType

from menu.compute.utils import make_workspace_client, match_team_rules
from menu.settings.storage import get_cached_settings

st.header("Jobs Performance")

w = make_workspace_client()
_workspace_host = (w.config.host or "").rstrip("/")
_settings = get_cached_settings(w)
_teams_cfg = _settings["teams"]
_team_names = [t["name"] for t in _teams_cfg]
_teams_by_name = {t["name"]: t for t in _teams_cfg}

st.markdown("""<style>
div[data-testid="column"],
div[data-testid="stHorizontalBlock"] { overflow: visible !important; }
div[data-testid="stHorizontalBlock"] button,
div[data-testid="stHorizontalBlock"] button:hover,
div[data-testid="stHorizontalBlock"] button:focus,
div[data-testid="stHorizontalBlock"] button:active,
div[data-testid="stHorizontalBlock"] button:focus-visible {
    background: transparent !important;
    border: none !important;
    border-color: transparent !important;
    box-shadow: none !important;
    outline: none !important;
    padding: 0 2px !important;
    font-weight: 700 !important;
    color: inherit !important;
    font-size: 0.95em !important;
}
div[data-testid="stHorizontalBlock"] button p {
    font-weight: 700 !important;
}
div[data-testid="stHorizontalBlock"] button:hover {
    color: #8b8b8b !important;
}
</style>""", unsafe_allow_html=True)


# ── helpers ────────────────────────────────────────────────────────────────────

def _format_cluster_size(spec) -> str:
    if spec is None:
        return "—"
    node = getattr(spec, "node_type_id", None) or "?"
    if getattr(spec, "autoscale", None):
        mn = spec.autoscale.min_workers
        mx = spec.autoscale.max_workers
        return f"{node} (auto {mn}–{mx})"
    nw = getattr(spec, "num_workers", None)
    if nw is not None:
        return f"{node} ×{nw}"
    return node


def _format_spark_version(spec) -> str:
    if spec is None:
        return "—"
    sv = getattr(spec, "spark_version", None) or "—"
    return sv.split("-")[0] if sv != "—" else "—"


def extract_cluster_info(job, cluster_cache: dict | None = None) -> tuple[str, str, str]:
    settings = job.settings
    if settings is None:
        return "Serverless", "—", "—"

    tasks = settings.tasks or []
    job_clusters_map = {
        jc.job_cluster_key: jc.new_cluster
        for jc in (settings.job_clusters or [])
        if getattr(jc, "job_cluster_key", None)
    }

    for task in tasks:
        if getattr(task, "sql_task", None) and getattr(task.sql_task, "warehouse_id", None):
            return "SQL Warehouse", "SQL Warehouse", "—"
        if getattr(task, "new_cluster", None):
            spec = task.new_cluster
            return "New Cluster", _format_cluster_size(spec), _format_spark_version(spec)
        if getattr(task, "job_cluster_key", None):
            spec = job_clusters_map.get(task.job_cluster_key)
            return "Job Cluster", _format_cluster_size(spec), _format_spark_version(spec)
        if getattr(task, "existing_cluster_id", None):
            cid = task.existing_cluster_id
            spec = (cluster_cache or {}).get(cid)
            if spec is not None:
                return "All-purpose", _format_cluster_size(spec), _format_spark_version(spec)
            return "All-purpose", "—", "—"

    return "Serverless", "—", "—"


def _format_duration(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"




# ── data fetch ─────────────────────────────────────────────────────────────────

with st.spinner("Fetching jobs…"):
    try:
        jobs = list(w.jobs.list(expand_tasks=True))
    except Exception as e:
        st.error(f"Failed to fetch jobs: {e}")
        st.stop()

    def _fetch_run_as(job_id: int) -> tuple[int, str | None]:
        try:
            return job_id, w.jobs.get(job_id=job_id).run_as_user_name
        except Exception:
            return job_id, None

    with ThreadPoolExecutor(max_workers=20) as _pool:
        _run_as_map: dict[int, str | None] = dict(
            f.result() for f in as_completed(
                _pool.submit(_fetch_run_as, j.job_id) for j in jobs if j.job_id
            )
        )

if not jobs:
    st.info("No jobs found.")
    st.stop()

# ── entity filters (creator / run_as / teams) ──────────────────────────────────

all_creators = sorted({
    getattr(j, "creator_user_name", None) or "unknown"
    for j in jobs
})

all_run_as = sorted({
    _run_as_map.get(j.job_id) or "unknown"
    for j in jobs
})

if "jobs_perf_creators" not in st.session_state:
    _qp = st.query_params.get("creators", "")
    st.session_state["jobs_perf_creators"] = [
        c for c in _qp.split(",") if c in all_creators
    ] if _qp else []

if "jobs_perf_run_as" not in st.session_state:
    _qp_ra = st.query_params.get("run_as", "")
    st.session_state["jobs_perf_run_as"] = [
        r for r in _qp_ra.split(",") if r in all_run_as
    ] if _qp_ra else []


def _on_creators_change():
    vals = st.session_state.get("jobs_perf_creators", [])
    if vals:
        st.query_params["creators"] = ",".join(vals)
    elif "creators" in st.query_params:
        del st.query_params["creators"]


def _on_run_as_change():
    vals = st.session_state.get("jobs_perf_run_as", [])
    if vals:
        st.query_params["run_as"] = ",".join(vals)
    elif "run_as" in st.query_params:
        del st.query_params["run_as"]


col_creator, col_run_as, col_teams = st.columns([0.33, 0.33, 0.33])
selected_creators = col_creator.multiselect(
    "Created by",
    options=all_creators,
    default=st.session_state["jobs_perf_creators"],
    placeholder="All creators",
    key="jobs_perf_creators",
    on_change=_on_creators_change,
)
selected_run_as = col_run_as.multiselect(
    "Run as",
    options=all_run_as,
    default=st.session_state["jobs_perf_run_as"],
    placeholder="All run as",
    key="jobs_perf_run_as",
    on_change=_on_run_as_change,
)
if "jobs_perf_teams" not in st.session_state:
    _default_team_ids = _settings.get("default_teams", [])
    _id_to_name = {t["id"]: t["name"] for t in _teams_cfg}
    _default_team_names = [_id_to_name[tid] for tid in _default_team_ids if tid in _id_to_name]
    st.session_state["jobs_perf_teams"] = [n for n in _default_team_names if n in _team_names]
selected_teams = col_teams.multiselect(
    "Teams", options=_team_names, default=st.session_state["jobs_perf_teams"],
    placeholder="All teams", key="jobs_perf_teams",
)

if selected_creators:
    jobs = [
        j for j in jobs
        if (getattr(j, "creator_user_name", None) or "unknown") in selected_creators
    ]

if selected_run_as:
    jobs = [
        j for j in jobs
        if (_run_as_map.get(j.job_id) or "unknown") in selected_run_as
    ]

if selected_teams:
    jobs = [
        j for j in jobs
        if any(
            m in selected_teams
            for m in match_team_rules(
                (j.settings.name or f"job-{j.job_id}") if j.settings else f"job-{j.job_id}",
                _run_as_map.get(j.job_id) or getattr(j, "creator_user_name", None) or "unknown",
                _teams_cfg,
                tags=j.settings.tags if j.settings else {},
            )
        )
    ]

if not jobs:
    st.info("No jobs match the selected filters.")
    st.stop()

# ── all-purpose cluster pre-fetch ──────────────────────────────────────────────
_existing_cluster_ids: set[str] = set()
for _j in jobs:
    if _j.settings:
        for _t in (_j.settings.tasks or []):
            _cid = getattr(_t, "existing_cluster_id", None)
            if _cid:
                _existing_cluster_ids.add(_cid)

_cluster_cache: dict[str, object] = {}
if _existing_cluster_ids:
    with st.spinner("Fetching cluster details…"):
        for _cid in _existing_cluster_ids:
            try:
                _cluster_cache[_cid] = w.clusters.get(_cid)
            except Exception:
                pass

# ── fetch completed runs globally and group by job_id ──────────────────────────

_job_ids = {j.job_id for j in jobs if j.job_id}

with st.spinner("Fetching completed runs…"):
    try:
        completed_runs = list(
            w.jobs.list_runs(
                completed_only=True,
                expand_tasks=False,
                run_type=RunType.JOB_RUN,
            )
        )
    except Exception as e:
        st.error(f"Failed to fetch runs: {e}")
        st.stop()

# For each job collect successful runs to compute avg of last 5 and last run duration
# _success_runs[job_id] = list of (end_ms, duration_sec) sorted by end_ms desc
_success_runs: dict[int, list[tuple[int, float]]] = {}

for _run in completed_runs:
    _jid = getattr(_run, "job_id", None)
    if _jid not in _job_ids:
        continue
    _state = getattr(_run, "state", None)
    _rs = getattr(_state, "result_state", None)
    _rs_val = (_rs.value if hasattr(_rs, "value") else str(_rs)) if _rs else ""
    if _rs_val.upper() != "SUCCESS":
        continue
    _st = getattr(_run, "start_time", None)
    _et = getattr(_run, "end_time", None)
    if _st and _et and _et > _st:
        _success_runs.setdefault(_jid, []).append((_et, (_et - _st) / 1000))

# perf_map[job_id] = (avg_last5_sec, last_duration_sec)
_perf_map: dict[int, tuple[float | None, float | None]] = {}
for _j in jobs:
    _jid = _j.job_id
    _runs = sorted(_success_runs.get(_jid, []), key=lambda x: x[0], reverse=True)
    if not _runs:
        _perf_map[_jid] = (None, None)
    else:
        _last5 = _runs[:5]
        _avg = sum(d for _, d in _last5) / len(_last5)
        _perf_map[_jid] = (_avg, _runs[0][1])

# ── pre-compute teams ──────────────────────────────────────────────────────────
_job_teams: dict[int, list[str]] = {}
for _j in jobs:
    _jname = (_j.settings.name or f"job-{_j.job_id}") if _j.settings else f"job-{_j.job_id}"
    _jcreator = _run_as_map.get(_j.job_id) or getattr(_j, "creator_user_name", None) or "unknown"
    _job_teams[_j.job_id] = match_team_rules(
        _jname, _jcreator, _teams_cfg, tags=_j.settings.tags if _j.settings else {}
    )

# ── statistics ─────────────────────────────────────────────────────────────────

total = len(jobs)
_min_runtime_str = (_settings.get("min_runtime_version") or "16.4").strip()
try:
    _min_rt_parts = _min_runtime_str.split(".")
    _min_rt = (int(_min_rt_parts[0]), int(_min_rt_parts[1]) if len(_min_rt_parts) > 1 else 0)
except (ValueError, IndexError):
    _min_rt = (16, 4)


def _is_old_runtime(sv: str) -> bool:
    if sv in ("—", ""):
        return False
    try:
        parts = sv.split(".")
        major, minor = int(parts[0]), int(parts[1]) if len(parts) > 1 else 0
        return (major, minor) < _min_rt
    except (ValueError, IndexError):
        return False


_spark_versions: dict[int, str] = {}
type_counts: dict[str, int] = {}
for j in jobs:
    ct, _, sv = extract_cluster_info(j, _cluster_cache)
    type_counts[ct] = type_counts.get(ct, 0) + 1
    _spark_versions[j.job_id] = sv

_ALLOWED_CLUSTER_TYPES = {"Job Cluster", "Serverless"}
job_cluster_count = sum(v for ct, v in type_counts.items() if ct not in _ALLOWED_CLUSTER_TYPES)
old_runtime = sum(1 for sv in _spark_versions.values() if _is_old_runtime(sv))
no_team = sum(1 for j in jobs if len(_job_teams[j.job_id]) != 1)

COL_WIDTHS  = [2.2, 1.0, 1.0, 1.2, 0.8, 1.4, 1.4]
COL_HEADERS = ["Job Name", "Team", "Cluster Type", "Run As", "Runtime", "Avg Last 5 Runs", "Last Run Duration"]

stat_cols = st.columns(COL_WIDTHS)


def _stat(col, label: str, value, color: str = "inherit") -> None:
    col.markdown(
        f"<div style='text-align:center;font-size:0.8em;color:rgba(250,250,250,0.6);margin-bottom:2px'>{label}</div>"
        f"<div style='text-align:center;font-size:1.6em;font-weight:600;color:{color}'>{value}</div>",
        unsafe_allow_html=True,
    )


_stat(stat_cols[0], "Total Jobs", total)
_no_team_color = "#ff8c00" if no_team > 0 else "inherit"
_stat(stat_cols[1], "No/Multi Team", no_team, _no_team_color)
_jc_color = "#ff4b4b" if job_cluster_count > 0 else "inherit"
_stat(stat_cols[2], "Wrong Cluster", job_cluster_count, _jc_color)
stat_cols[3].empty()
_rt_color = "#ff8c00" if old_runtime > 0 else "inherit"
stat_cols[4].markdown(
    f"<div style='text-align:center;font-size:0.8em;color:rgba(250,250,250,0.6);margin-bottom:2px'>Old Runtime &lt;{_min_runtime_str}</div>"
    f"<div style='text-align:center;font-size:1.6em;font-weight:600;color:{_rt_color}'>{old_runtime}</div>",
    unsafe_allow_html=True,
)
stat_cols[5].empty()
stat_cols[6].empty()

st.divider()

# ── sort ───────────────────────────────────────────────────────────────────────

if "jobs_perf_sort_col" not in st.session_state:
    st.session_state.jobs_perf_sort_col = st.query_params.get("sort_col") or "Avg Last 5 Runs"
    st.session_state.jobs_perf_sort_dir = int(st.query_params.get("sort_dir", "-1"))


def _sort_key(job):
    col = st.session_state.jobs_perf_sort_col
    ct, _, sv = extract_cluster_info(job, _cluster_cache)
    if col == "Job Name":
        return ((job.settings.name or f"job-{job.job_id}") if job.settings else f"job-{job.job_id}").lower()
    if col == "Team":
        _n = (job.settings.name or f"job-{job.job_id}") if job.settings else f"job-{job.job_id}"
        _c = _run_as_map.get(job.job_id) or getattr(job, "creator_user_name", None) or "unknown"
        return ", ".join(match_team_rules(_n, _c, _teams_cfg, tags=job.settings.tags if job.settings else {}))
    if col == "Cluster Type":
        return ct
    if col == "Run As":
        return (_run_as_map.get(job.job_id) or getattr(job, "creator_user_name", None) or "").lower()
    if col == "Runtime":
        if sv in ("—", ""):
            return (0, 0)
        try:
            parts = sv.split(".")
            return (int(parts[0]), int(parts[1]) if len(parts) > 1 else 0)
        except (ValueError, IndexError):
            return (0, 0)
    if col == "Last Success Time":
        _, last = _perf_map.get(job.job_id, (None, None))
        return last if last is not None else -1
    if col == "Last Run Duration":
        avg, _ = _perf_map.get(job.job_id, (None, None))
        return avg if avg is not None else -1
    return ""


if st.session_state.jobs_perf_sort_col:
    jobs = sorted(jobs, key=_sort_key, reverse=(st.session_state.jobs_perf_sort_dir == -1))

# ── table ──────────────────────────────────────────────────────────────────────

header_cols = st.columns(COL_WIDTHS)
for hcol, h in zip(header_cols, COL_HEADERS):
    is_active = st.session_state.jobs_perf_sort_col == h
    arrow = (" ▲" if st.session_state.jobs_perf_sort_dir == 1 else " ▼") if is_active else " ⇅"
    if hcol.button(f"{h}{arrow}", key=f"perf_sort_{h}", use_container_width=True):
        if st.session_state.jobs_perf_sort_col == h:
            st.session_state.jobs_perf_sort_dir *= -1
        else:
            st.session_state.jobs_perf_sort_col = h
            st.session_state.jobs_perf_sort_dir = 1
        st.query_params["sort_col"] = st.session_state.jobs_perf_sort_col
        st.query_params["sort_dir"] = str(st.session_state.jobs_perf_sort_dir)
        st.rerun()

st.divider()

for job in jobs:
    name = (
        (job.settings.name if job.settings and job.settings.name else None)
        or f"job-{job.job_id}"
    )
    job_id = job.job_id

    cluster_type, _, spark_ver = extract_cluster_info(job, _cluster_cache)
    run_as = _run_as_map.get(job_id) or "—"

    _matched_teams = _job_teams[job.job_id]
    if not _matched_teams:
        _team_display = "<span style='color:orange;font-size:0.8em'>no team</span>"
    elif len(_matched_teams) > 1:
        _team_display = f"<span style='color:orange'>{', '.join(_matched_teams)}</span>"
    else:
        _team_display = _matched_teams[0]

    avg_sec, last_sec = _perf_map.get(job_id, (None, None))

    def _dur_display(sec):
        if sec is None:
            return "<span style='color:gray'>—</span>"
        if sec > 7200:
            return f"<span style='color:#ff4b4b'>{_format_duration(sec)}</span>"
        if sec > 3600:
            return f"<span style='color:#ff8c00'>{_format_duration(sec)}</span>"
        return _format_duration(sec)

    avg_display = _dur_display(avg_sec)
    last_display = _dur_display(last_sec)

    row = st.columns(COL_WIDTHS)
    _job_url = f"{_workspace_host}/#job/{job_id}"
    row[0].markdown(
        f"<a href='{_job_url}' target='_blank' style='text-decoration:underline;color:inherit'>{name}</a>"
        f"<br><span style='color:gray;font-size:0.75em'>ID: {job_id}</span>",
        unsafe_allow_html=True,
    )
    row[1].markdown(f"<div style='text-align:center'>{_team_display}</div>", unsafe_allow_html=True)
    if cluster_type == "All-purpose":
        row[2].markdown("<div style='text-align:center'><span style='color:#ff4b4b'>All-purpose</span></div>", unsafe_allow_html=True)
    else:
        row[2].markdown(f"<div style='text-align:center'>{cluster_type}</div>", unsafe_allow_html=True)
    row[3].html(f"<span>{run_as}</span>")
    _sv_color = "#ff8c00" if _is_old_runtime(spark_ver) else "inherit"
    row[4].markdown(f"<div style='text-align:center;color:{_sv_color}'>{spark_ver}</div>", unsafe_allow_html=True)
    row[5].markdown(f"<div style='text-align:center'>{avg_display}</div>", unsafe_allow_html=True)
    row[6].markdown(f"<div style='text-align:center'>{last_display}</div>", unsafe_allow_html=True)
    st.markdown("<hr style='margin:4px 0;border:none;border-top:1px solid rgba(128,128,128,0.15);'>", unsafe_allow_html=True)
