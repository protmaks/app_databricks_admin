import streamlit as st
from concurrent.futures import ThreadPoolExecutor, as_completed
from databricks.sdk import WorkspaceClient

from menu.compute.utils import quartz_to_standard_cron, make_workspace_client, match_team_rules
from menu.settings.storage import get_cached_settings

st.header("Jobs Settings")

w = make_workspace_client()
_settings = get_cached_settings(w)
_teams_cfg = _settings["teams"]
_team_names = [t["name"] for t in _teams_cfg]
_teams_by_name = {t["name"]: t for t in _teams_cfg}

st.markdown("""<style>
div[data-testid="column"],
div[data-testid="stHorizontalBlock"] { overflow: visible !important; }
.tt { position: relative; display: inline-block; cursor: default; }
.tt .tt-box {
    display: none; position: absolute; bottom: 130%; left: 50%;
    transform: translateX(-50%);
    background: #1e1e2e; color: #e0e0e0; border: 1px solid #444;
    border-radius: 6px; padding: 8px 10px; font-size: 12px;
    min-width: 180px; max-width: 320px;
    white-space: normal; word-wrap: break-word;
    z-index: 9999; box-shadow: 0 4px 12px rgba(0,0,0,.4); line-height: 1.5;
}
.tt:hover .tt-box { display: block; }
.red-cell { color: #ff4b4b !important; }
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

_DOW_NAME = {"0": "Sun", "1": "Mon", "2": "Tue", "3": "Wed",
             "4": "Thu", "5": "Fri", "6": "Sat", "7": "Sun"}
_DOW_FULL = {"0": "Sundays", "1": "Mondays", "2": "Tuesdays", "3": "Wednesdays",
             "4": "Thursdays", "5": "Fridays", "6": "Saturdays", "7": "Sundays"}


def cron_to_human(cron: str) -> str:
    """Convert a 5-field cron to a short human-readable string."""
    try:
        parts = cron.strip().split()
        if len(parts) != 5:
            return cron
        minute, hour, dom, month, dow = parts

        # Every N minutes
        if minute.startswith("*/") and hour == "*" and dom == "*" and month == "*" and dow == "*":
            n = minute[2:]
            return "Every minute" if n == "1" else f"Every {n} min"

        # Every N hours (at :00)
        if minute == "0" and hour.startswith("*/") and dom == "*" and month == "*" and dow == "*":
            n = hour[2:]
            return "Every hour" if n == "1" else f"Every {n}h"

        # Fixed time patterns
        if minute.isdigit() and hour.isdigit():
            t = f"{int(hour):02d}:{int(minute):02d}"
            if dom == "*" and month == "*":
                if dow == "*":
                    return f"Daily at {t}"
                if dow in ("1-5", "MON-FRI"):
                    return f"Weekdays at {t}"
                if dow in ("0,6", "6,0", "SAT,SUN", "SUN,SAT"):
                    return f"Weekends at {t}"
                if dow in _DOW_FULL:
                    return f"{_DOW_FULL[dow]} at {t}"
                # multiple days like 1,3,5
                days = [_DOW_NAME.get(d, d) for d in dow.split(",")]
                return f"{'/'.join(days)} at {t}"
            if dom != "*" and month == "*" and dow in ("*", "?"):
                return f"Monthly on {dom} at {t}"
        return cron
    except Exception:
        return cron


def make_tooltip(icon: str, html_content: str | None) -> str:
    if not html_content:
        return icon
    return (
        f'<span class="tt">{icon}'
        f'<span class="tt-box">{html_content}</span>'
        f'</span>'
    )


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


def extract_schedule_info(job) -> tuple[str, str, str]:
    sched = getattr(job.settings, "schedule", None) if job.settings else None
    if sched is None:
        return "Not scheduled", "", ""
    quartz = getattr(sched, "quartz_cron_expression", None) or ""
    pause = getattr(sched, "pause_status", None)
    tz = getattr(sched, "timezone_id", None) or ""
    cron5 = quartz_to_standard_cron(quartz) if quartz else ""
    pause_str = (pause.value if hasattr(pause, "value") else str(pause)) if pause is not None else ""
    paused = pause_str.split(".")[-1].upper() == "PAUSED"
    label = "Scheduled (paused)" if paused else "Scheduled"
    return label, cron5 or quartz, tz


def extract_threshold_tooltip(job) -> str | None:
    if not job.settings:
        return None
    timeout = getattr(job.settings, "timeout_seconds", None)
    if not timeout:
        return None
    minutes, seconds = divmod(int(timeout), 60)
    hours, minutes = divmod(minutes, 60)
    parts = []
    if hours:   parts.append(f"{hours}h")
    if minutes: parts.append(f"{minutes}m")
    if seconds: parts.append(f"{seconds}s")
    duration = " ".join(parts) or f"{timeout}s"
    return f"<b>Timeout:</b> {duration}"


def _check_access(job, matched_teams: list[str], teams_by_name: dict, job_can_manage: dict) -> str:
    """Returns 'no_team', 'ok', or 'fail'.
    Uses pre-fetched Permissions API data (CAN_MANAGE or IS_OWNER).
    """
    if not matched_teams:
        return "no_team"
    can_manage_set = job_can_manage.get(job.job_id, set())
    for tname in matched_teams:
        cfg = teams_by_name.get(tname, {})
        access_val = (cfg.get("access") or "").strip()
        if not access_val:
            return "ok" if can_manage_set else "fail"
        return "ok" if access_val in can_manage_set else "fail"
    return "fail"


def _check_run_as(job, matched_teams: list[str], teams_by_name: dict, run_as_value: str | None = None) -> str:
    """Returns 'no_team', 'ok', or 'fail'."""
    if not matched_teams:
        return "no_team"
    job_run_as = (run_as_value or getattr(job, "creator_user_name", None) or "").strip().lower()
    for tname in matched_teams:
        cfg = teams_by_name.get(tname, {})
        allowed = [a.strip().lower() for a in (cfg.get("run_as") or "").split(",") if a.strip()]
        if not allowed:
            return "ok"
        return "ok" if job_run_as in allowed else "fail"
    return "fail"


def _check_notification(job, matched_teams: list[str], teams_by_name: dict) -> str:
    """Returns 'no_team', 'ok', or 'fail'."""
    if not matched_teams:
        return "no_team"
    email_set = set()
    email = getattr(job.settings, "email_notifications", None) if job.settings else None
    if email:
        for ev in ("on_failure", "on_success", "on_start", "on_duration_warning_threshold_exceeded"):
            addrs = getattr(email, ev, None)
            if addrs:
                email_set.update(addrs)
    for tname in matched_teams:
        cfg = teams_by_name.get(tname, {})
        notif_val = (cfg.get("notification") or "").strip()
        if not notif_val:
            return "ok" if email_set else "fail"
        return "ok" if notif_val in email_set else "fail"
    return "fail"


def extract_notification_tooltip(job) -> str | None:
    if not job.settings:
        return None
    email = getattr(job.settings, "email_notifications", None)
    webhook = getattr(job.settings, "webhook_notifications", None)
    lines = []
    events = ("on_failure", "on_duration_warning_threshold_exceeded")
    if email:
        for ev in events:
            addrs = getattr(email, ev, None)
            if addrs:
                label = ev.replace("on_", "").replace("_", " ").title()
                lines.append(f"<b>{label}:</b> {', '.join(addrs)}")
    if webhook:
        for ev in events:
            hooks = getattr(webhook, ev, None)
            if hooks:
                label = ev.replace("on_", "").replace("_", " ").title()
                ids = ", ".join(getattr(h, "id", "?") for h in hooks)
                lines.append(f"<b>Webhook {label}:</b> {ids}")
    return "<br>".join(lines) if lines else None


def extract_access_tooltip(job) -> str | None:
    if not job.settings:
        return None
    acl = getattr(job.settings, "access_control_list", None)
    if not acl:
        return None
    lines = ["<b>Access Control:</b>"]
    for entry in acl:
        principal = (
            getattr(entry, "user_name", None)
            or getattr(entry, "group_name", None)
            or getattr(entry, "service_principal_name", None)
            or "?"
        )
        perm = getattr(entry, "permission_level", "?")
        p_str = perm.value if hasattr(perm, "value") else str(perm)
        lines.append(f"{principal}: {p_str}")
    return "<br>".join(lines)


def extract_notebooks_path_tooltip(job) -> str | None:
    if not job.settings:
        return None
    tasks = job.settings.tasks or []
    paths = []
    for task in tasks:
        nb = getattr(task, "notebook_task", None)
        if nb:
            path = getattr(nb, "notebook_path", None)
            if path:
                paths.append(path)
    if not paths:
        return None
    return "<br>".join(f"<b>Notebook:</b> {p}" for p in paths)


def _check_notebooks_path(job, matched_teams: list[str], teams_by_name: dict) -> str:
    """Returns 'no_team', 'ok', or 'fail'."""
    if not matched_teams:
        return "no_team"
    tasks = (job.settings.tasks or []) if job.settings else []
    job_paths = []
    for task in tasks:
        nb = getattr(task, "notebook_task", None)
        if nb:
            path = getattr(nb, "notebook_path", None)
            if path:
                job_paths.append(path)
    for tname in matched_teams:
        cfg = teams_by_name.get(tname, {})
        nb_val = (cfg.get("notebooks_path") or "").strip()
        if not nb_val:
            return "ok" if job_paths else "fail"
        return "ok" if any(p.startswith(nb_val) for p in job_paths) else "fail"
    return "fail"


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


# ── filters ────────────────────────────────────────────────────────────────────

all_creators = sorted({
    getattr(j, "creator_user_name", None) or "unknown"
    for j in jobs
})

# Restore filter state from URL query params on first load
if "jobs_settings_creators" not in st.session_state:
    _qp = st.query_params.get("creators", "")
    st.session_state["jobs_settings_creators"] = [
        c for c in _qp.split(",") if c in all_creators
    ] if _qp else []

def _on_creators_change():
    vals = st.session_state.get("jobs_settings_creators", [])
    if vals:
        st.query_params["creators"] = ",".join(vals)
    elif "creators" in st.query_params:
        del st.query_params["creators"]

col_creator, col_teams = st.columns([0.5, 0.5])
selected_creators = col_creator.multiselect(
    "Created by",
    options=all_creators,
    default=st.session_state["jobs_settings_creators"],
    placeholder="All creators",
    key="jobs_settings_creators",
    on_change=_on_creators_change,
)
if "jobs_settings_teams" not in st.session_state:
    _default_team_ids = _settings.get("default_teams", [])
    _id_to_name = {t["id"]: t["name"] for t in _teams_cfg}
    _default_team_names = [_id_to_name[tid] for tid in _default_team_ids if tid in _id_to_name]
    st.session_state["jobs_settings_teams"] = [n for n in _default_team_names if n in _team_names]
selected_teams = col_teams.multiselect(
    "Teams", options=_team_names, default=st.session_state["jobs_settings_teams"],
    placeholder="All teams", key="jobs_settings_teams",
)

if selected_creators:
    jobs = [
        j for j in jobs
        if (getattr(j, "creator_user_name", None) or "unknown") in selected_creators
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

# ── permissions pre-fetch (Permissions API) ────────────────────────────────────
_job_can_manage: dict[int, set[str]] = {}
with st.spinner("Fetching job permissions…"):
    for _j in jobs:
        try:
            _obj_perms = w.permissions.get("jobs", str(_j.job_id))
            _can_manage: set[str] = set()
            for _acl_e in (_obj_perms.access_control_list or []):
                _p = (
                    getattr(_acl_e, "user_name", None)
                    or getattr(_acl_e, "group_name", None)
                    or getattr(_acl_e, "service_principal_name", None)
                )
                if not _p:
                    continue
                for _perm in (getattr(_acl_e, "all_permissions", None) or []):
                    _level = getattr(_perm, "permission_level", None)
                    _ls = _level.value if _level and hasattr(_level, "value") else (str(_level) if _level else "")
                    if _ls in ("CAN_MANAGE", "IS_OWNER"):
                        _can_manage.add(_p)
                        break
            _job_can_manage[_j.job_id] = _can_manage
        except Exception:
            _job_can_manage[_j.job_id] = set()

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

COL_WIDTHS  = [2.0, 1.0, 0.9, 1.2, 0.8, 1.0, 0.8, 0.6, 0.6, 0.6]
COL_HEADERS = ["Job Name", "Team", "Cluster Type", "Run As", "Runtime", "Schedule", "Threshold", "Notif.", "Access", "Path"]

# ── pre-compute per-job check results (reused in stats + row render) ───────────
_job_checks: dict[int, dict] = {}
for _j in jobs:
    _jname = (_j.settings.name or f"job-{_j.job_id}") if _j.settings else f"job-{_j.job_id}"
    _jcreator = _run_as_map.get(_j.job_id) or getattr(_j, "creator_user_name", None) or "unknown"
    _jteams = match_team_rules(_jname, _jcreator, _teams_cfg, tags=_j.settings.tags if _j.settings else {})
    _job_checks[_j.job_id] = {
        "teams":        _jteams,
        "is_scheduled": getattr(_j.settings, "schedule", None) is not None if _j.settings else False,
        "notif":        _check_notification(_j, _jteams, _teams_by_name),
        "access":       _check_access(_j, _jteams, _teams_by_name, _job_can_manage),
        "path":         _check_notebooks_path(_j, _jteams, _teams_by_name),
        "run_as":       _check_run_as(_j, _jteams, _teams_by_name, run_as_value=_run_as_map.get(_j.job_id)),
    }

# ── statistics ─────────────────────────────────────────────────────────────────

total = len(jobs)

type_counts: dict[str, int] = {}
_spark_versions: dict[int, str] = {}
for j in jobs:
    ct, _, sv = extract_cluster_info(j, _cluster_cache)
    type_counts[ct] = type_counts.get(ct, 0) + 1
    _spark_versions[j.job_id] = sv

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

old_runtime = sum(1 for sv in _spark_versions.values() if _is_old_runtime(sv))
_ALLOWED_CLUSTER_TYPES = {"Job Cluster", "Serverless"}
job_cluster_count = sum(v for ct, v in type_counts.items() if ct not in _ALLOWED_CLUSTER_TYPES)
def _is_paused_schedule(j) -> bool:
    sched = getattr(j.settings, "schedule", None) if j.settings else None
    if not sched:
        return False
    ps = getattr(sched, "pause_status", None)
    ps_str = (ps.value if hasattr(ps, "value") else str(ps)) if ps is not None else ""
    return ps_str.split(".")[-1].upper() == "PAUSED"

scheduled = sum(
    1 for j in jobs
    if getattr(j.settings, "schedule", None) is not None
    and not _is_paused_schedule(j)
)
has_threshold = sum(
    1 for j in jobs
    if _job_checks[j.job_id]["is_scheduled"] and extract_threshold_tooltip(j) is not None
)
has_notifications = sum(1 for j in jobs if _job_checks[j.job_id]["notif"] == "ok")
has_access        = sum(1 for j in jobs if _job_checks[j.job_id]["access"] == "ok")
has_notebooks_path = sum(1 for j in jobs if _job_checks[j.job_id]["path"] == "ok")
no_run_as         = sum(1 for j in jobs if _job_checks[j.job_id]["run_as"] == "fail")
no_team           = sum(1 for j in jobs if len(_job_checks[j.job_id]["teams"]) != 1)

def pct(n: int) -> str:
    return f"{n}" if total else str(n)

# Stats aligned with table columns: Job Name | Cluster Type | Cluster Size | Runtime | Schedule | Threshold | Notif. | Access
stat_cols = st.columns(COL_WIDTHS)
def _stat(col, label: str, value, color: str = "inherit") -> None:
    col.markdown(
        f"<div style='text-align:center;font-size:0.8em;color:rgba(250,250,250,0.6);margin-bottom:2px'>{label}</div>"
        f"<div style='text-align:center;font-size:1.6em;font-weight:600;color:{color}'>{value}</div>",
        unsafe_allow_html=True,
    )

_jc_color = "#ff4b4b" if job_cluster_count > 0 else "inherit"
_stat(stat_cols[0], "Total Jobs", total)
_no_team_color = "#ff8c00" if no_team > 0 else "inherit"
_stat(stat_cols[1], "No/Multi Team", no_team, _no_team_color)
stat_cols[2].markdown(
    f"<div style='text-align:center;font-size:0.8em;color:rgba(250,250,250,0.6);margin-bottom:2px'>Wrong Cluster</div>"
    f"<div style='text-align:center;font-size:1.6em;font-weight:600;color:{_jc_color}'>{pct(job_cluster_count)}</div>",
    unsafe_allow_html=True,
)
_run_as_stat_color = "#ff8c00" if no_run_as > 0 else "inherit"
stat_cols[3].markdown(
    f"<div style='text-align:center;font-size:0.8em;color:rgba(250,250,250,0.6);margin-bottom:2px'>Wrong Run As</div>"
    f"<div style='text-align:center;font-size:1.6em;font-weight:600;color:{_run_as_stat_color}'>{no_run_as}</div>",
    unsafe_allow_html=True,
)
_rt_color = "#ff8c00" if old_runtime > 0 else "inherit"
stat_cols[4].markdown(
    f"<div style='text-align:center;font-size:0.8em;color:rgba(250,250,250,0.6);margin-bottom:2px'>Old Runtime &lt;{_min_runtime_str}</div>"
    f"<div style='text-align:center;font-size:1.6em;font-weight:600;color:{_rt_color}'>{old_runtime}</div>",
    unsafe_allow_html=True,
)
no_scheduled = total - scheduled
_sched_color = "#ff8c00" if no_scheduled > 0 else "inherit"
stat_cols[5].markdown(
    f"<div style='text-align:center;font-size:0.8em;color:rgba(250,250,250,0.6);margin-bottom:2px'>Not Scheduled</div>"
    f"<div style='text-align:center;font-size:1.6em;font-weight:600;color:{_sched_color}'>{no_scheduled}</div>",
    unsafe_allow_html=True,
)

no_threshold      = sum(
    1 for j in jobs
    if _job_checks[j.job_id]["is_scheduled"] and extract_threshold_tooltip(j) is None
)
no_notifications  = sum(1 for j in jobs if _job_checks[j.job_id]["notif"]   == "fail" and len(_job_checks[j.job_id]["teams"]) <= 1)
no_access         = sum(1 for j in jobs if _job_checks[j.job_id]["access"]  == "fail" and len(_job_checks[j.job_id]["teams"]) <= 1)
no_notebooks_path = sum(1 for j in jobs if _job_checks[j.job_id]["path"]    == "fail" and len(_job_checks[j.job_id]["teams"]) <= 1)

for col, label, val in [
    (stat_cols[6], "Threshold", no_threshold),
    (stat_cols[7], "Notif.",    no_notifications),
    (stat_cols[8], "Access",    no_access),
    (stat_cols[9], "Path",      no_notebooks_path),
]:
    _stat(col, label, val, "#ff4b4b" if val > 0 else "inherit")

st.divider()

# ── sort ───────────────────────────────────────────────────────────────────────

if "jobs_sort_col" not in st.session_state:
    st.session_state.jobs_sort_col = st.query_params.get("sort_col") or None
    st.session_state.jobs_sort_dir = int(st.query_params.get("sort_dir", "1"))

def _sort_key(job):
    col = st.session_state.jobs_sort_col
    ct, _, sv = extract_cluster_info(job, _cluster_cache)
    if col == "Job Name":
        return ((job.settings.name or f"job-{job.job_id}") if job.settings else f"job-{job.job_id}").lower()
    if col == "Team":
        _n = (job.settings.name or f"job-{job.job_id}") if job.settings else f"job-{job.job_id}"
        _c = _run_as_map.get(job.job_id) or getattr(job, "creator_user_name", None) or "unknown"
        return ", ".join(match_team_rules(_n, _c, _teams_cfg, tags=job.settings.tags if job.settings else {}))
    if col == "Cluster Type":   return ct
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
    if col == "Schedule":
        label, *_ = extract_schedule_info(job)
        return label
    if col == "Threshold":  return 0 if extract_threshold_tooltip(job) else 1
    if col == "Notif.":     return 0 if extract_notification_tooltip(job) else 1
    if col == "Access":     return 0 if extract_access_tooltip(job) else 1
    if col == "Path":       return 0 if extract_notebooks_path_tooltip(job) else 1
    return ""

if st.session_state.jobs_sort_col:
    jobs = sorted(jobs, key=_sort_key, reverse=(st.session_state.jobs_sort_dir == -1))

# ── table ──────────────────────────────────────────────────────────────────────

CENTERED_HEADERS = {"Runtime", "Schedule", "Threshold", "Notif.", "Access"}
header_cols = st.columns(COL_WIDTHS)
for hcol, h in zip(header_cols, COL_HEADERS):
    is_active = st.session_state.jobs_sort_col == h
    arrow = (" ▲" if st.session_state.jobs_sort_dir == 1 else " ▼") if is_active else " ⇅"
    if hcol.button(f"{h}{arrow}", key=f"sort_{h}", use_container_width=True):
        if st.session_state.jobs_sort_col == h:
            st.session_state.jobs_sort_dir *= -1
        else:
            st.session_state.jobs_sort_col = h
            st.session_state.jobs_sort_dir = 1
        st.query_params["sort_col"] = st.session_state.jobs_sort_col
        st.query_params["sort_dir"] = str(st.session_state.jobs_sort_dir)
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
    sched_label, cron_str, sched_tz = extract_schedule_info(job)

    _checks = _job_checks[job.job_id]
    _matched_teams = _checks["teams"]
    if not _matched_teams:
        _team_display = "<span style='color:orange;font-size:0.8em'>no team</span>"
    elif len(_matched_teams) > 1:
        _team_display = f"<span style='color:orange'>{', '.join(_matched_teams)}</span>"
    else:
        _team_display = _matched_teams[0]

    thresh_html   = extract_threshold_tooltip(job)
    notif_html    = extract_notification_tooltip(job)
    access_html   = extract_access_tooltip(job)
    nb_path_html  = extract_notebooks_path_tooltip(job)

    if _checks["is_scheduled"]:
        thresh_cell = make_tooltip("✓" if thresh_html else "❗", thresh_html)
    else:
        thresh_cell = make_tooltip("<span style='color:grey;font-size:0.8em'>—</span>", None)

    def _cell(status, html):
        if status == "no_team":
            return make_tooltip("<span style='color:orange;font-size:0.8em''>no team</span>", html)
        return make_tooltip("✓" if status == "ok" else "❗", html)

    _multi_team_cell = "<span style='color:orange;font-size:0.8em'>more 1 team</span>"
    if len(_matched_teams) > 1:
        notif_cell   = _multi_team_cell
        access_cell  = _multi_team_cell
        nb_path_cell = _multi_team_cell
    else:
        notif_cell   = _cell(_checks["notif"],  notif_html)
        access_cell  = _cell(_checks["access"], access_html)
        nb_path_cell = _cell(_checks["path"],   nb_path_html)

    _is_paused = "paused" in sched_label.lower()
    if cron_str:
        _label_html = f"<span style='color:#ff8c00'>{sched_label}</span>" if _is_paused else sched_label
        _cron_human = cron_to_human(cron_str)
        _tz_short = sched_tz.split("/")[-1].replace("_", " ") if sched_tz else ""
        _tz_html = f" <span style='color:gray;font-size:0.78em'>({_tz_short})</span>" if _tz_short else ""
        sched_display = (
            f"{_label_html}<br>"
            f"<span style='color:gray;font-size:0.82em'>{_cron_human}{_tz_html}</span>"
        )
    elif sched_label == "Not scheduled":
        sched_display = "<span style='color:red'>Not scheduled</span>"
    else:
        sched_display = sched_label

    row = st.columns(COL_WIDTHS)
    row[0].markdown(
        f"{name}<br><span style='color:gray;font-size:0.75em'>ID: {job_id}</span>",
        unsafe_allow_html=True,
    )
    row[1].markdown(f"<div style='text-align:center'>{_team_display}</div>", unsafe_allow_html=True)
    if cluster_type == "All-purpose":
        row[2].markdown("<div style='text-align:center'><span class='red-cell'>All-purpose</span></div>", unsafe_allow_html=True)
    else:
        row[2].markdown(f"<div style='text-align:center'>{cluster_type}</div>", unsafe_allow_html=True)
    _run_as_color = "#ff8c00" if _checks["run_as"] == "fail" else "inherit"
    row[3].html(f"<span style='color:{_run_as_color}'>{run_as}</span>")
    _sv_color = "#ff8c00" if _is_old_runtime(spark_ver) else "inherit"
    row[4].markdown(f"<div style='text-align:center;color:{_sv_color}'>{spark_ver}</div>", unsafe_allow_html=True)
    row[5].markdown(f"<div style='text-align:center'>{sched_display}</div>", unsafe_allow_html=True)
    row[6].markdown(f"<div style='text-align:center'>{thresh_cell}</div>",  unsafe_allow_html=True)
    row[7].markdown(f"<div style='text-align:center'>{notif_cell}</div>",   unsafe_allow_html=True)
    row[8].markdown(f"<div style='text-align:center'>{access_cell}</div>",   unsafe_allow_html=True)
    row[9].markdown(f"<div style='text-align:center'>{nb_path_cell}</div>", unsafe_allow_html=True)
    st.markdown("<hr style='margin:4px 0;border:none;border-top:1px solid rgba(128,128,128,0.15);'>", unsafe_allow_html=True)
