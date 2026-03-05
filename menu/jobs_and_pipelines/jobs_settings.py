import streamlit as st
from databricks.sdk import WorkspaceClient

from menu.compute.utils import quartz_to_standard_cron, make_workspace_client

st.header("Jobs Settings")

w = make_workspace_client()

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
</style>""", unsafe_allow_html=True)


# ── helpers ────────────────────────────────────────────────────────────────────

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


def extract_cluster_info(job) -> tuple[str, str, str]:
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
            return "Existing Cluster", "—", "—"

    return "Serverless", "—", "—"


def extract_schedule_info(job) -> tuple[str, str]:
    sched = getattr(job.settings, "schedule", None) if job.settings else None
    if sched is None:
        return "Not scheduled", ""
    quartz = getattr(sched, "quartz_cron_expression", None) or ""
    pause = getattr(sched, "pause_status", None)
    cron5 = quartz_to_standard_cron(quartz) if quartz else ""
    paused = pause is not None and str(pause).upper() == "PAUSED"
    label = "Scheduled (paused)" if paused else "Scheduled"
    return label, cron5 or quartz


def extract_threshold_tooltip(job) -> str | None:
    health = getattr(job.settings, "health", None) if job.settings else None
    rules = getattr(health, "rules", None) if health else None
    if not rules:
        return None
    lines = ["<b>Health Rules:</b>"]
    for r in rules:
        metric = getattr(r, "metric", "?")
        op = getattr(r, "op", "?")
        value = getattr(r, "value", "?")
        m_str = metric.value if hasattr(metric, "value") else str(metric)
        o_str = op.value if hasattr(op, "value") else str(op)
        lines.append(f"{m_str} {o_str} {value}")
    return "<br>".join(lines)


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


# ── data fetch ─────────────────────────────────────────────────────────────────

with st.spinner("Fetching jobs…"):
    try:
        jobs = list(w.jobs.list(expand_tasks=True))
    except Exception as e:
        st.error(f"Failed to fetch jobs: {e}")
        st.stop()

if not jobs:
    st.info("No jobs found.")
    st.stop()


# ── filters ────────────────────────────────────────────────────────────────────

all_creators = sorted({
    getattr(j, "creator_user_name", None) or "unknown"
    for j in jobs
})

col_creator, col_teams = st.columns([0.5, 0.5])
selected_creators = col_creator.multiselect(
    "Created by",
    options=all_creators,
    default=[],
    placeholder="All creators",
    key="jobs_settings_creators",
)
col_teams.multiselect("Teams", options=[], default=[], disabled=True, help="Coming soon")

if selected_creators:
    jobs = [
        j for j in jobs
        if (getattr(j, "creator_user_name", None) or "unknown") in selected_creators
    ]

if not jobs:
    st.info("No jobs match the selected filters.")
    st.stop()


# ── statistics ─────────────────────────────────────────────────────────────────

total = len(jobs)

scheduled = sum(
    1 for j in jobs
    if getattr(j.settings, "schedule", None) is not None
    and str(getattr(j.settings.schedule, "pause_status", "")).upper() != "PAUSED"
)
has_threshold = sum(
    1 for j in jobs
    if getattr(j.settings, "health", None)
    and getattr(j.settings.health, "rules", None)
)
has_notifications = sum(
    1 for j in jobs
    if (getattr(j.settings, "email_notifications", None)
        or getattr(j.settings, "webhook_notifications", None))
)
has_access = sum(
    1 for j in jobs
    if getattr(j.settings, "access_control_list", None)
)

type_counts: dict[str, int] = {}
for j in jobs:
    ct, _, _ = extract_cluster_info(j)
    type_counts[ct] = type_counts.get(ct, 0) + 1

def pct(n: int) -> str:
    return f"{n} ({n * 100 // total}%)" if total else str(n)

cluster_type_order = ["Job Cluster", "New Cluster", "SQL Warehouse"]
present_types = [ct for ct in cluster_type_order if ct in type_counts]

all_cols = st.columns(4 + len(present_types))
all_cols[0].metric("Total Jobs",      total)
all_cols[1].metric("Scheduled",       pct(scheduled))
all_cols[2].metric("With Threshold",  pct(has_threshold))
all_cols[3].metric("With Notif.",     pct(has_notifications))
for i, ct in enumerate(present_types):
    all_cols[4 + i].metric(ct, pct(type_counts[ct]))

st.divider()

# ── table ──────────────────────────────────────────────────────────────────────

COL_WIDTHS  = [2.0, 0.9, 1.2, 0.8, 1.6, 0.5, 0.5, 0.5]
COL_HEADERS = ["Job Name", "Cluster Type", "Cluster Size", "Runtime", "Schedule", "Threshold", "Notif.", "Access"]

header_cols = st.columns(COL_WIDTHS)
for col, h in zip(header_cols, COL_HEADERS):
    col.markdown(f"**{h}**")

st.divider()

for job in jobs:
    name = (
        (job.settings.name if job.settings and job.settings.name else None)
        or f"job-{job.job_id}"
    )
    job_id = job.job_id

    cluster_type, cluster_size, spark_ver = extract_cluster_info(job)
    sched_label, cron_str               = extract_schedule_info(job)

    thresh_html  = extract_threshold_tooltip(job)
    notif_html   = extract_notification_tooltip(job)
    access_html  = extract_access_tooltip(job)

    thresh_cell  = make_tooltip("✅" if thresh_html  else "❌", thresh_html)
    notif_cell   = make_tooltip("✅" if notif_html   else "❌", notif_html)
    access_cell  = make_tooltip("✅" if access_html  else "❌", access_html)

    if cron_str:
        sched_display = (
            f"{sched_label}<br>"
            f"<span style='color:gray;font-size:0.82em'>{cron_str}</span>"
        )
    else:
        sched_display = sched_label

    row = st.columns(COL_WIDTHS)
    row[0].markdown(
        f"{name}<br><span style='color:gray;font-size:0.75em'>ID: {job_id}</span>",
        unsafe_allow_html=True,
    )
    if cluster_type == "Existing Cluster":
        row[1].markdown("<span class='red-cell'>Existing Cluster</span>", unsafe_allow_html=True)
    else:
        row[1].write(cluster_type)
    row[2].write(cluster_size)
    row[3].write(spark_ver)
    row[4].markdown(sched_display, unsafe_allow_html=True)
    row[5].markdown(thresh_cell,   unsafe_allow_html=True)
    row[6].markdown(notif_cell,    unsafe_allow_html=True)
    row[7].markdown(access_cell,   unsafe_allow_html=True)
    st.markdown("<hr style='margin:4px 0;border:none;border-top:1px solid rgba(128,128,128,0.15);'>", unsafe_allow_html=True)
