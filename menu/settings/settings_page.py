"""Global Settings page — timezone and team rule configuration."""
import uuid

import streamlit as st

from menu.compute.utils import COMMON_TZ, make_workspace_client
from menu.settings.storage import get_cached_settings, save_settings

st.header("Settings")

w = make_workspace_client()

# ── Initialize widget keys from cache (re-runs on navigation back to this page)
# Widget-associated keys (settings_tz, settings_teams) are cleared by Streamlit
# when navigating away, so we reinitialize from the persistent global_settings cache.
_loaded = get_cached_settings(w)
if "settings_tz" not in st.session_state:
    st.session_state["settings_tz"] = _loaded["timezone"]
if "settings_teams" not in st.session_state:
    st.session_state["settings_teams"] = [{**t} for t in _loaded["teams"]]
if "settings_default_teams" not in st.session_state:
    st.session_state["settings_default_teams"] = list(_loaded.get("default_teams", []))
if "settings_min_runtime" not in st.session_state:
    st.session_state["settings_min_runtime"] = _loaded.get("min_runtime_version", "16.4")

# ── Section 1: Timezone ──────────────────────────────────────────────────────
tz_index = (
    COMMON_TZ.index(st.session_state["settings_tz"])
    if st.session_state["settings_tz"] in COMMON_TZ
    else 0
)
_tz_label, _tz_col, _tz_desc = st.columns([0.05, 0.1, 0.85])
_tz_label.markdown(
    "<div style='padding-top:8px'>Timezone:</div>",
    unsafe_allow_html=True,
)
_tz_col.selectbox(
    "Timezone",
    options=COMMON_TZ,
    index=tz_index,
    key="settings_tz",
    label_visibility="collapsed",
)
_tz_desc.caption("Applied as the default timezone on all pages. Can be overridden per page.")

_rt_label, _rt_col, _rt_desc = st.columns([0.09, 0.05, 0.86])
_rt_label.markdown(
    "<div style='padding-top:8px'>Minimal Runtime:</div>",
    unsafe_allow_html=True,
)
_rt_col.text_input(
    "Minimal Runtime",
    key="settings_min_runtime",
    label_visibility="collapsed",
)
_rt_desc.caption("Minimum Databricks Runtime version required for jobs.")

# ── Section 2: Teams ─────────────────────────────────────────────────────────
st.subheader("Teams")

FIELD_OPTIONS = ["job_name", "creator"]
FIELD_LABELS = {"job_name": "Job Name", "creator": "Creator"}
OP_OPTIONS = ["starts_with", "ends_with", "contains", "equals"]
OP_LABELS = {
    "starts_with": "starts with",
    "ends_with": "ends with",
    "contains": "contains",
    "equals": "equals",
}
LOGIC_OPTIONS = ["AND", "OR"]

_col_btn, _col_hint = st.columns([0.08, 0.92])
_col_hint.caption(
    "Define teams by combining job-name and creator conditions. "
    "The Teams filter on every page will show these team names."
)
if _col_btn.button("＋ Add Team", key="add_team_btn"):
    st.session_state["settings_teams"].append(
        {
            "id": str(uuid.uuid4()),
            "name": "",
            "conditions": [],
            "notification": "",
            "access": "",
            "notebooks_path": "",
            "run_as": "",
        }
    )
    st.rerun()

teams: list[dict] = st.session_state["settings_teams"]

for team_idx, team in enumerate(teams):
    team_id = team["id"]

    confirm_key = f"confirm_del_{team_id}"
    expanded_key = f"expanded_{team_id}"
    is_expanded = st.session_state.get(expanded_key, False) or st.session_state.get(confirm_key, False)

    _default_marker = "★ " if team_id in st.session_state["settings_default_teams"] else ""
    with st.expander(_default_marker + (team["name"] or f"Team {team_idx + 1}"), expanded=is_expanded):
        # Default checkbox + Team name + Delete button in one row
        col_default, col_name, col_del = st.columns([0.08, 0.83, 0.09])

        def _on_default_change(tid=team_id):
            if st.session_state[f"team_default_{tid}"]:
                if tid not in st.session_state["settings_default_teams"]:
                    st.session_state["settings_default_teams"].append(tid)
            else:
                st.session_state["settings_default_teams"] = [
                    x for x in st.session_state["settings_default_teams"] if x != tid
                ]

        col_default.checkbox(
            "Default",
            value=team_id in st.session_state["settings_default_teams"],
            key=f"team_default_{team_id}",
            on_change=_on_default_change,
            help="Pre-select this team on all filter pages",
        )

        def _on_name_change(tidx=team_idx, tid=team_id):
            teams[tidx]["name"] = st.session_state[f"team_name_{tid}"]

        col_name.text_input(
            "Team name",
            value=team["name"],
            key=f"team_name_{team_id}",
            on_change=_on_name_change,
            placeholder="e.g. Alpha Team",
        )

        # Delete with confirmation
        if st.session_state.get(confirm_key):
            c1, c2 = col_del.columns(2)
            if c1.button("✓", key=f"confirm_yes_{team_id}", type="primary", help="Yes, delete"):
                teams.pop(team_idx)
                st.session_state.pop(confirm_key, None)
                st.session_state.pop(expanded_key, None)
                st.rerun()
            if c2.button("✕", key=f"confirm_no_{team_id}", help="Cancel"):
                st.session_state.pop(confirm_key, None)
                st.rerun()
        else:
            col_del.markdown("<div style='padding-top:28px'></div>", unsafe_allow_html=True)
            if col_del.button("Delete", key=f"del_team_{team_id}"):
                st.session_state[confirm_key] = True
                st.rerun()

        # ── Conditions ───────────────────────────────────────────────────────
        conditions: list[dict] = team["conditions"]

        if conditions:
            st.markdown("**Conditions**")

        for cond_idx, cond in enumerate(conditions):
            cond_key = f"{team_id}_{cond_idx}"
            ccol_logic, ccol_field, ccol_op, ccol_val, ccol_del = st.columns(
                [0.07, 0.15, 0.1, 0.61, 0.07]
            )

            # Logic column: "IF" label for first row, AND/OR selector for the rest
            if cond_idx == 0:
                ccol_logic.markdown(
                    "<div style='padding-top:28px;font-weight:600;color:rgba(250,250,250,0.5)'>IF</div>",
                    unsafe_allow_html=True,
                )
            else:
                def _on_logic_change(tidx=team_idx, cidx=cond_idx, ck=cond_key):
                    teams[tidx]["conditions"][cidx]["logic"] = st.session_state[
                        f"cond_logic_{ck}"
                    ]

                _cur_logic = cond.get("logic", "AND")
                ccol_logic.selectbox(
                    "Logic",
                    options=LOGIC_OPTIONS,
                    index=LOGIC_OPTIONS.index(_cur_logic) if _cur_logic in LOGIC_OPTIONS else 0,
                    key=f"cond_logic_{cond_key}",
                    label_visibility="collapsed",
                    on_change=_on_logic_change,
                )

            def _on_field_change(tidx=team_idx, cidx=cond_idx, ck=cond_key):
                teams[tidx]["conditions"][cidx]["field"] = st.session_state[f"cond_field_{ck}"]

            def _on_op_change(tidx=team_idx, cidx=cond_idx, ck=cond_key):
                teams[tidx]["conditions"][cidx]["operator"] = st.session_state[f"cond_op_{ck}"]

            def _on_val_change(tidx=team_idx, cidx=cond_idx, ck=cond_key):
                teams[tidx]["conditions"][cidx]["value"] = st.session_state[f"cond_val_{ck}"].strip()

            ccol_field.selectbox(
                "Field",
                options=FIELD_OPTIONS,
                format_func=lambda x: FIELD_LABELS[x],
                index=FIELD_OPTIONS.index(cond.get("field", "job_name")),
                key=f"cond_field_{cond_key}",
                label_visibility="collapsed",
                on_change=_on_field_change,
            )
            ccol_op.selectbox(
                "Operator",
                options=OP_OPTIONS,
                format_func=lambda x: OP_LABELS[x],
                index=OP_OPTIONS.index(cond.get("operator", "starts_with")),
                key=f"cond_op_{cond_key}",
                label_visibility="collapsed",
                on_change=_on_op_change,
            )
            ccol_val.text_input(
                "Value",
                value=cond.get("value", ""),
                key=f"cond_val_{cond_key}",
                label_visibility="collapsed",
                placeholder="e.g. alpha_",
                on_change=_on_val_change,
            )
            if ccol_del.button("✕", key=f"del_cond_{cond_key}"):
                conditions.pop(cond_idx)
                st.session_state[expanded_key] = True
                st.rerun()

        if st.button("＋ Add Condition", key=f"add_cond_{team_id}"):
            new_cond = {"field": "job_name", "operator": "starts_with", "value": ""}
            if conditions:  # not the first condition — add default logic connector
                new_cond["logic"] = "AND"
            conditions.append(new_cond)
            st.session_state[expanded_key] = True
            st.rerun()

        # ── Jobs Settings ─────────────────────────────────────────────────────
        st.markdown("---")
        st.markdown("**Jobs Settings**")
        js_col1, js_col2, js_col3 = st.columns(3)

        def _on_notification_change(tidx=team_idx, tid=team_id):
            teams[tidx]["notification"] = st.session_state[f"team_notification_{tid}"]

        def _on_access_change(tidx=team_idx, tid=team_id):
            teams[tidx]["access"] = st.session_state[f"team_access_{tid}"]

        def _on_notebooks_path_change(tidx=team_idx, tid=team_id):
            teams[tidx]["notebooks_path"] = st.session_state[f"team_notebooks_path_{tid}"]

        def _on_run_as_change(tidx=team_idx, tid=team_id):
            teams[tidx]["run_as"] = st.session_state[f"team_run_as_{tid}"]

        js_col1.text_input(
            "Notification",
            value=team.get("notification", ""),
            key=f"team_notification_{team_id}",
            placeholder="e.g. team@example.com",
            on_change=_on_notification_change,
        )
        js_col2.text_input(
            "Access",
            value=team.get("access", ""),
            key=f"team_access_{team_id}",
            placeholder="e.g. user1, group1",
            on_change=_on_access_change,
        )
        js_col3.text_input(
            "Notebooks path",
            value=team.get("notebooks_path", ""),
            key=f"team_notebooks_path_{team_id}",
            placeholder="e.g. /Shared/team/notebooks",
            on_change=_on_notebooks_path_change,
        )
        st.text_input(
            "Run As accounts",
            value=team.get("run_as", ""),
            key=f"team_run_as_{team_id}",
            placeholder="e.g. user1@example.com, svc_principal_name",
            on_change=_on_run_as_change,
        )

# ── Save ─────────────────────────────────────────────────────────────────────
st.divider()

col_save, col_msg = st.columns([0.2, 0.8])

if col_save.button("Save Settings", type="primary", key="save_settings_btn"):
    settings_to_save = {
        "version": 1,
        "timezone": st.session_state["settings_tz"],
        "min_runtime_version": st.session_state["settings_min_runtime"],
        "teams": st.session_state["settings_teams"],
        "default_teams": st.session_state["settings_default_teams"],
    }

    errors: list[str] = []
    for t in settings_to_save["teams"]:
        if not t.get("name", "").strip():
            errors.append(f"A team has no name (id: {t['id'][:8]}…).")
        if not t.get("conditions"):
            name = t.get("name") or f"(id: {t['id'][:8]}…)"
            errors.append(f"Team '{name}' has no conditions.")
        for c in t.get("conditions", []):
            if not c.get("value", "").strip():
                name = t.get("name") or f"(id: {t['id'][:8]}…)"
                errors.append(f"Team '{name}': a condition has an empty value.")

    if errors:
        col_msg.error("Fix before saving:\n" + "\n".join(f"- {e}" for e in errors))
    else:
        valid_team_ids = {t["id"] for t in settings_to_save["teams"]}
        settings_to_save["default_teams"] = [
            tid for tid in settings_to_save["default_teams"] if tid in valid_team_ids
        ]
        try:
            save_settings(w, settings_to_save)
            # Invalidate the per-session cache so all other pages reload from DBFS
            st.session_state.pop("global_settings", None)
            col_msg.success("Settings saved.")
        except RuntimeError as exc:
            col_msg.error(str(exc))
