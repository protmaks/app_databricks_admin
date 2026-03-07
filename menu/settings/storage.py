"""Settings storage: DBFS on Databricks, local JSON file for local dev."""
import base64
import json
from pathlib import Path

import streamlit as st
from databricks.sdk import WorkspaceClient

SETTINGS_PATH = "/FileStore/databricks_admin_app/settings.json"

# Local fallback — sits in the project root, git-ignored
_LOCAL_PATH = Path(__file__).parent.parent.parent / "settings_local.json"

DEFAULT_SETTINGS: dict = {
    "version": 1,
    "timezone": "UTC",
    "min_runtime_version": "16.4",
    "teams": [],
    "default_teams": [],
}


def _migrate(data: dict) -> dict:
    """Backfill new fields added to settings without a version bump."""
    data.setdefault("default_teams", [])
    data.setdefault("min_runtime_version", "16.4")
    return data


def _load_local() -> dict:
    try:
        data = json.loads(_LOCAL_PATH.read_text(encoding="utf-8"))
        return _migrate(data) if data.get("version") == 1 else DEFAULT_SETTINGS.copy()
    except Exception:
        return DEFAULT_SETTINGS.copy()


def _save_local(settings: dict) -> None:
    _LOCAL_PATH.write_text(json.dumps(settings, indent=2, ensure_ascii=False), encoding="utf-8")


def load_settings(w: WorkspaceClient) -> dict:
    """Load settings from DBFS, falling back to local file on error."""
    try:
        resp = w.dbfs.read(path=SETTINGS_PATH)
        data = json.loads(base64.b64decode(resp.data).decode("utf-8"))
        return _migrate(data) if data.get("version") == 1 else DEFAULT_SETTINGS.copy()
    except Exception:
        return _load_local()


def save_settings(w: WorkspaceClient, settings: dict) -> None:
    """Write settings JSON to DBFS (base64-encoded string as required by SDK v0.96).

    Falls back to a local JSON file when DBFS is unavailable (local dev).
    Raises RuntimeError if both fail.
    """
    raw = json.dumps(settings, indent=2, ensure_ascii=False).encode("utf-8")
    encoded = base64.b64encode(raw).decode("ascii")
    try:
        w.dbfs.put(path=SETTINGS_PATH, overwrite=True, contents=encoded)
        return
    except Exception as dbfs_exc:
        pass  # try local fallback

    try:
        _save_local(settings)
    except Exception as local_exc:
        raise RuntimeError(
            f"Failed to save settings. DBFS: {dbfs_exc}. Local: {local_exc}"
        ) from local_exc


def get_cached_settings(w: WorkspaceClient) -> dict:
    """Return settings from session_state cache, loading from storage on first call.

    Invalidated by the Settings page when the user saves new settings.
    """
    if "global_settings" not in st.session_state:
        st.session_state["global_settings"] = load_settings(w)
    return st.session_state["global_settings"]
