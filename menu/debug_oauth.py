"""OAuth debug page — checks user token without calling clusters/jobs API."""
import base64
import json
import os

import requests
import streamlit as st

st.header("OAuth Debug")

host = os.getenv("DATABRICKS_HOST", "").rstrip("/")
if host and not host.startswith("https://"):
    host = "https://" + host
user_token = st.context.headers.get("x-forwarded-access-token", "")
user_email = st.context.headers.get("x-forwarded-email", "")

# ── 1. Headers ────────────────────────────────────────────────────────────────
st.subheader("1. Headers")
col1, col2 = st.columns(2)
col1.metric("x-forwarded-access-token", "present" if user_token else "missing")
col2.metric("x-forwarded-email", user_email or "—")
col1.metric("DATABRICKS_HOST", "set" if host else "not set")
col2.metric("DATABRICKS_CLIENT_ID", "set" if os.getenv("DATABRICKS_CLIENT_ID") else "not set")

# ── 2. JWT decode (no verification) ──────────────────────────────────────────
st.subheader("2. Token contents (JWT decode)")
if user_token:
    try:
        parts = user_token.split(".")
        if len(parts) >= 2:
            payload_b64 = parts[1]
            payload_b64 += "=" * (4 - len(payload_b64) % 4)
            payload = json.loads(base64.urlsafe_b64decode(payload_b64))

            scopes = payload.get("scp", payload.get("scope", "—"))
            st.write("**Scopes (scp/scope):**", scopes)
            st.write("**sub (user):**", payload.get("sub", "—"))
            st.write("**exp:**", payload.get("exp", "—"))

            with st.expander("Full token payload"):
                st.json(payload)
        else:
            st.warning("Token is not a JWT")
    except Exception as e:
        st.error(f"Failed to decode token: {e}")
else:
    st.warning("Token missing — app is running under SP identity or oauth.scopes not applied yet.")

# ── 3. Call /api/2.0/preview/scim/v2/Me (always available) ───────────────────
st.subheader("3. Test: /api/2.0/preview/scim/v2/Me (iam.current-user:read)")
st.caption("This endpoint is always accessible with default scopes — verifies the token works at all.")

if st.button("Check /Me endpoint") and host and user_token:
    try:
        resp = requests.get(
            f"{host}/api/2.0/preview/scim/v2/Me",
            headers={"Authorization": f"Bearer {user_token}"},
            timeout=10,
        )
        st.write(f"**Status:** {resp.status_code}")
        if resp.ok:
            data = resp.json()
            st.success(f"User: {data.get('displayName', '')} ({data.get('userName', '')})")
            with st.expander("Full response"):
                st.json(data)
        else:
            st.error(resp.text)
    except Exception as e:
        st.error(str(e))
elif not host:
    st.info("DATABRICKS_HOST is not set")
elif not user_token:
    st.info("Token is missing")

# ── 4. All HTTP headers ───────────────────────────────────────────────────────
with st.expander("All HTTP request headers"):
    st.json(dict(st.context.headers))
