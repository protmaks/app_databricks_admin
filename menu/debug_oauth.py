"""OAuth debug page — проверяет токен пользователя без вызова clusters/jobs API."""
import base64
import json
import os

import requests
import streamlit as st

st.header("OAuth Debug")

host = os.getenv("DATABRICKS_HOST", "")
user_token = st.context.headers.get("x-forwarded-access-token", "")
user_email = st.context.headers.get("x-forwarded-email", "")

# ── 1. Базовая информация ──────────────────────────────────────────────────────
st.subheader("1. Заголовки")
col1, col2 = st.columns(2)
col1.metric("x-forwarded-access-token", "✅ присутствует" if user_token else "❌ отсутствует")
col2.metric("x-forwarded-email", user_email or "—")
col1.metric("DATABRICKS_HOST", "✅ задан" if host else "❌ не задан")
col2.metric("DATABRICKS_CLIENT_ID", "✅ задан" if os.getenv("DATABRICKS_CLIENT_ID") else "❌ не задан")

# ── 2. Декодирование JWT (без верификации) ─────────────────────────────────────
st.subheader("2. Содержимое токена (JWT decode)")
if user_token:
    try:
        parts = user_token.split(".")
        if len(parts) >= 2:
            payload_b64 = parts[1]
            # Добавляем padding если нужно
            payload_b64 += "=" * (4 - len(payload_b64) % 4)
            payload = json.loads(base64.urlsafe_b64decode(payload_b64))

            scopes = payload.get("scp", payload.get("scope", "—"))
            st.write("**Скоупы (scp/scope):**", scopes)
            st.write("**sub (user):**", payload.get("sub", "—"))
            st.write("**exp:**", payload.get("exp", "—"))

            with st.expander("Полный payload токена"):
                st.json(payload)
        else:
            st.warning("Токен не является JWT")
    except Exception as e:
        st.error(f"Не удалось декодировать токен: {e}")
else:
    st.warning("Токен отсутствует — приложение работает под SP или oauth.scopes не применились.")

# ── 3. Вызов /api/2.0/preview/scim/v2/Me (всегда доступен) ───────────────────
st.subheader("3. Тест: /api/2.0/preview/scim/v2/Me (iam.current-user:read)")
st.caption("Этот endpoint всегда доступен с дефолтными скоупами — проверяет, что токен вообще работает.")

if st.button("Проверить /Me endpoint") and host and user_token:
    try:
        resp = requests.get(
            f"{host}/api/2.0/preview/scim/v2/Me",
            headers={"Authorization": f"Bearer {user_token}"},
            timeout=10,
        )
        st.write(f"**Статус:** {resp.status_code}")
        if resp.ok:
            data = resp.json()
            st.success(f"Пользователь: {data.get('displayName', '')} ({data.get('userName', '')})")
            with st.expander("Полный ответ"):
                st.json(data)
        else:
            st.error(resp.text)
    except Exception as e:
        st.error(str(e))
elif not host:
    st.info("DATABRICKS_HOST не задан")
elif not user_token:
    st.info("Токен отсутствует")

# ── 4. Все заголовки ──────────────────────────────────────────────────────────
with st.expander("Все HTTP-заголовки запроса"):
    st.json(dict(st.context.headers))
