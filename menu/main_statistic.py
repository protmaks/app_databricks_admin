import datetime as dt

import altair as alt
import pandas as pd
import pytz
import streamlit as st
from databricks.sdk import WorkspaceClient

st.header("Job Fails Details")

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
selected_tz = col_tz.selectbox("Timezone", options=COMMON_TZ, index=0, key="fails_tz")
lookback_days = col_days.slider("Lookback days", min_value=1, max_value=60, value=30)
col_teams.multiselect("Teams", options=[], default=[], disabled=True, help="Coming soon")

tz = pytz.timezone(selected_tz)
now_local = dt.datetime.now(tz)
start_ms = int((now_local - dt.timedelta(days=lookback_days)).timestamp() * 1000)
end_ms = int(now_local.timestamp() * 1000)

w = WorkspaceClient(profile="DEFAULT")
