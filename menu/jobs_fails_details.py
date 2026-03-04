import datetime as dt

import altair as alt
import pandas as pd
import pytz
import streamlit as st
from databricks.sdk import WorkspaceClient

st.header("Job Fails Details")

col_teams, _ = st.columns([0.5, 0.5])

col_teams.multiselect("Teams", options=[], default=[], disabled=True, help="Coming soon")

w = WorkspaceClient(profile="DEFAULT")
