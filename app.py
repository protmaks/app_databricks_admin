import os
from dotenv import load_dotenv

load_dotenv()
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
from pathlib import Path
import pandas as pd
import altair as alt
import datetime as dt

st.set_page_config(
    page_title="Databricks Cost and Optimization",
    layout="wide",
)

menu = {
    "Help": [
        st.Page(
            "menu/description.py",
            title="APP Description",
            icon=":material/description:",
        ),
    ],
    "Clusters": [
        st.Page(
            "menu/cluster_1.py",
            title="All-Purpose Clusters",
            icon=":material/desktop_windows:",
        ),
        st.Page("menu/cluster_2.py", title="SQL Warehouses", icon=":material/cloud:"),
        st.Page(
            "menu/cluster_timeline.py",
            title="All-Purpose Daily Runs",
            icon=":material/timeline:",
        ),
    ],
    "Jobs": [
        st.Page(
            "menu/jobs_timeline.py", title="Jobs Timeline", icon=":material/timeline:"
        ),
        st.Page(
            "menu/jobs_last_run.py",
            title="Jobs Daily Runs",
            icon=":material/check_circle:",
        ),
    ],
}

pg = st.navigation(menu)
pg.run()
