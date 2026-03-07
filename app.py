from dotenv import load_dotenv

load_dotenv()
import streamlit as st
from pathlib import Path

st.set_page_config(
    page_title="Databricks Cost and Optimization",
    layout="wide",
)

logo_path = Path(__file__).parent / "assets" / "logo.png"
logo_sm_path = Path(__file__).parent / "assets" / "logo_sm.png"
st.logo(
    str(logo_path),
    icon_image=str(logo_sm_path),
)

menu = {
    "Help": [
        st.Page("menu/description.py", title="APP Description", icon=":material/description:",),
    ],
    "Compute": [
        st.Page("menu/compute/compute.py", title="Active Compute", icon=":material/description:",),

    ],
    "Compute Monitoring": [
        st.Page("menu/monitoring/monitoring_allpurp_timeline.py", title="All-Purpose Daily Runs", icon=":material/timeline:",),
        st.Page("menu/monitoring/jobs_in_allpurp_cluster.py", title="Jobs in All-purp Cluster", icon=":material/memory:",),
    ],
    "Jobs": [
        st.Page("menu/jobs_and_pipelines/jobs_settings.py", title="Job Settings", icon=":material/check_circle:",),
        st.Page("menu/jobs_and_pipelines/jobs_run_daily.py", title="Jobs Runs (Daily)", icon=":material/grid_view:"),
        st.Page("menu/jobs_and_pipelines/jobs_timeline_hourly.py", title="Jobs Timeline (Hourly)", icon=":material/schedule:"),
        st.Page("menu/jobs_and_pipelines/jobs_fails_details.py", title="Job Fails Details", icon=":material/bug_report:"),
    ],
    "Admin": [
        st.Page("menu/settings/settings_page.py", title="Settings", icon=":material/settings:"),
    ],
}

st.markdown(
    """<style>
    hr { margin-top: 0.25rem !important; margin-bottom: 0.25rem !important; }
    [data-testid="stMarkdownContainer"] p { margin: 0 !important; }
    </style>""",
    unsafe_allow_html=True,
)

pg = st.navigation(menu)
pg.run()
