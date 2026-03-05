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
        st.Page("menu/compute_all.py", title="Compute All", icon=":material/description:",),
        st.Page("menu/clusters_allpurp.py", title="All-Purpose compute", icon=":material/desktop_windows:",        ),
        st.Page("menu/jobs_settings.py", title="Job compute and Settings", icon=":material/check_circle:",),
        st.Page("menu/clusters_sqlwh.py", title="SQL warehouses", icon=":material/cloud:"),
        st.Page("menu/cluster_apps.py", title="Apps", icon=":material/apps:"),
        st.Page("menu/cluster_lakebase.py", title="Lakebase", icon=":material/apps:"), 
    ],
    "Compute Monitoring": [
        st.Page("menu/cluster_allpurp_timeline.py", title="All-Purpose Daily Runs", icon=":material/timeline:",),
        st.Page("menu/jobs_in_allpurp_cluster.py", title="Jobs in All-purp Cluster", icon=":material/memory:",),
    ],
    "Jobs": [
        st.Page("menu/jobs_runs_daily.py", title="Jobs Runs (Daily) v1", icon=":material/check_circle:",),
        st.Page("menu/jobs_run_daily.py", title="Jobs Runs (Daily)", icon=":material/grid_view:"),
        st.Page("menu/jobs_timeline_hourly.py", title="Jobs Timeline (Hourly)", icon=":material/schedule:"),
        st.Page("menu/jobs_fails_details.py", title="Job Fails Details", icon=":material/bug_report:"),
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
