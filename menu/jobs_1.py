import os
import datetime as dt
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

APP_NAME = os.getenv("DATABRICKS_APP_NAME")