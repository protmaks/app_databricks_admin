import streamlit as st
from pathlib import Path

st.title("Description")

md = Path(__file__).resolve().parents[1] / "README.md"
st.markdown(md.read_text(encoding="utf-8"))