import duckdb
import streamlit as st

from src import config


@st.cache_resource
def get_conn():
    if not config.DB_PATH.exists():
        return None
    return duckdb.connect(str(config.DB_PATH), read_only=True)
