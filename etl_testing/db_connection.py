import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

DATABASE_URL=st.secrets["DATABASE_URL"]
engine=create_engine(DATABASE_URL)
def get_query(query):
    if not query:
        raise ValueError("Query is None")


    return pd.read_sql(query,engine)
