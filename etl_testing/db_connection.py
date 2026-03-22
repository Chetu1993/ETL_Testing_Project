import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# DATABASE_URL=st.secrets["DATABASE_URL"]
database_url="mysql+pymysql://root:root@localhost:3306/mydb"
engine=create_engine(database_url)
def get_query(query):
    if not query:
        raise ValueError("Query is None")


    return pd.read_sql(query,engine)
