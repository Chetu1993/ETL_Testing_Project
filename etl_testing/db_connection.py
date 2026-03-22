
import pandas as pd
from sqlalchemy import create_engine
engine=create_engine("mysql+pymysql://root:root@localhost:3306/mydb")
def get_query(query):
    if not query:
        raise ValueError("Query is None")


    return pd.read_sql(query,engine)
