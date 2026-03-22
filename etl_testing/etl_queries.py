
import pandas as pd
from sqlalchemy import create_engine
engine=create_engine("mysql+pymysql://root:root@localhost:3306/mydb")



query="select * from source_table;"
df_source=pd.read_sql(query,engine)
print(df_source)

df_target=pd.read_sql("select * from target_table",engine)
print(df_target)

if len(df_source)==len(df_target):
    print("row count matching")
else:
    print("row count not matching")

df_source['expected']=df_source["salary"]*1.2
merged=df_source.merge(df_target,on="id")
mismatch=merged[merged["expected"]!=merged["salary_bonus"]]

if mismatch.empty:
    print("data matched")
else:
    print("data mismatch found")
    print(mismatch)

if df_target.isnull().sum().sum()==0:
    print("no null values")
else:
    print("null values found")