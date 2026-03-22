import yagmail
import os
import streamlit as st

user=st.secrets["ETL_EMAIL_USER"]
password=st.secrets["ETL_EMAIL_PASSWORD"]
def send_email_alert(df):
    failed=df[df["status"]=="FAIL"]
    if failed.empty:
        return
    yag=yagmail.SMTP(user=user,password=password)

    subject="ETL test Failed"
    body=f"""Failed_test_counts:{len(failed)}
    Details:
    {failed.to_string()}
"""
    yag.send(to="schetankumar1993@gmail.com",subject=subject,contents=body)

