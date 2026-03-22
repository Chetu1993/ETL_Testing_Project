import yagmail
import os
import streamlit as st
import datetime

def send_email_alert(df):
    user = st.secrets["ETL_EMAIL_USER"]
    password = st.secrets["ETL_EMAIL_PASSWORD"]
    failed=df[df["status"]=="FAIL"]
    if failed.empty:
        return
    yag=yagmail.SMTP(user=user,password=password)

    subject="ETL test Failed"
    body=f"""Failed_test_counts:{len(failed)}
    Details:
    {failed.to_string()}
"""
    report_filename=f"Report_{datetime.datetime.now().strftime('%Y-%m-%d_%I-%M-%S')}.csv"
    df.to_csv(report_filename,index=False)
    yag.send(to="schetankumar1993@gmail.com",subject=subject,contents=body,attachments=report_filename)

