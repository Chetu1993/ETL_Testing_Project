import yagmail
from dotenv import load_dotenv
import os

load_dotenv()
user=os.getenv("ETL_EMAIL_USER")
password=os.getenv("ETL_EMAIL_PASSWORD")
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

