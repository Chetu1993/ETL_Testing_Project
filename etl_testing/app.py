import streamlit as st
import json
from etl_testing.db_validations import run_tests
import pandas as pd
import matplotlib.pyplot as plt
import datetime
from email_utils import send_email_alert


def highlight_status(row):
    if row["status"]=="PASS":
        return ["background-color: #d4edda"] * len(row)
    elif row["status"]=="FAIL":
        return ["background-color: #f8d7da"] * len(row)
    elif row["status"]=="ERROR":
        return ["background-color: #fff3cd"] * len(row)

    return [""]*len(row)



st.title("ETL Testing Framework")
st.write("upload your test config JSON and run ETL validations")

upload_file=st.file_uploader("Upload JSON config file",type=["json"])
config=None

if upload_file is not None:
    config=json.load(upload_file)
    st.success("JSON file loaded successfully")

if config:
    run_clicked=st.button("Run Tests",disabled=(config is None))
    if run_clicked:
        progress=st.progress(0)
        status_text=st.empty()
        with st.spinner("Running ETL tests..."):


            results=run_tests(config,progress_bar=progress,status_text=status_text)
            df=pd.DataFrame(results)
            df["run_time"]=datetime.datetime.now()


        st.session_state["results"]=df
        if df["status"].isin(["FAIL","ERROR"]).any():
            send_email_alert(df)
        if "history" not in st.session_state:
            st.session_state["history"]=[]
        st.session_state["history"].append(df.copy())

        status_text.text("All Tests Completed!")






if "history" in st.session_state:
    st.subheader("Run History")
    for i,run in enumerate(st.session_state["history"]):
        with st.expander(f"Run {i+1}-{run['run_time'].iloc[0]}"):
            st.dataframe(run)


if "results" in st.session_state:
    df=st.session_state["results"]

    col1,col2,col3,col4=st.columns(4)
    col1.metric("Total",len(df))
    col2.metric("Passed",(df["status"]=="PASS").sum())
    col3.metric("Failed",(df["status"]=="FAIL").sum())
    col4.metric("ERRORS",(df["status"]=="ERROR").sum())

    st.subheader("Test Results")

    status_filter = st.selectbox("Filter by status", ["ALL", "PASS", "FAIL", "ERROR"],key="status_filter")
    if status_filter == "ALL":
        filtered_df = df
    else:
        filtered_df = df[df["status"] == status_filter]

    # styled_df = filtered_df.style.apply(highlight_status, axis=1)
    st.dataframe(filtered_df.style.apply(highlight_status,axis=1),use_container_width=True)

    failed_tests=df[df["status"]=="FAIL"]
    if not failed_tests.empty:
        st.subheader("Inspect Failed Tests")
        select_test=st.selectbox("select failed test",failed_tests["test"].unique(),key="select_failed_test")

        selected_row=failed_tests[failed_tests["test"]==select_test].iloc[0]

        with st.expander("View Failure Details:"):
            st.write(f"Test Name:{selected_row['test']}")

            if pd.notna(selected_row.get("mismatch_count")):
                st.write(f"Mismatch Count:{selected_row['mismatch_count']}")

            if pd.notna(selected_row.get("invalid_rows")):
                st.write(f"Invalid Rows:{selected_row['invalid_rows']}")

            if pd.notna(selected_row.get("duplicate_count")):
                st.write(f"Duplicate Count:{selected_row['duplicate_count']}")

            if selected_row.get("null_details"):
                st.write("### Null Details")
                st.json(selected_row["null_details"])

            if selected_row.get("sample_mismatches"):
                st.write("### Sample Mismatches")
                mismatched_df=pd.DataFrame(selected_row["sample_mismatches"])
                st.dataframe(mismatched_df)







    st.subheader("Test Summary Chart")
    summary=df["status"].value_counts()
    # st.bar_chart(summary)
    if not summary.empty:

        fig,ax=plt.subplots()
        ax.pie(summary,labels=summary.index,autopct="%1.1f%%")
        ax.set_title("Test Status Distribution")
        st.pyplot(fig)
    else:
        st.warning("No data to display chart")





    total=len(filtered_df)
    passed=(filtered_df["status"]=="PASS").sum()
    failed=(filtered_df["status"]=="FAIL").sum()
    errors=(filtered_df["status"]=="ERROR").sum()

    st.write(f"Total Tests:{total}")
    st.write(f"Passed Tests:{passed}")
    st.write(f"Failed Tests:{failed}")
    st.write(f"Error tests:{errors}")
    csv=df.to_csv(index=False)
    st.download_button("Download Report",data=csv,file_name=f"Report_{datetime.datetime.now().strftime('%Y-%m-%d_%I-%M-%S')}.csv",mime="text/csv")



