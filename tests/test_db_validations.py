import pandas as pd
from etl_testing import db_connection
from etl_testing.db_validations import *


def get_source_data():
    return pd.DataFrame({"id":[1,2,3],"salary":[1000,2000,3000]})

def get_target_data():
    return pd.DataFrame({"id":[1,2,3],"salary_bonus":[1200,2400,3600]})


def test_row_count():
    res=row_count_check(get_source_data(),get_target_data())
    assert res["status"]=='PASS'

def test_null_check():
    res=target_null_count(get_target_data())
    assert res["status"]=="PASS"

def test_data_validation():
    res=data_validation_check(get_source_data(),get_target_data())
    assert res["status"]=='PASS'

def test_schema():
    res=schema_validation_check(get_source_data(),get_target_data())
    assert res["status"]=='FAIL'

def test_duplicates():
    df=get_target_data()
    res=duplicate_validation_check(df)
    assert res["status"]=='PASS'

def test_datatype():
    res=datatype_check_validation(get_source_data(),get_target_data())
    assert res["status"]==('FAIL')

def test_salary_range():
    res=salary_range_validation(get_target_data())
    assert res["status"]=='PASS'