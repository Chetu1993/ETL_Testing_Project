import db_connection
import pandas as pd
from logger import get_logging
logger=get_logging()
from datetime import datetime
import time

def row_count_check(source,target):
    source_count=len(source)
    target_count=len(target)
    return {"test":"row_count_check",
            "status":"PASS" if source_count==target_count else "FAIL",
            "source_table_count":source_count,
            "target_table_count":target_count}


def target_null_count(target):
    target_table=target.isnull().sum()
    total_sum=target_table.sum()

    return {"test":"target_null_count",
            "status":"PASS" if total_sum==0 else "FAIL",
            "null_details":target_table.to_dict(),
            }

def data_validation_check(source,target):
    required_cols=["id","salary"]
    target_cols=["id","salary_bonus"]
    if not all(col in source.columns for col in required_cols) or \
        not all(col in target.columns for col in target_cols):
        return {"test":"data_validation_check",
                "status":"ERROR",
                "error_message":"required_columns_are_missing"}
    source_copy=source.copy()
    source_copy["expected_salary"]=source_copy["salary"]*1.2
    merged=source_copy.merge(target,on="id",how="inner")

    mismatches=merged[merged["expected_salary"]!=merged["salary_bonus"]]

    return {"test":"data_validation_check",
            "status":"PASS" if mismatches.empty else "FAIL",
            "mismatch_count":len(mismatches),
            "sample_mismatches":mismatches.head(5).to_dict(orient="records")}


def schema_validation_check(source,target):
    return {"test":"schema_validation_check",
            "status":"PASS" if list(source.columns)==list(target.columns) else "FAIL",
            "source_columns":list(source.columns),
            "target_columns":list(target.columns)}


def duplicate_validation_check(target):
    target_cols=["id","salary_bonus"]
    if not all(col in target.columns for col in target_cols):
        return {"test":"duplicate_validation_check","status":"ERROR","error_message":"required columns are missing"}
    duplicates=target[target.duplicated(subset=["id"],keep=False)]
    return {"test":"duplicate_validation_check",
            "status":"PASS" if duplicates.empty else "FAIL",
            "duplicate_count":len(duplicates)}

def datatype_check_validation(source,target):
    return {"test":"datatype_check_validation",
            "status":"PASS" if source.dtypes.to_dict()==target.dtypes.to_dict() else "FAIL"}

def salary_range_validation(target):
    target_cols=["id","salary_bonus"]
    if not all(col in target.columns for col in target_cols):
        return {"test":"salary_range_validation","status":"ERROR","error_message":"target columns are missing"}
    invalid=target[target["salary_bonus"]<0]
    return {"test":"salary_range_validation",
            "status":"PASS" if invalid.empty else "FAIL",
            "invalid_rows":len(invalid)}

def run_tests(config,progress_bar=None,status_text=None):
    results=[]

    tests=config.get("tests",[])
    total_tests=len(tests)
    if total_tests==0:
        logger.warning("No tests found in config")
        return []
    TEST_FUNCTIONS={"row_count_check":row_count_check,
                        "target_null_count":target_null_count,
                        "data_validation_check":data_validation_check,
                        "schema_validation_check":schema_validation_check,
                        "duplicate_validation_check":duplicate_validation_check,
                        "datatype_check_validation":datatype_check_validation,
                        "salary_range_validation":salary_range_validation}

    for i,test in enumerate(tests):
        if status_text:
            status_text.text(f"Running {test.get('name','Unknown_test')}")



        test_name=test.get("name")

        source_query=test.get("source_query")
        target_query=test.get("target_query")

        logger.info(f"{'='*10} Running test:{test_name} {'='*10}")
        logger.info(f'Source query:{source_query}')
        logger.info(f'Target query:{target_query}\n')
        start_time=time.time()
        try:
            source=db_connection.get_query(source_query) if source_query else None
            target=db_connection.get_query(target_query) if target_query else None

        except Exception as db_err:
            logger.error(f"DB error in {test_name}:{db_err}")
            end_time=time.time()
            results.append({
                "test":test_name,
                "status":"ERROR",
                "error_message":"database query failed",
                "execution_time_sec":round(end_time-start_time,3)
            })
            continue



        func=TEST_FUNCTIONS.get(test_name)
        if not func:
            logger.error(f"No function mapped for the test:{test_name}")

            results.append({
                "test":test_name,
                "status":"ERROR",
                "error_message":"No function mapped",
                "execution_time_sec":round(time.time()-start_time,3)
            })
            continue




        try:
            if test_name in ["target_null_count","duplicate_validation_check","salary_range_validation"]:
                if target is None:
                    raise ValueError("Target data is missing")
                res=func(target)


            else:
                if source is None or target is None:
                    raise ValueError("source/target data missing")
                res=func(source,target)


        except Exception as e:

            res={"test":test_name,"status":"ERROR","error_message":str(e)
            }
            res["execution_time_sec"]=round(time.time()-start_time,3)



        if res["status"]=="FAIL":
            logger.error(f"{test_name} Failed-{res.get('error_message','')}")

        elif res["status"]=="ERROR":
            logger.error(f"{test_name} ERROR-{res.get('error_message','')}")

        else:
            logger.info(f"{test_name} PASSED")


        results.append(res)
        if progress_bar:
            progress_bar.progress((i+1)/total_tests)


    if results:
        df=pd.DataFrame(results)
        filename=f"reports_{datetime.now().strftime('%Y-%m-%d_%I-%M-%S')}.csv"
        df.to_csv(filename,index=False)
        summary=df["status"].value_counts()
        print("\nFinal Summary:\n",summary)
    else:
        logger.warning(f"No test results generated")




    return results




