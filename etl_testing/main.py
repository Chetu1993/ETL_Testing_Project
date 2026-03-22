from db_connection import get_query
from db_validations import *
import json
try:
    source=get_query("select * from source_table;")
    target = get_query("select* from target_table;")
except Exception as e:
    logger.error(e)

#
# print("Row count check:",row_count_match(source,target))
#
# print("Null values check:",target_null_count(target))
#
# mismatch=data_validation(source,target)
#
# if mismatch.empty:
#     print("Data match:PASS")
# else:
#     print("Data match:FAIL")
#     print(mismatch)
print("running Main file")
if __name__=="__main__":

    with open("test_config.json","r") as f:
        config=json.load(f)

    results=run_tests(config)
    for r in results:
        print(r)

