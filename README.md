# ETL Testing Framework

## ETL Testing Flow
<p align='center'>
 <img src="etl_testing/images/etl_testing_image.jpg" alt="ETL flow diagram" width="600"></p>
 
## Overview

The ETL Testing Framework is a Python-based tool designed to validate data integrity and transformations in ETL (Extract, Transform, Load) pipelines. It allows automated verification of row counts, schema, duplicates, data validation rules, null values, datatypes, and salary ranges. The framework also provides an interactive Streamlit dashboard for visualizing test results and generating reports.

This tool identifies failures in the ETL process without manually modifying target data, ensuring that ETL logic is accurate and traceable.

***
## Features

Automated tests for:
- Row count comparison (row_count_check)
- Null value detection in target (target_null_count)
- Data validation (e.g., salary vs. bonus) (data_validation_check)
- Schema consistency (schema_validation_check)
- Duplicate detection (duplicate_validation_check)
- Data type validation (datatype_check_validation)
- Salary range validation (salary_range_validation)

Interactive dashboard using Streamlit
- Upload JSON test configuration
- Run tests and track progress
- Highlight PASS / FAIL / ERROR statuses
- Inspect failed tests with mismatch details
- Visual summary charts (pie chart of test results)
- Export CSV reports

Email alerts for failed tests (optional)

***
Installation
1. Clone the repository:
 ```
 git clone https://github.com/yourusername/etl-testing-framework.git
 cd etl-testing-framework
 ```
2. Create a virtual environment:
   ```
   python -m venv .venv
   ```
3.Activate the environment:
 ```
 .venv\Scripts\activate
 ```
4. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

***

## Configuration
1. Database Connection
 - Update db_connection.py with your database URL.
   ```
   database_url = "mysql+pymysql://username:password@localhost:3306/dbname"
   ```
2. Test Configuration JSON
 - Define the ETL tests to run in a JSON file. Example:
   
```
{
  "tests": [
    {
      "name": "row_count_check",
      "source_query": "SELECT * FROM source_table",
      "target_query": "SELECT * FROM target_table"
    },
    {
      "name": "data_validation_check",
      "source_query": "SELECT * FROM source_table",
      "target_query": "SELECT * FROM target_table"
    }
  ]
}
```

## Usage
1. Run the Streamlit app:
```
streamlit run app.py
```
2. In the browser:
- Upload your JSON test configuration
- Click Run Tests
- View results in the dashboard
- Inspect failed tests and mismatch details
- Download CSV reports
- Receive email alerts for failures (optional)

## Project Structure

```
etl-testing-framework/
├── app.py                  # Streamlit dashboard
├── db_validations.py       # ETL validation test functions
├── db_connection.py        # Database connection helper
├── email_utils.py          # Email alert functions
├── logger.py               # Logging configuration
├── requirements.txt        # Python dependencies
├── README.md               # Project documentation

```

## Author

## Chetan Kumar

