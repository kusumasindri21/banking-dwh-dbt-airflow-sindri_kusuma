from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd
import duckdb
import os

BASE_DIR = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
DBT_PROJECT_DIR = os.path.join(BASE_DIR, "dbt", "banking_dw")
DUCKDB_PATH = os.path.join(DBT_PROJECT_DIR, "dev.duckdb")

RAW_FOLDER = os.path.join(BASE_DIR, "data", "raw")
os.makedirs(RAW_FOLDER, exist_ok=True)

# ---------------------------------------------------------
# 1. Extract Data → Save Directly to CSV
# ---------------------------------------------------------
def extract_and_save_raw(**context):
    try:
        conn = duckdb.connect(DUCKDB_PATH)
        print(f"Connected to DuckDB → {DUCKDB_PATH}")

        query = "SELECT * FROM transactions;"
        df = conn.execute(query).fetchdf()

        if df.empty:
            raise ValueError("❌ No data extracted — table 'transactions' is empty!")

        file_path = os.path.join(
            RAW_FOLDER,
            f"raw_transactions_{datetime.now().strftime('%Y%m%d')}.csv"
        )

        df.to_csv(file_path, index=False)

        print(f"📁 Extracted & saved CSV → {file_path}")

    except Exception as e:
        raise Exception(f"❌ ERROR: {str(e)}")


# ---------------------------------------------------------
# 2. Validate Final Output After dbt Run
# ---------------------------------------------------------

def validate_output():
    conn = duckdb.connect("/opt/airflow/dbt/banking_dw/dev.duckdb")

    # Check table fact_transaction
    result = conn.execute("SELECT COUNT(*) FROM fact_transaction").fetchone()
    count = result[0]

    if count == 0:
        raise ValueError("❌ fact_transaction exists but contains NO DATA!")

    print(f"✅ fact_transaction table contains {count} rows 🎉")





# ---------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------
default_args = {
    "owner": "Sindri",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="banking_etl_dag",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    description="ETL Pipeline Banking using Airflow + dbt",
) as dag:

    extract_and_save_task = PythonOperator(
        task_id="extract_and_save_raw",
        python_callable=extract_and_save_raw,
    )

    run_dbt_task = BashOperator(
        task_id="run_dbt_transform",
        bash_command="cd /opt/airflow/dbt && dbt run"
    )

    validate_output_task = PythonOperator(
        task_id="validate_output",
        python_callable=validate_output,
    )

    extract_and_save_task >> run_dbt_task >> validate_output_task
