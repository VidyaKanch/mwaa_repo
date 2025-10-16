import datetime as dt
import logging
import os
import io
import pendulum
import requests
from urllib.parse import urlparse

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


# -----------------------------
# CONSTANTS / CONFIG
# -----------------------------
DATA_SOURCE_URL = (
    "https://raw.githubusercontent.com/apache/airflow/main/"
    "airflow-core/docs/tutorial/pipeline_example.csv"
)

# ✅ Per your request: keep this EXACT string unchanged
data_path = "s3://mwaadagsbucket2025/dags/employee.csv"

# MWAA workers can only write locally to /tmp
LOCAL_TMP_FILE = "/tmp/employee.csv"
POSTGRES_CONN_ID = "mwaa_db_postgres"


@dag(
    dag_id="process_employees_latest",
    schedule="0 0 * * *",  # midnight UTC
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=dt.timedelta(minutes=60),
    default_args={
        "owner": "data-eng",
        "retries": 3,
        "retry_delay": dt.timedelta(minutes=2),
    },
    tags=["mwaa", "etl", "postgres", "idempotent"],
)
def ProcessEmployees():
    # -----------------------------
    # 1) Ensure target & staging tables exist
    # -----------------------------
    create_employees_table = SQLExecuteQueryOperator(
        task_id="create_employees_table",
        conn_id=POSTGRES_CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS employees (
                "Serial Number"      NUMERIC PRIMARY KEY,
                "Company Name"       TEXT,
                "Employee Markme"    TEXT,
                "Description"        TEXT,
                "Leave"              INTEGER
            );
        """,
    )

    create_employees_temp_table = SQLExecuteQueryOperator(
        task_id="create_employees_temp_table",
        conn_id=POSTGRES_CONN_ID,
        sql="""
            DROP TABLE IF EXISTS employees_temp;
            CREATE TABLE employees_temp (
                "Serial Number"      NUMERIC PRIMARY KEY,
                "Company Name"       TEXT,
                "Employee Markme"    TEXT,
                "Description"        TEXT,
                "Leave"              INTEGER
            );
        """,
    )

    # -----------------------------
    # 2) Download CSV -> /tmp, then upload to the exact S3 path in `data_path`
    #    (keeps your data_path string unchanged, handles S3 correctly)
    # -----------------------------
    @task(task_id="get_data", retries=3, retry_delay=dt.timedelta(minutes=1))
    def get_data() -> str:
        # Download with a short timeout; fail fast on bad networks
        logging.info({"step": "get_data:start", "url": DATA_SOURCE_URL})
        resp = requests.get(DATA_SOURCE_URL, timeout=10)
        resp.raise_for_status()

        # Write to MWAA local temp
        os.makedirs(os.path.dirname(LOCAL_TMP_FILE), exist_ok=True)
        with open(LOCAL_TMP_FILE, "wb") as f:
            f.write(resp.content)
        logging.info({"step": "get_data:written_local", "path": LOCAL_TMP_FILE, "bytes": len(resp.content)})

        # Parse the kept-as-is S3 URL and upload
        # e.g., s3://mwaadagsbucket2025/dags/employee.csv
        parsed = urlparse(data_path)
        if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
            raise ValueError(f"Invalid S3 URL in data_path: {data_path}")

        bucket = parsed.netloc
        key = parsed.path.lstrip("/")

        s3 = S3Hook()
        s3.load_file(filename=LOCAL_TMP_FILE, key=key, bucket_name=bucket, replace=True)
        logging.info({"step": "get_data:uploaded_s3", "bucket": bucket, "key": key})

        # Return the local file path for the next task (COPY from local)
        return LOCAL_TMP_FILE

    # -----------------------------
    # 3) COPY the CSV into staging (transactional)
    # -----------------------------
    @task(task_id="stage_copy", retries=2, retry_delay=dt.timedelta(minutes=1))
    def stage_copy(local_file_path: str) -> int:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        conn.autocommit = False
        try:
            with conn.cursor() as cur, open(local_file_path, "r") as f:
                cur.copy_expert(
                    """
                    COPY employees_temp FROM STDIN
                    WITH CSV HEADER DELIMITER ',' QUOTE '"'
                    """,
                    f,
                )
            conn.commit()

            # Rough row count (minus header)
            with open(local_file_path, "r") as f:
                rows = max(0, sum(1 for _ in f) - 1)
            logging.info({"step": "stage_copy:done", "rows": rows})
            return rows
        except Exception:
            conn.rollback()
            logging.exception("stage_copy failed; rolled back.")
            raise
        finally:
            conn.close()

    # -----------------------------
    # 4) Idempotent merge from staging -> target (UPSERT)
    # -----------------------------
    @task(task_id="merge_data", retries=2, retry_delay=dt.timedelta(minutes=1))
    def merge_data() -> int:
        query = """
            INSERT INTO employees AS e
            SELECT * FROM (
                SELECT DISTINCT * FROM employees_temp
            ) t
            ON CONFLICT ("Serial Number") DO UPDATE
            SET
              "Employee Markme" = EXCLUDED."Employee Markme",
              "Description"     = EXCLUDED."Description",
              "Leave"           = EXCLUDED."Leave";
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                affected = cur.rowcount if cur.rowcount is not None else -1
            conn.commit()
            logging.info({"step": "merge_data:done", "affected": affected})
            return affected
        except Exception:
            conn.rollback()
            logging.exception("merge_data failed; rolled back.")
            raise
        finally:
            conn.close()

    # -----------------------------
    # DAG graph
    # -----------------------------
    created = [create_employees_table, create_employees_temp_table]
    local_path = get_data()
    rows = stage_copy(local_file_path=local_path)
    created >> local_path >> rows >> merge_data()


dag = ProcessEmployees()
