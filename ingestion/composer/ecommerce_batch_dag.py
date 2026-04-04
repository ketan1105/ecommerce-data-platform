import json
import requests
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import tempfile
import os

# Config
PROJECT_ID  = "arboreal-tracer-491416-r1"
BUCKET_NAME = "ecommerce-raw-arboreal-tracer-491416-r1"
API_BASE = "https://mock-api-YOUR_HASH-el.a.run.app"

default_args = {
    "owner"           : "ketan",
    "retries"         : 2,
    "retry_delay"     : timedelta(minutes=5),
    "email_on_failure": False,
}

# Helper — fetch + validate
def fetch_and_validate(endpoint: str, min_rows: int, **context):
    """Fetch data from API, validate, save to temp file, push path via XCom."""
    url      = f"{API_BASE}/{endpoint}"
    response = requests.get(url, params={"limit": 200}, timeout=30)
    response.raise_for_status()

    records = response.json()["data"]
    count   = len(records)
    logging.info(f"Fetched {count} records from {endpoint}")

    # Validation
    if count < min_rows:
        raise ValueError(f"Expected >= {min_rows} rows, got {count}. Failing task.")

    # Save as newline-delimited JSON (what BigQuery loves)
    tmp_path = os.path.join(tempfile.gettempdir(), f"{endpoint}_{datetime.now().strftime('%Y%m%d')}.json")
    with open(tmp_path, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")

    logging.info(f"Saved {count} records to {tmp_path}")

    # Push file path to XCom so next task can pick it up
    context["ti"].xcom_push(key=f"{endpoint}_path", value=tmp_path)
    context["ti"].xcom_push(key=f"{endpoint}_count", value=count)

def fetch_products(**context):
    fetch_and_validate("products", min_rows=50, **context)

def fetch_customers(**context):
    fetch_and_validate("customers", min_rows=50, **context)

def log_summary(**context):
    """Log final summary after all loads complete."""
    ti = context["ti"]
    p_count = ti.xcom_pull(task_ids="fetch_products",  key="products_count")
    c_count = ti.xcom_pull(task_ids="fetch_customers", key="customers_count")
    logging.info(f"Pipeline complete. Products: {p_count} | Customers: {c_count}")

# DAG definition
with DAG(
    dag_id            = "ecommerce_batch_ingestion",
    default_args      = default_args,
    description       = "Daily batch ingestion of products and customers to GCS + BQ",
    schedule_interval = "0 1 * * *",   # runs every day at 1 AM UTC
    start_date        = datetime(2024, 1, 1),
    catchup           = False,
    tags              = ["ecommerce", "batch", "ingestion"],
) as dag:

    # Products chain
    fetch_products_task = PythonOperator(
        task_id         = "fetch_products",
        python_callable = fetch_products,
    )

    upload_products_task = LocalFilesystemToGCSOperator(
        task_id      = "upload_products_to_gcs",
        src          = "{{ ti.xcom_pull(task_ids='fetch_products', key='products_path') }}",
        dst          = "products/{{ ds }}/products.json",
        bucket       = BUCKET_NAME,
    )

    load_products_bq = GCSToBigQueryOperator(
        task_id              = "load_products_to_bq",
        bucket               = BUCKET_NAME,
        source_objects       = ["products/{{ ds }}/products.json"],
        destination_project_dataset_table = f"{PROJECT_ID}.ecommerce_raw.products",
        source_format        = "NEWLINE_DELIMITED_JSON",
        write_disposition    = "WRITE_TRUNCATE",   # full refresh daily
        autodetect           = True,
    )

    # Customers chain
    fetch_customers_task = PythonOperator(
        task_id         = "fetch_customers",
        python_callable = fetch_customers,
    )

    upload_customers_task = LocalFilesystemToGCSOperator(
        task_id      = "upload_customers_to_gcs",
        src          = "{{ ti.xcom_pull(task_ids='fetch_customers', key='customers_path') }}",
        dst          = "customers/{{ ds }}/customers.json",
        bucket       = BUCKET_NAME,
    )

    load_customers_bq = GCSToBigQueryOperator(
        task_id              = "load_customers_to_bq",
        bucket               = BUCKET_NAME,
        source_objects       = ["customers/{{ ds }}/customers.json"],
        destination_project_dataset_table = f"{PROJECT_ID}.ecommerce_raw.customers",
        source_format        = "NEWLINE_DELIMITED_JSON",
        write_disposition    = "WRITE_TRUNCATE",
        autodetect           = True,
    )

    # Summary task
    summary_task = PythonOperator(
        task_id         = "log_summary",
        python_callable = log_summary,
    )

    # Dependencies — defines the order
    fetch_products_task  >> upload_products_task  >> load_products_bq  >> summary_task
    fetch_customers_task >> upload_customers_task >> load_customers_bq >> summary_task