"""
AWS Data Lake Pipeline - Apache Airflow DAG
============================================
Orchestrates the full 3-layer data lake pipeline:
  Raw (Bronze) → Cleaned (Silver) → Curated (Gold)

Author  : Sai Teja
Schedule: Daily at 02:00 UTC
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.providers.amazon.aws.hooks.cloudwatch import CloudWatchHook
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

# ─── Default Args ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["saiteja09012001@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=3),
}

# ─── Config (pulled from Airflow Variables) ────────────────────────────────────
ENV                  = Variable.get("ENV", default_var="dev")
AWS_REGION           = Variable.get("AWS_REGION", default_var="us-east-1")
AWS_CONN_ID          = "aws_default"
S3_BUCKET            = Variable.get("DATA_LAKE_BUCKET", default_var=f"data-lake-{ENV}-bucket")
GLUE_RAW_JOB         = f"data-lake-{ENV}-raw-to-cleaned"
GLUE_CURATED_JOB     = f"data-lake-{ENV}-cleaned-to-curated"
INGESTION_LAMBDA     = f"data-lake-{ENV}-api-ingestion"
VALIDATION_LAMBDA    = f"data-lake-{ENV}-data-validator"
CW_NAMESPACE         = "DataLakePipeline"


# ─── Python Callables ──────────────────────────────────────────────────────────

def check_source_availability(**context) -> str:
    """Ping source APIs / check S3 manifests; branch on availability."""
    import boto3
    import urllib.request

    sources = {
        "open_meteo": "https://api.open-meteo.com/v1/forecast?latitude=40.71&longitude=-74.01&hourly=temperature_2m&forecast_days=1",
        "coindesk":   "https://api.coindesk.com/v1/bpi/currentprice.json",
    }
    available = []
    for name, url in sources.items():
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "DataLakeChecker/1.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                if r.status == 200:
                    available.append(name)
                    logger.info("✅  Source available: %s", name)
        except Exception as exc:
            logger.warning("⚠️  Source unavailable: %s – %s", name, exc)

    context["ti"].xcom_push(key="available_sources", value=available)
    return "ingest_data" if available else "skip_run"


def push_cloudwatch_metric(metric_name: str, value: float, unit: str = "Count", **context):
    """Push a custom metric to CloudWatch."""
    import boto3
    client = boto3.client("cloudwatch", region_name=AWS_REGION)
    execution_date = context["ds"]
    client.put_metric_data(
        Namespace=CW_NAMESPACE,
        MetricData=[
            {
                "MetricName": metric_name,
                "Dimensions": [
                    {"Name": "Environment", "Value": ENV},
                    {"Name": "Pipeline",    "Value": "DataLake"},
                    {"Name": "ExecutionDate", "Value": execution_date},
                ],
                "Value": value,
                "Unit": unit,
                "Timestamp": datetime.utcnow(),
            }
        ],
    )
    logger.info("📊  CloudWatch metric pushed: %s = %s %s", metric_name, value, unit)


def validate_raw_data(**context):
    """Check row counts and schema drift in raw layer before proceeding."""
    import boto3

    s3 = boto3.client("s3", region_name=AWS_REGION)
    prefix = f"raw/{context['ds_nodash']}/"
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)

    objects  = response.get("Contents", [])
    if not objects:
        raise ValueError(f"No raw files found at s3://{S3_BUCKET}/{prefix}")

    total_size = sum(o["Size"] for o in objects)
    file_count = len(objects)
    logger.info("📁  Raw layer: %d files, %.2f MB", file_count, total_size / 1_048_576)

    # Push metrics
    push_cloudwatch_metric("RawFileCount", file_count, **context)
    push_cloudwatch_metric("RawDataSizeBytes", total_size, unit="Bytes", **context)

    context["ti"].xcom_push(key="raw_file_count", value=file_count)
    context["ti"].xcom_push(key="raw_data_size",  value=total_size)


def send_pipeline_success_notification(**context):
    """Emit success metric and log summary."""
    ti = context["ti"]
    raw_count = ti.xcom_pull(task_ids="validate_raw_layer", key="raw_file_count") or 0
    logger.info("🎉  Pipeline completed successfully for %s. Raw files processed: %s", context["ds"], raw_count)
    push_cloudwatch_metric("PipelineSuccess", 1, **context)


def send_pipeline_failure_notification(**context):
    """Emit failure metric."""
    logger.error("❌  Pipeline FAILED for execution date %s", context["ds"])
    push_cloudwatch_metric("PipelineFailure", 1, **context)


# ─── Ingestion Lambda Payload ──────────────────────────────────────────────────

def build_ingestion_payload(**context) -> dict:
    return json.dumps({
        "execution_date": context["ds"],
        "s3_bucket": S3_BUCKET,
        "sources": [
            {"name": "open_meteo",  "type": "rest_api"},
            {"name": "coindesk",    "type": "rest_api"},
            {"name": "transactions","type": "flat_file", "s3_key": f"uploads/transactions_{context['ds_nodash']}.csv"},
        ],
    })


# ─── DAG Definition ───────────────────────────────────────────────────────────
with DAG(
    dag_id="aws_data_lake_pipeline",
    description="3-layer data lake ingestion & transformation pipeline",
    default_args=default_args,
    schedule_interval="0 2 * * *",   # Daily at 02:00 UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["data-lake", "production", "pyspark", "aws"],
    doc_md=__doc__,
) as dag:

    # ── Branch: check sources ─────────────────────────────────────────────────
    check_sources = BranchPythonOperator(
        task_id="check_source_availability",
        python_callable=check_source_availability,
    )

    skip_run = EmptyOperator(task_id="skip_run")

    # ── Stage 1 – Ingestion (Lambda) ──────────────────────────────────────────
    ingest_data = LambdaInvokeFunctionOperator(
        task_id="ingest_data",
        function_name=INGESTION_LAMBDA,
        payload=build_ingestion_payload,
        aws_conn_id=AWS_CONN_ID,
        log_type="Tail",
    )

    # ── Stage 2 – Validate raw layer ──────────────────────────────────────────
    validate_raw = PythonOperator(
        task_id="validate_raw_layer",
        python_callable=validate_raw_data,
    )

    # ── Stage 3 – Validate with Lambda (schema + nulls) ──────────────────────
    run_validation_lambda = LambdaInvokeFunctionOperator(
        task_id="run_validation_lambda",
        function_name=VALIDATION_LAMBDA,
        payload=json.dumps({"layer": "raw", "execution_date": "{{ ds }}"}),
        aws_conn_id=AWS_CONN_ID,
    )

    # ── Stage 4 – Glue: Raw → Cleaned (PySpark) ───────────────────────────────
    raw_to_cleaned = GlueJobOperator(
        task_id="glue_raw_to_cleaned",
        job_name=GLUE_RAW_JOB,
        script_args={
            "--execution_date": "{{ ds }}",
            "--s3_bucket":      S3_BUCKET,
            "--env":            ENV,
        },
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        num_of_dpus=2,
    )

    wait_raw_to_cleaned = GlueJobSensor(
        task_id="wait_raw_to_cleaned",
        job_name=GLUE_RAW_JOB,
        run_id="{{ task_instance.xcom_pull('glue_raw_to_cleaned') }}",
        aws_conn_id=AWS_CONN_ID,
        poke_interval=60,
        timeout=3600,
    )

    # ── Stage 5 – Glue: Cleaned → Curated (PySpark) ───────────────────────────
    cleaned_to_curated = GlueJobOperator(
        task_id="glue_cleaned_to_curated",
        job_name=GLUE_CURATED_JOB,
        script_args={
            "--execution_date": "{{ ds }}",
            "--s3_bucket":      S3_BUCKET,
            "--env":            ENV,
        },
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        num_of_dpus=4,
    )

    wait_cleaned_to_curated = GlueJobSensor(
        task_id="wait_cleaned_to_curated",
        job_name=GLUE_CURATED_JOB,
        run_id="{{ task_instance.xcom_pull('glue_cleaned_to_curated') }}",
        aws_conn_id=AWS_CONN_ID,
        poke_interval=60,
        timeout=7200,
    )

    # ── Stage 6 – Success notification ───────────────────────────────────────
    pipeline_success = PythonOperator(
        task_id="pipeline_success_notification",
        python_callable=send_pipeline_success_notification,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    pipeline_failure = PythonOperator(
        task_id="pipeline_failure_notification",
        python_callable=send_pipeline_failure_notification,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ── DAG Wiring ────────────────────────────────────────────────────────────
    check_sources >> [ingest_data, skip_run]

    (
        ingest_data
        >> validate_raw
        >> run_validation_lambda
        >> raw_to_cleaned
        >> wait_raw_to_cleaned
        >> cleaned_to_curated
        >> wait_cleaned_to_curated
        >> pipeline_success
    )

    [validate_raw, run_validation_lambda, raw_to_cleaned, cleaned_to_curated] >> pipeline_failure
