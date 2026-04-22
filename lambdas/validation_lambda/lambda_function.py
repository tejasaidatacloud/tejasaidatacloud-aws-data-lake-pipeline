"""
AWS Lambda – Data Validation Handler
======================================
Performs schema validation, null-rate checks, row-count anomaly detection,
and data-freshness verification on any layer of the data lake.
Publishes pass/fail results to CloudWatch and writes a validation report to S3.
"""

from __future__ import annotations

import json
import logging
import os
import statistics
from datetime import datetime, timezone
from typing import Any

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET    = os.environ.get("S3_BUCKET",   "data-lake-dev-bucket")
AWS_REGION   = os.environ.get("AWS_REGION",  "us-east-1")
CW_NAMESPACE = "DataLakePipeline"
MAX_NULL_RATE  = float(os.environ.get("MAX_NULL_RATE",  "0.05"))   # 5 %
MIN_ROW_COUNT  = int(os.environ.get("MIN_ROW_COUNT",   "10"))
ANOMALY_STDDEV = float(os.environ.get("ANOMALY_STDDEV", "3.0"))    # z-score threshold

s3_client = boto3.client("s3",         region_name=AWS_REGION)
cw_client = boto3.client("cloudwatch", region_name=AWS_REGION)
ssm_client= boto3.client("ssm",        region_name=AWS_REGION)


# ─── Expected Schemas ─────────────────────────────────────────────────────────
LAYER_SCHEMAS: dict[str, dict] = {
    "raw": {
        "weather": ["source", "ingested_at", "execution_date", "data"],
        "crypto":  ["source", "ingested_at", "execution_date", "data"],
    },
    "cleaned": {
        "weather": ["date", "hour", "temperature_c", "precipitation_mm", "windspeed_kmh", "ingested_at"],
        "crypto":  ["date", "currency", "rate_usd", "rate_gbp", "rate_eur", "ingested_at"],
    },
    "curated": {
        "daily_summary": ["date", "avg_temp_c", "max_temp_c", "min_temp_c",
                          "total_precip_mm", "btc_usd_close", "processed_at"],
    },
}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _put_metric(name: str, value: float, unit: str = "Count",
                layer: str = "", dataset: str = "", execution_date: str = "") -> None:
    cw_client.put_metric_data(
        Namespace=CW_NAMESPACE,
        MetricData=[{
            "MetricName": name,
            "Dimensions": [
                {"Name": "Layer",          "Value": layer or "unknown"},
                {"Name": "Dataset",        "Value": dataset or "unknown"},
                {"Name": "ExecutionDate",  "Value": execution_date or "unknown"},
            ],
            "Value":     value,
            "Unit":      unit,
            "Timestamp": datetime.now(tz=timezone.utc),
        }],
    )


def _list_s3_objects(prefix: str) -> list[dict]:
    paginator = s3_client.get_paginator("list_objects_v2")
    objects   = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        objects.extend(page.get("Contents", []))
    return objects


def _read_json_object(key: str) -> Any:
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    return json.loads(obj["Body"].read())


def _get_historical_row_counts(layer: str, dataset: str, lookback: int = 7) -> list[int]:
    """Retrieve recent daily row counts from SSM Parameter Store for anomaly detection."""
    param_name = f"/data-lake/{layer}/{dataset}/row_counts"
    try:
        resp = ssm_client.get_parameter(Name=param_name)
        counts = json.loads(resp["Parameter"]["Value"])
        return [int(c) for c in counts[-lookback:]]
    except ssm_client.exceptions.ParameterNotFound:
        return []
    except Exception as exc:
        logger.warning("Could not fetch historical row counts: %s", exc)
        return []


def _store_row_count(layer: str, dataset: str, count: int) -> None:
    param_name = f"/data-lake/{layer}/{dataset}/row_counts"
    try:
        try:
            resp   = ssm_client.get_parameter(Name=param_name)
            counts = json.loads(resp["Parameter"]["Value"])
        except ssm_client.exceptions.ParameterNotFound:
            counts = []
        counts.append(count)
        counts = counts[-30:]   # keep 30 days
        ssm_client.put_parameter(
            Name=param_name, Value=json.dumps(counts),
            Type="String", Overwrite=True,
        )
    except Exception as exc:
        logger.warning("Could not store row count: %s", exc)


# ─── Validation Checks ────────────────────────────────────────────────────────

class ValidationResult:
    def __init__(self, dataset: str, layer: str):
        self.dataset   = dataset
        self.layer     = layer
        self.checks:   list[dict] = []
        self.passed    = 0
        self.failed    = 0

    def add(self, name: str, ok: bool, detail: str = "") -> None:
        status = "PASS" if ok else "FAIL"
        icon   = "✅" if ok else "❌"
        self.checks.append({"check": name, "status": status, "detail": detail})
        if ok:
            self.passed += 1
        else:
            self.failed += 1
        logger.info("%s  [%s/%s] %s: %s", icon, self.layer, self.dataset, name, detail or status)

    @property
    def overall(self) -> str:
        return "PASS" if self.failed == 0 else "FAIL"

    def to_dict(self) -> dict:
        return {
            "dataset":  self.dataset,
            "layer":    self.layer,
            "overall":  self.overall,
            "passed":   self.passed,
            "failed":   self.failed,
            "checks":   self.checks,
        }


def run_schema_check(result: ValidationResult, records: list[dict], expected_fields: list[str]) -> None:
    if not records:
        result.add("schema_check", False, "No records to validate")
        return
    sample = records[0]
    missing = [f for f in expected_fields if f not in sample]
    result.add(
        "schema_check",
        len(missing) == 0,
        f"All {len(expected_fields)} fields present" if not missing else f"Missing fields: {missing}",
    )


def run_null_rate_check(result: ValidationResult, records: list[dict], fields: list[str]) -> None:
    if not records:
        result.add("null_rate_check", False, "No records")
        return
    for field in fields:
        null_count = sum(1 for r in records if r.get(field) is None or r.get(field) == "")
        null_rate  = null_count / len(records)
        result.add(
            f"null_rate_{field}",
            null_rate <= MAX_NULL_RATE,
            f"{null_rate:.1%} null ({null_count}/{len(records)}) — threshold {MAX_NULL_RATE:.0%}",
        )


def run_row_count_check(result: ValidationResult, count: int, layer: str, dataset: str) -> None:
    # Minimum threshold
    result.add("min_row_count", count >= MIN_ROW_COUNT, f"{count} rows (min {MIN_ROW_COUNT})")

    # Anomaly detection via z-score
    history = _get_historical_row_counts(layer, dataset)
    if len(history) >= 3:
        mean   = statistics.mean(history)
        stddev = statistics.stdev(history) or 1
        z      = abs(count - mean) / stddev
        result.add(
            "row_count_anomaly",
            z <= ANOMALY_STDDEV,
            f"z-score={z:.2f} (mean={mean:.0f}, stddev={stddev:.0f}) — threshold {ANOMALY_STDDEV}",
        )

    _store_row_count(layer, dataset, count)


def run_freshness_check(result: ValidationResult, objects: list[dict], max_age_hours: int = 26) -> None:
    if not objects:
        result.add("freshness_check", False, "No files found")
        return
    latest_mod  = max(o["LastModified"] for o in objects)
    age_hours   = (datetime.now(tz=timezone.utc) - latest_mod).total_seconds() / 3600
    result.add(
        "freshness_check",
        age_hours <= max_age_hours,
        f"Latest file {age_hours:.1f}h old (max {max_age_hours}h)",
    )


# ─── Per-Layer Validators ─────────────────────────────────────────────────────

def validate_layer(layer: str, dataset: str, execution_date: str) -> ValidationResult:
    result  = ValidationResult(dataset=dataset, layer=layer)
    prefix  = f"{layer}/{dataset}/{execution_date.replace('-', '/')}/"
    objects = _list_s3_objects(prefix)

    run_freshness_check(result, objects)

    if not objects:
        logger.warning("No files at prefix %s", prefix)
        return result

    # Load all JSON records from files in this partition
    records: list[dict] = []
    for obj in objects[:20]:   # cap at 20 files for Lambda memory
        try:
            data = _read_json_object(obj["Key"])
            if isinstance(data, list):
                records.extend(data)
            elif isinstance(data, dict):
                inner = data.get("data") or data.get("records") or data.get("items") or [data]
                records.extend(inner if isinstance(inner, list) else [inner])
        except Exception as exc:
            logger.warning("Could not read %s: %s", obj["Key"], exc)

    expected_fields = LAYER_SCHEMAS.get(layer, {}).get(dataset, [])

    run_row_count_check(result, len(records), layer, dataset)
    run_schema_check(result, records, expected_fields)
    run_null_rate_check(result, records, expected_fields[:5])   # check first 5 critical fields

    _put_metric("ValidationPassed", result.passed, layer=layer, dataset=dataset, execution_date=execution_date)
    _put_metric("ValidationFailed", result.failed, layer=layer, dataset=dataset, execution_date=execution_date)
    _put_metric("RowCount",         len(records),  layer=layer, dataset=dataset, execution_date=execution_date)

    return result


# ─── Lambda Handler ────────────────────────────────────────────────────────────

def lambda_handler(event: dict, context: Any) -> dict:
    """
    Event schema:
    {
        "layer":          "raw" | "cleaned" | "curated",
        "execution_date": "2026-04-22",
        "datasets":       ["weather", "crypto"]   # optional; defaults to all for layer
    }
    """
    logger.info("🔍  Validation event: %s", json.dumps(event))

    layer          = event.get("layer", "raw")
    execution_date = event.get("execution_date", datetime.now(tz=timezone.utc).strftime("%Y-%m-%d"))
    all_datasets   = list(LAYER_SCHEMAS.get(layer, {}).keys())
    datasets       = event.get("datasets", all_datasets)

    all_results = []
    for dataset in datasets:
        res = validate_layer(layer, dataset, execution_date)
        all_results.append(res.to_dict())

    overall_status = "PASS" if all(r["overall"] == "PASS" for r in all_results) else "FAIL"

    report = {
        "execution_date": execution_date,
        "layer":          layer,
        "overall_status": overall_status,
        "validated_at":   datetime.now(tz=timezone.utc).isoformat(),
        "datasets":       all_results,
    }

    # Persist validation report to S3
    report_key = f"validation_reports/{layer}/{execution_date.replace('-', '/')}/report_{datetime.now(tz=timezone.utc).strftime('%H%M%S')}.json"
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=report_key,
        Body=json.dumps(report, indent=2).encode(),
        ContentType="application/json",
    )
    logger.info("📝  Validation report → s3://%s/%s", S3_BUCKET, report_key)

    return {
        "statusCode":     200 if overall_status == "PASS" else 400,
        "overall_status": overall_status,
        "report_s3_uri":  f"s3://{S3_BUCKET}/{report_key}",
        "results":        all_results,
    }
