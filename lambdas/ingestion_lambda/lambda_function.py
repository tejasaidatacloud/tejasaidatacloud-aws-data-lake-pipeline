"""
AWS Lambda – Data Ingestion Handler
=====================================
Pulls data from REST APIs and validates flat-file uploads,
then lands everything in the Raw (Bronze) layer of S3.

Triggered by: Airflow DAG  |  EventBridge schedule
"""

from __future__ import annotations

import io
import json
import logging
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ─── Environment ──────────────────────────────────────────────────────────────
S3_BUCKET   = os.environ.get("S3_BUCKET",   "data-lake-dev-bucket")
AWS_REGION  = os.environ.get("AWS_REGION",  "us-east-1")
CW_NAMESPACE = "DataLakePipeline"
MAX_RETRIES  = 3
RETRY_BACKOFF = 2  # seconds

s3_client  = boto3.client("s3",          region_name=AWS_REGION)
cw_client  = boto3.client("cloudwatch",  region_name=AWS_REGION)


# ─── Source Definitions ────────────────────────────────────────────────────────
SOURCE_REGISTRY: dict[str, dict] = {
    "open_meteo": {
        "url": (
            "https://api.open-meteo.com/v1/forecast"
            "?latitude=40.71&longitude=-74.01"
            "&hourly=temperature_2m,precipitation,windspeed_10m"
            "&forecast_days=1"
        ),
        "format": "json",
        "s3_prefix": "raw/weather/",
    },
    "coindesk": {
        "url": "https://api.coindesk.com/v1/bpi/currentprice.json",
        "format": "json",
        "s3_prefix": "raw/crypto/",
    },
}


# ─── Helpers ───────────────────────────────────────────────────────────────────

def _put_metric(name: str, value: float, unit: str = "Count", execution_date: str = "") -> None:
    """Push a single metric to CloudWatch."""
    try:
        cw_client.put_metric_data(
            Namespace=CW_NAMESPACE,
            MetricData=[{
                "MetricName": name,
                "Dimensions": [
                    {"Name": "Pipeline",       "Value": "Ingestion"},
                    {"Name": "ExecutionDate",  "Value": execution_date},
                ],
                "Value":     value,
                "Unit":      unit,
                "Timestamp": datetime.now(tz=timezone.utc),
            }],
        )
    except Exception as exc:
        logger.warning("CloudWatch metric push failed: %s", exc)


def _fetch_url(url: str) -> bytes:
    """HTTP GET with retry/backoff."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "DataLakePipeline/1.0", "Accept": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = resp.read()
                logger.info("✅  Fetched %s bytes from %s", len(data), url)
                return data
        except urllib.error.HTTPError as e:
            logger.warning("HTTP %s on attempt %d/%d for %s", e.code, attempt, MAX_RETRIES, url)
            if e.code in (400, 401, 403, 404):
                raise  # non-retriable
        except Exception as exc:
            logger.warning("Attempt %d/%d failed: %s", attempt, MAX_RETRIES, exc)

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF ** attempt)

    raise RuntimeError(f"All {MAX_RETRIES} attempts failed for {url}")


def _upload_to_s3(data: bytes, bucket: str, key: str) -> str:
    """Upload bytes to S3 and return the full S3 URI."""
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType="application/json",
        Metadata={
            "ingested_at": datetime.now(tz=timezone.utc).isoformat(),
            "pipeline":    "data-lake-ingestion",
        },
    )
    uri = f"s3://{bucket}/{key}"
    logger.info("📦  Uploaded → %s", uri)
    return uri


def _ingest_api_source(source_name: str, config: dict, execution_date: str) -> dict:
    """Fetch a REST API source and land it in S3 raw layer."""
    start = time.perf_counter()
    try:
        raw_bytes = _fetch_url(config["url"])

        # Parse to validate JSON integrity
        payload = json.loads(raw_bytes)

        # Wrap with ingestion metadata
        envelope = {
            "source":          source_name,
            "ingested_at":     datetime.now(tz=timezone.utc).isoformat(),
            "execution_date":  execution_date,
            "record_count":    _count_records(payload),
            "data":            payload,
        }
        enriched = json.dumps(envelope, ensure_ascii=False).encode()

        ts  = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        key = f"{config['s3_prefix']}{execution_date.replace('-', '/')}/{source_name}_{ts}.json"

        uri = _upload_to_s3(enriched, S3_BUCKET, key)
        elapsed = time.perf_counter() - start

        _put_metric("RecordsIngested", envelope["record_count"], execution_date=execution_date)
        _put_metric("IngestionDurationSeconds", elapsed, unit="Seconds", execution_date=execution_date)

        return {"source": source_name, "status": "success", "s3_uri": uri, "records": envelope["record_count"]}

    except Exception as exc:
        _put_metric("IngestionErrors", 1, execution_date=execution_date)
        logger.error("❌  Failed to ingest %s: %s", source_name, exc, exc_info=True)
        return {"source": source_name, "status": "failed", "error": str(exc)}


def _ingest_flat_file(source_cfg: dict, execution_date: str) -> dict:
    """Copy a flat-file upload from the landing zone to the raw layer."""
    src_key  = source_cfg.get("s3_key", "")
    src_name = source_cfg.get("name", "unknown")

    if not src_key:
        return {"source": src_name, "status": "skipped", "reason": "no s3_key provided"}

    try:
        obj      = s3_client.get_object(Bucket=S3_BUCKET, Key=src_key)
        content  = obj["Body"].read()
        size_kb  = len(content) / 1024

        ts  = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        dest_key = f"raw/flat_files/{execution_date.replace('-', '/')}/{src_name}_{ts}.csv"

        s3_client.put_object(
            Bucket=S3_BUCKET, Key=dest_key, Body=content,
            Metadata={"source": src_key, "ingested_at": datetime.now(tz=timezone.utc).isoformat()},
        )

        logger.info("📄  Flat file copied → s3://%s/%s (%.1f KB)", S3_BUCKET, dest_key, size_kb)
        return {"source": src_name, "status": "success", "s3_uri": f"s3://{S3_BUCKET}/{dest_key}", "size_kb": size_kb}

    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "NoSuchKey":
            logger.warning("⚠️  Flat file not found: %s", src_key)
            return {"source": src_name, "status": "skipped", "reason": "file not found"}
        raise


def _count_records(payload: Any) -> int:
    """Best-effort record count from arbitrary JSON."""
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        for key in ("data", "records", "items", "results", "hourly"):
            val = payload.get(key)
            if isinstance(val, (list, dict)):
                return len(val) if isinstance(val, list) else sum(len(v) for v in val.values() if isinstance(v, list))
    return 1


# ─── Lambda Handler ────────────────────────────────────────────────────────────

def lambda_handler(event: dict, context: Any) -> dict:
    """
    Entry point.

    Event schema:
    {
        "execution_date": "2026-04-22",
        "s3_bucket": "data-lake-dev-bucket",          # optional override
        "sources": [
            {"name": "open_meteo",  "type": "rest_api"},
            {"name": "transactions","type": "flat_file","s3_key": "uploads/transactions_20260422.csv"}
        ]
    }
    """
    logger.info("📨  Event received: %s", json.dumps(event))

    global S3_BUCKET
    if event.get("s3_bucket"):
        S3_BUCKET = event["s3_bucket"]

    execution_date = event.get("execution_date", datetime.now(tz=timezone.utc).strftime("%Y-%m-%d"))
    sources        = event.get("sources", list({"name": k, "type": "rest_api"} for k in SOURCE_REGISTRY))

    results = []
    for src in sources:
        src_name = src.get("name")
        src_type = src.get("type", "rest_api")

        if src_type == "rest_api":
            cfg = SOURCE_REGISTRY.get(src_name)
            if not cfg:
                results.append({"source": src_name, "status": "skipped", "reason": "not in registry"})
                continue
            results.append(_ingest_api_source(src_name, cfg, execution_date))

        elif src_type == "flat_file":
            results.append(_ingest_flat_file(src, execution_date))

        else:
            results.append({"source": src_name, "status": "skipped", "reason": f"unknown type: {src_type}"})

    success_count = sum(1 for r in results if r["status"] == "success")
    failure_count = sum(1 for r in results if r["status"] == "failed")

    logger.info("📊  Ingestion summary: %d success, %d failed, %d total", success_count, failure_count, len(results))

    return {
        "statusCode": 200 if failure_count == 0 else 207,
        "execution_date": execution_date,
        "summary": {"success": success_count, "failed": failure_count, "total": len(results)},
        "results": results,
    }
