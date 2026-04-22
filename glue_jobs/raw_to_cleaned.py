"""
AWS Glue PySpark Job – Raw (Bronze) → Cleaned (Silver)
=======================================================
Reads raw JSON files from S3, applies transformations:
  • Schema enforcement & type casting
  • Deduplication
  • Null handling & imputation
  • Data standardisation
Writes Parquet partitioned output to the Cleaned layer.

Run via: AWS Glue Console or Airflow GlueJobOperator
"""

import sys
import logging
from datetime import datetime, timezone

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, DateType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── Job Arguments ────────────────────────────────────────────────────────────
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "execution_date", "s3_bucket", "env"],
)
EXECUTION_DATE = args["execution_date"]          # e.g. "2026-04-22"
S3_BUCKET      = args["s3_bucket"]
ENV            = args["env"]
DATE_PATH      = EXECUTION_DATE.replace("-", "/")  # "2026/04/22"

RAW_BASE     = f"s3://{S3_BUCKET}/raw"
CLEANED_BASE = f"s3://{S3_BUCKET}/cleaned"

# ─── Spark / Glue Setup ───────────────────────────────────────────────────────
sc           = SparkContext()
glueContext  = GlueContext(sc)
spark        = glueContext.spark_session
job          = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.adaptive.enabled",               "true")
spark.conf.set("spark.sql.shuffle.partitions",             "50")

PROCESSED_AT = datetime.now(tz=timezone.utc).isoformat()


# ─── Schema Definitions ───────────────────────────────────────────────────────

WEATHER_CLEANED_SCHEMA = StructType([
    StructField("date",             DateType(),      False),
    StructField("hour",             IntegerType(),   False),
    StructField("temperature_c",    DoubleType(),    True),
    StructField("precipitation_mm", DoubleType(),    True),
    StructField("windspeed_kmh",    DoubleType(),    True),
    StructField("source",           StringType(),    True),
    StructField("ingested_at",      TimestampType(), True),
    StructField("execution_date",   DateType(),      True),
])

CRYPTO_CLEANED_SCHEMA = StructType([
    StructField("date",        DateType(),   False),
    StructField("currency",    StringType(), False),
    StructField("rate_usd",    DoubleType(), True),
    StructField("rate_gbp",    DoubleType(), True),
    StructField("rate_eur",    DoubleType(), True),
    StructField("description", StringType(), True),
    StructField("ingested_at", TimestampType(), True),
    StructField("execution_date", DateType(), True),
])


# ─── Transformation Functions ─────────────────────────────────────────────────

def read_raw_json(dataset: str) -> DataFrame:
    """Read all raw JSON files for the given dataset and date partition."""
    path = f"{RAW_BASE}/{dataset}/{DATE_PATH}/*.json"
    logger.info("📖  Reading raw data from %s", path)
    try:
        df = spark.read.option("multiLine", True).json(path)
        logger.info("✅  Loaded %d records from %s", df.count(), dataset)
        return df
    except Exception as exc:
        logger.error("❌  Failed to read %s: %s", path, exc)
        raise


def clean_weather(df: DataFrame) -> DataFrame:
    """
    Flatten Open-Meteo API response and produce clean hourly weather records.
    Raw structure: { source, ingested_at, data: { hourly: { time:[], temperature_2m:[], ... } } }
    """
    logger.info("🌤   Cleaning weather data …")

    # Explode arrays from Open-Meteo format
    df_exploded = (
        df
        .select(
            F.col("source"),
            F.col("ingested_at"),
            F.col("execution_date"),
            F.explode(
                F.arrays_zip(
                    F.col("data.hourly.time"),
                    F.col("data.hourly.temperature_2m"),
                    F.col("data.hourly.precipitation"),
                    F.col("data.hourly.windspeed_10m"),
                )
            ).alias("zipped"),
        )
        .select(
            F.col("source"),
            F.col("ingested_at"),
            F.col("execution_date"),
            F.col("zipped.0").alias("time_str"),
            F.col("zipped.1").alias("temperature_c"),
            F.col("zipped.2").alias("precipitation_mm"),
            F.col("zipped.3").alias("windspeed_kmh"),
        )
    )

    df_typed = (
        df_exploded
        .withColumn("datetime",         F.to_timestamp("time_str", "yyyy-MM-dd'T'HH:mm"))
        .withColumn("date",             F.to_date("datetime"))
        .withColumn("hour",             F.hour("datetime"))
        .withColumn("temperature_c",    F.round(F.col("temperature_c").cast(DoubleType()), 2))
        .withColumn("precipitation_mm", F.round(F.col("precipitation_mm").cast(DoubleType()), 3))
        .withColumn("windspeed_kmh",    F.round(F.col("windspeed_kmh").cast(DoubleType()), 2))
        .withColumn("ingested_at",      F.to_timestamp("ingested_at"))
        .withColumn("execution_date",   F.to_date(F.lit(EXECUTION_DATE)))
    )

    # Filter out-of-bound values (data quality)
    df_filtered = (
        df_typed
        .filter(F.col("temperature_c").between(-89, 57))    # historic extremes °C
        .filter(F.col("windspeed_kmh") >= 0)
        .filter(F.col("precipitation_mm") >= 0)
    )

    # Deduplicate
    df_dedup = df_filtered.dropDuplicates(["date", "hour", "source"])

    # Fill nulls with column medians (simple imputation)
    median_temp  = df_dedup.approxQuantile("temperature_c",    [0.5], 0.01)[0]
    median_wind  = df_dedup.approxQuantile("windspeed_kmh",    [0.5], 0.01)[0]

    df_imputed = (
        df_dedup
        .fillna({"temperature_c":    median_temp or 15.0})
        .fillna({"precipitation_mm": 0.0})
        .fillna({"windspeed_kmh":    median_wind or 10.0})
    )

    result = df_imputed.select(
        "date", "hour", "temperature_c", "precipitation_mm",
        "windspeed_kmh", "source", "ingested_at", "execution_date",
    )

    logger.info("🌤   Weather cleaned: %d records", result.count())
    return result


def clean_crypto(df: DataFrame) -> DataFrame:
    """
    Flatten CoinDesk BPI response.
    Raw structure: { data: { bpi: { USD: {rate_float}, GBP: {...}, EUR: {...} }, time: {...} } }
    """
    logger.info("₿  Cleaning crypto data …")

    df_flat = (
        df
        .select(
            F.col("ingested_at"),
            F.col("execution_date"),
            F.col("data.bpi.USD.rate_float").cast(DoubleType()).alias("rate_usd"),
            F.col("data.bpi.GBP.rate_float").cast(DoubleType()).alias("rate_gbp"),
            F.col("data.bpi.EUR.rate_float").cast(DoubleType()).alias("rate_eur"),
            F.col("data.bpi.USD.description").alias("description"),
        )
        .withColumn("date",           F.to_date(F.lit(EXECUTION_DATE)))
        .withColumn("currency",       F.lit("BTC"))
        .withColumn("ingested_at",    F.to_timestamp("ingested_at"))
        .withColumn("execution_date", F.to_date(F.lit(EXECUTION_DATE)))
    )

    # Validate rates are positive
    df_valid = df_flat.filter(
        F.col("rate_usd") > 0
    )

    df_dedup = df_valid.dropDuplicates(["date", "currency"])

    result = df_dedup.select(
        "date", "currency", "rate_usd", "rate_gbp", "rate_eur",
        "description", "ingested_at", "execution_date",
    )

    logger.info("₿  Crypto cleaned: %d records", result.count())
    return result


def write_cleaned(df: DataFrame, dataset: str) -> None:
    """Write cleaned Parquet output with date partitioning."""
    out_path = f"{CLEANED_BASE}/{dataset}"
    logger.info("💾  Writing cleaned %s → %s", dataset, out_path)

    (
        df
        .withColumn("year",  F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day",   F.dayofmonth("date"))
        .write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .option("compression", "snappy")
        .parquet(out_path)
    )
    logger.info("✅  %s written successfully", dataset)


def compute_data_quality_metrics(df: DataFrame, dataset: str) -> dict:
    """Compute and log DQ metrics to stdout (picked up by CloudWatch Logs Insights)."""
    total = df.count()
    null_counts = {
        col: df.filter(F.col(col).isNull()).count()
        for col in df.columns
    }
    null_rates = {col: f"{(n / total * 100):.2f}%" for col, n in null_counts.items()} if total > 0 else {}

    metrics = {
        "dataset":     dataset,
        "total_rows":  total,
        "null_rates":  null_rates,
        "processed_at": PROCESSED_AT,
    }

    import json
    # Emit as a structured log line for CloudWatch Logs Insights
    print(f"DQ_METRICS: {json.dumps(metrics)}")
    logger.info("📊  DQ metrics for %s: total=%d", dataset, total)
    return metrics


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    logger.info("🚀  raw_to_cleaned started | date=%s | env=%s", EXECUTION_DATE, ENV)

    # --- Weather ---
    try:
        raw_weather = read_raw_json("weather")
        cleaned_weather = clean_weather(raw_weather)
        compute_data_quality_metrics(cleaned_weather, "weather")
        write_cleaned(cleaned_weather, "weather")
    except Exception as exc:
        logger.error("❌  Weather pipeline failed: %s", exc, exc_info=True)
        raise

    # --- Crypto ---
    try:
        raw_crypto  = read_raw_json("crypto")
        cleaned_crypto = clean_crypto(raw_crypto)
        compute_data_quality_metrics(cleaned_crypto, "crypto")
        write_cleaned(cleaned_crypto, "crypto")
    except Exception as exc:
        logger.error("❌  Crypto pipeline failed: %s", exc, exc_info=True)
        raise

    logger.info("🎉  raw_to_cleaned completed successfully")


main()
job.commit()
