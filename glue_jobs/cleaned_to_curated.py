"""
AWS Glue PySpark Job – Cleaned (Silver) → Curated (Gold)
=========================================================
Reads cleaned Parquet datasets, performs business-level aggregations,
joins cross-domain data, and writes analytics-ready Parquet to the Curated layer.

Output datasets:
  • daily_summary   – weather + crypto join, KPIs per day
  • weather_hourly_agg – hourly aggregations with moving averages
  • crypto_stats    – statistical summary of BTC rates
"""

import sys
import json
import logging
from datetime import datetime, timezone

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── Job Arguments ────────────────────────────────────────────────────────────
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "execution_date", "s3_bucket", "env"],
)
EXECUTION_DATE = args["execution_date"]
S3_BUCKET      = args["s3_bucket"]
ENV            = args["env"]
DATE_PATH      = EXECUTION_DATE.replace("-", "/")

CLEANED_BASE = f"s3://{S3_BUCKET}/cleaned"
CURATED_BASE = f"s3://{S3_BUCKET}/curated"

# ─── Spark / Glue Setup ───────────────────────────────────────────────────────
sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.adaptive.enabled",               "true")
spark.conf.set("spark.sql.shuffle.partitions",             "50")

PROCESSED_AT = F.lit(datetime.now(tz=timezone.utc).isoformat())


# ─── Read Helpers ─────────────────────────────────────────────────────────────

def read_cleaned(dataset: str) -> DataFrame:
    path = f"{CLEANED_BASE}/{dataset}"
    logger.info("📖  Reading cleaned %s from %s", dataset, path)
    df = spark.read.parquet(path)
    logger.info("✅  Loaded %d records", df.count())
    return df


def write_curated(df: DataFrame, dataset: str, partition_cols: list[str] = None) -> None:
    out_path = f"{CURATED_BASE}/{dataset}"
    logger.info("💾  Writing curated %s → %s (%d rows)", dataset, out_path, df.count())
    writer = (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(out_path)
    logger.info("✅  Curated %s written", dataset)


# ─── Transformation: Daily Summary ────────────────────────────────────────────

def build_daily_summary(weather_df: DataFrame, crypto_df: DataFrame) -> DataFrame:
    """
    Join daily weather aggregates with crypto closing prices.
    Produces one row per (date) with KPIs.
    """
    logger.info("📊  Building daily_summary …")

    weather_daily = (
        weather_df
        .groupBy("date")
        .agg(
            F.round(F.avg("temperature_c"),    2).alias("avg_temp_c"),
            F.round(F.max("temperature_c"),    2).alias("max_temp_c"),
            F.round(F.min("temperature_c"),    2).alias("min_temp_c"),
            F.round(F.stddev("temperature_c"), 4).alias("stddev_temp_c"),
            F.round(F.sum("precipitation_mm"), 3).alias("total_precip_mm"),
            F.round(F.avg("windspeed_kmh"),    2).alias("avg_windspeed_kmh"),
            F.max("windspeed_kmh").alias("max_windspeed_kmh"),
            F.count("*").alias("hourly_records"),
        )
    )

    crypto_daily = (
        crypto_df
        .groupBy("date")
        .agg(
            F.round(F.avg("rate_usd"), 2).alias("btc_usd_avg"),
            F.round(F.max("rate_usd"), 2).alias("btc_usd_high"),
            F.round(F.min("rate_usd"), 2).alias("btc_usd_low"),
            F.round(F.avg("rate_gbp"), 2).alias("btc_gbp_avg"),
            F.round(F.avg("rate_eur"), 2).alias("btc_eur_avg"),
        )
    )

    summary = (
        weather_daily
        .join(crypto_daily, on="date", how="left")
        .withColumn("processed_at", PROCESSED_AT)
        .withColumn("pipeline_version", F.lit("1.0.0"))
        .orderBy("date")
    )

    return summary


# ─── Transformation: Weather Hourly Aggregations ──────────────────────────────

def build_weather_hourly_agg(weather_df: DataFrame) -> DataFrame:
    """
    Enrich hourly weather with rolling averages and heat-index approximation.
    """
    logger.info("🌤   Building weather_hourly_agg …")

    window_6h  = Window.partitionBy("date").orderBy("hour").rowsBetween(-5, 0)
    window_24h = Window.orderBy(F.col("date").cast("long"), "hour").rowsBetween(-23, 0)

    df_enriched = (
        weather_df
        .withColumn("temp_6h_rolling_avg",  F.round(F.avg("temperature_c").over(window_6h),  2))
        .withColumn("temp_24h_rolling_avg", F.round(F.avg("temperature_c").over(window_24h), 2))
        .withColumn("wind_6h_rolling_avg",  F.round(F.avg("windspeed_kmh").over(window_6h),  2))
        # Simplified heat index (for demo purposes)
        .withColumn(
            "heat_index_c",
            F.round(
                F.col("temperature_c")
                + (0.33 * F.lit(40))          # approximate humidity constant
                - (0.7 * F.col("windspeed_kmh") / F.lit(3.6))
                - F.lit(4.0),
                2,
            ),
        )
        .withColumn("is_rainy",    (F.col("precipitation_mm") > 0).cast("boolean"))
        .withColumn("is_windy",    (F.col("windspeed_kmh") > 30).cast("boolean"))
        .withColumn("processed_at", PROCESSED_AT)
    )

    return df_enriched


# ─── Transformation: Crypto Stats ─────────────────────────────────────────────

def build_crypto_stats(crypto_df: DataFrame) -> DataFrame:
    """
    Daily descriptive statistics and simple momentum indicators for BTC/USD.
    """
    logger.info("₿  Building crypto_stats …")

    window_7d = Window.orderBy("date").rowsBetween(-6, 0)

    df_stats = (
        crypto_df
        .withColumn("rate_usd_7d_avg",    F.round(F.avg("rate_usd").over(window_7d), 2))
        .withColumn("rate_usd_7d_stddev", F.round(F.stddev("rate_usd").over(window_7d), 4))
        .withColumn(
            "price_change_pct",
            F.round(
                (F.col("rate_usd") - F.lag("rate_usd", 1).over(Window.orderBy("date")))
                / F.lag("rate_usd", 1).over(Window.orderBy("date"))
                * 100,
                4,
            ),
        )
        .withColumn(
            "volatility_band_upper",
            F.round(F.col("rate_usd_7d_avg") + 2 * F.col("rate_usd_7d_stddev"), 2),
        )
        .withColumn(
            "volatility_band_lower",
            F.round(F.col("rate_usd_7d_avg") - 2 * F.col("rate_usd_7d_stddev"), 2),
        )
        .withColumn("processed_at", PROCESSED_AT)
    )

    return df_stats


# ─── Data Quality Report ──────────────────────────────────────────────────────

def emit_curated_metrics(daily_summary: DataFrame) -> None:
    """Emit structured metrics for CloudWatch Logs Insights."""
    row_count = daily_summary.count()
    metrics = {
        "stage":           "cleaned_to_curated",
        "execution_date":  EXECUTION_DATE,
        "daily_summary_rows": row_count,
        "processed_at":    datetime.now(tz=timezone.utc).isoformat(),
    }
    print(f"CURATED_METRICS: {json.dumps(metrics)}")
    logger.info("📊  Curated metrics emitted: %s", metrics)


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    logger.info("🚀  cleaned_to_curated started | date=%s | env=%s", EXECUTION_DATE, ENV)

    # Read cleaned layers
    weather_df = read_cleaned("weather")
    crypto_df  = read_cleaned("crypto")

    # Build curated datasets
    daily_summary     = build_daily_summary(weather_df, crypto_df)
    weather_hourly    = build_weather_hourly_agg(weather_df)
    crypto_stats      = build_crypto_stats(crypto_df)

    # Emit metrics before write
    emit_curated_metrics(daily_summary)

    # Write curated outputs
    write_curated(daily_summary,  "daily_summary",       partition_cols=["date"])
    write_curated(weather_hourly, "weather_hourly_agg",  partition_cols=["date"])
    write_curated(crypto_stats,   "crypto_stats",        partition_cols=["date"])

    logger.info("🎉  cleaned_to_curated completed successfully for %s", EXECUTION_DATE)


main()
job.commit()
