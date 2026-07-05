"""
Gold Aggregation Job
====================
Reads Silver Parquet, joins with warehouse data,
calculates coverage days, writes to Gold layer.

Gold layer powers all API and AI queries.
This is where business logic lives.
"""
import os
import requests
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, round as spark_round,
    when, sum as spark_sum, avg, max as spark_max,
    min as spark_min, count, datediff, to_date
)


# ── Configuration ─────────────────────────────────────────────────────────────

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8000")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "changeme123456")

TENANT_ID = "samsung"


# ── Spark Session ──────────────────────────────────────────────────────────────

def create_spark_session():
    return (
        SparkSession.builder
        .appName("GoldAggregate-Samsung-Inventory")
        .master("spark://spark-master:7077")
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


# ── Main ───────────────────────────────────────────────────────────────────────

def run():
    print(f"\n🚀 Gold Aggregation Job starting...")
    print(f"   Tenant: {TENANT_ID}\n")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Read full Silver inventory layer
    silver_path = f"s3a://silver/tenants/{TENANT_ID}/inventory/"
    print(f"📥 Reading Silver layer: {silver_path}")

    silver_df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .parquet(silver_path)
    )

    total_silver = silver_df.count()
    print(f"   Found {total_silver} Silver records\n")

    if total_silver == 0:
        print("⚠️  No Silver records found. Run silver_transform.py first.")
        spark.stop()
        return

    # ── Calculate Coverage Days ────────────────────────────────────────────
    # Coverage days = quantity / average daily demand
    # We simulate daily demand as quantity / 30 for POC
    # In production this comes from the Forecast object

    print("🔄 Calculating inventory health metrics...")

    inventory_health = silver_df.withColumn(
        "daily_demand_estimate",
        when(col("quantity") > 0, col("quantity") / 30.0).otherwise(0)
    ).withColumn(
        "coverage_days",
        when(
            col("daily_demand_estimate") > 0,
            spark_round(col("quantity") / col("daily_demand_estimate"), 1)
        ).otherwise(lit(0.0))
    ).withColumn(
        "stock_status",
        when(col("quantity") == 0, "OUT_OF_STOCK")
        .when(col("coverage_days") <= 7, "CRITICAL")
        .when(col("coverage_days") <= 14, "LOW")
        .when(col("coverage_days") >= 90, "OVERSTOCKED")
        .otherwise("HEALTHY")
    ).withColumn(
        "risk_flag",
        when(col("coverage_days") <= 7, True).otherwise(False)
    )

    # ── Regional Summary ───────────────────────────────────────────────────
    print("🔄 Building regional summary...")

    regional_summary = inventory_health.groupBy(
        "_region", "_country", "product_sku"
    ).agg(
        spark_sum("quantity").alias("total_quantity"),
        spark_sum("available_qty").alias("total_available"),
        spark_sum("reserved_qty").alias("total_reserved"),
        avg("coverage_days").alias("avg_coverage_days"),
        spark_min("coverage_days").alias("min_coverage_days"),
        spark_max("coverage_days").alias("max_coverage_days"),
        count("warehouse_code").alias("warehouse_count"),
        spark_sum(
            when(col("stock_status") == "CRITICAL", 1).otherwise(0)
        ).alias("critical_warehouses"),
        spark_sum(
            when(col("stock_status") == "OVERSTOCKED", 1).otherwise(0)
        ).alias("overstocked_warehouses"),
    ).withColumn(
        "region_stock_status",
        when(col("min_coverage_days") <= 7, "CRITICAL")
        .when(col("min_coverage_days") <= 14, "LOW")
        .when(col("avg_coverage_days") >= 90, "OVERSTOCKED")
        .otherwise("HEALTHY")
    ).withColumn(
        "_gold_processed_at", current_timestamp()
    ).withColumn(
        "_gold_job", lit("gold_aggregate")
    )

    # ── Write Gold Layer ───────────────────────────────────────────────────
    print("\n   Writing Gold layer...\n")

    # Inventory health — full record level
    gold_inventory_path = f"s3a://gold/tenants/{TENANT_ID}/inventory_health/"
    print(f"   ✅ Writing inventory health → {gold_inventory_path}")
    inventory_health.write.mode("overwrite").parquet(gold_inventory_path)

    # Regional summary
    gold_regional_path = f"s3a://gold/tenants/{TENANT_ID}/regional_summary/"
    print(f"   ✅ Writing regional summary → {gold_regional_path}")
    regional_summary.write.mode("overwrite").parquet(gold_regional_path)

    # ── Stock Risk Report — answers the AI questions ───────────────────────
    print("\n🤖 Generating stock risk report (AI query preview)...\n")

    # Question 1: Which warehouses risk stock-out in 7 days?
    critical = inventory_health.filter(
        col("stock_status").isin("CRITICAL", "OUT_OF_STOCK")
    ).select(
        "product_sku", "warehouse_code", "_country",
        "quantity", "coverage_days", "stock_status"
    ).orderBy("coverage_days")

    print("⚠️  CRITICAL STOCK POSITIONS (< 7 days coverage):")
    critical_count = critical.count()
    if critical_count > 0:
        critical.show(10, truncate=False)
    else:
        print("   None — all positions healthy")

    # Question 2: Which products are overstocked?
    overstocked = inventory_health.filter(
        col("stock_status") == "OVERSTOCKED"
    ).groupBy("product_sku", "_region").agg(
        spark_sum("quantity").alias("total_qty"),
        avg("coverage_days").alias("avg_coverage_days"),
        count("warehouse_code").alias("warehouses")
    ).orderBy("avg_coverage_days", ascending=False)

    print("\n📦 OVERSTOCKED POSITIONS (> 90 days coverage):")
    overstocked_count = overstocked.count()
    if overstocked_count > 0:
        overstocked.show(10, truncate=False)
    else:
        print("   None — no overstocked positions")

    # Write stock risk report to Gold
    gold_risk_path = f"s3a://gold/tenants/{TENANT_ID}/stock_risk/"
    inventory_health.filter(
        col("stock_status").isin("CRITICAL", "OUT_OF_STOCK", "OVERSTOCKED")
    ).write.mode("overwrite").parquet(gold_risk_path)

    print(f"\n🎉 Gold aggregation complete!")
    print(f"   inventory_health: {total_silver} records")
    print(f"   regional_summary: {regional_summary.count()} records")
    print(f"   stock_risk: critical={critical_count}, overstocked={overstocked_count}")
    print(f"   Layer: Gold (query-ready)")
    print(f"   Format: Parquet\n")

    spark.stop()


if __name__ == "__main__":
    run()
