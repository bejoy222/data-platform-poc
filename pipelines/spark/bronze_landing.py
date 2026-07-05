"""
Bronze Landing Job
==================
Reads raw inventory records from Kafka and writes them as Parquet
to the Bronze layer on MinIO (S3-compatible).

This job:
1. Consults the Schema Registry for the correct storage path
2. Reads from Kafka topic: samsung.sap.inventory
3. Writes immutable raw Parquet to Bronze layer
4. Partitions by region and country for query performance

Bronze layer rule: data is NEVER modified after landing.
Raw SAP field names are preserved exactly as received.
"""
import json
import os
import requests
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, lit, current_timestamp,
    year, month, dayofmonth
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, FloatType, TimestampType
)


# ── Configuration ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8000")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "changeme123456")

TENANT_ID = "samsung"
OBJECT_NAME = "Inventory"
SOURCE_SYSTEM = "sap_erp"
TOPIC = "samsung.sap.inventory"


# ── Schema for incoming SAP records ───────────────────────────────────────────

SAP_INVENTORY_SCHEMA = StructType([
    StructField("MATNR", StringType(), True),      # Product SKU
    StructField("LGORT", StringType(), True),      # Warehouse code
    StructField("LABST", IntegerType(), True),     # Quantity
    StructField("EINME", IntegerType(), True),     # Reserved qty
    StructField("LBKUM", IntegerType(), True),     # Available qty
    StructField("DLINL", StringType(), True),      # Last counted
    StructField("_source_system", StringType(), True),
    StructField("_tenant_id", StringType(), True),
    StructField("_region", StringType(), True),
    StructField("_country", StringType(), True),
    StructField("_extracted_at", StringType(), True),
    StructField("_batch_id", StringType(), True),
])


def get_bronze_path(region: str, country: str) -> str:
    """
    Get the Bronze storage path from Schema Registry.
    Never construct paths manually — always ask the Schema Registry.
    """
    now = datetime.utcnow()
    params = {
        "layer": "bronze",
        "source_system": SOURCE_SYSTEM,
        "region": region,
        "country": country,
        "year": now.strftime("%Y"),
        "month": now.strftime("%m"),
        "day": now.strftime("%d"),
    }
    url = f"{SCHEMA_REGISTRY_URL}/api/v1/tenants/{TENANT_ID}/objects/{OBJECT_NAME}/path"
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()["path"]


def create_spark_session() -> SparkSession:
    """Create Spark session with MinIO S3 configuration."""
    return (
        SparkSession.builder
        .appName("BronzeLanding-Samsung-Inventory")
        .master("spark://spark-master:7077")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
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


def run():
    """Main job execution."""
    print(f"\n🚀 Bronze Landing Job starting...")
    print(f"   Tenant: {TENANT_ID}")
    print(f"   Topic: {TOPIC}")
    print(f"   Kafka: {KAFKA_BOOTSTRAP}")
    print(f"   MinIO: {MINIO_ENDPOINT}\n")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka — batch mode (reads all available messages)
    print("📥 Reading from Kafka...")
    raw_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    total_records = raw_df.count()
    print(f"   Found {total_records} records in Kafka\n")

    if total_records == 0:
        print("⚠️  No records found. Exiting.")
        spark.stop()
        return

    # Parse JSON from Kafka value
    print("🔄 Parsing records...")
    parsed_df = (
        raw_df
        .select(
            from_json(
                col("value").cast("string"),
                SAP_INVENTORY_SCHEMA
            ).alias("data")
        )
        .select("data.*")
        .filter(col("MATNR").isNotNull())
        .filter(col("LGORT").isNotNull())
    )

    # Add landing metadata
    landed_df = parsed_df.withColumn(
        "_landed_at", current_timestamp()
    ).withColumn(
        "_job_name", lit("bronze_landing")
    )

    # Get unique countries in this batch
    countries = (
        landed_df
        .select("_region", "_country")
        .distinct()
        .collect()
    )

    print(f"   Writing to {len(countries)} country partitions...\n")

    total_written = 0
    for row in countries:
        region = row["_region"]
        country = row["_country"]

        # Get path from Schema Registry
        path = get_bronze_path(region, country)

        # Convert s3:// to s3a:// for Hadoop
        s3a_path = path.replace("s3://", "s3a://")

        # Filter to this country
        country_df = landed_df.filter(
            (col("_region") == region) &
            (col("_country") == country)
        )

        count = country_df.count()

        print(f"   ✅ Writing {count} records → {path}")

        # Write as Parquet — immutable Bronze layer
        (
            country_df
            .write
            .mode("append")
            .parquet(s3a_path)
        )

        total_written += count

    print(f"\n🎉 Bronze landing complete!")
    print(f"   Total records written: {total_written}")
    print(f"   Layer: Bronze (immutable)")
    print(f"   Format: Parquet\n")

    spark.stop()


if __name__ == "__main__":
    run()
