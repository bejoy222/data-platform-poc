"""
Silver Transformation Job
=========================
Reads raw Bronze Parquet, applies Schema Registry field mappings,
validates and types the data, writes to Silver layer.

Bronze → Silver transformation rules:
- SAP field names replaced with Samsung business object field names
- Data types enforced (string, integer, float, timestamp)
- Null required fields filtered out
- Deduplication applied (keep latest per unique key)
- Tenant isolation maintained throughout
"""
import os
import requests
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp,
    to_timestamp, coalesce
)
from pyspark.sql.types import IntegerType, FloatType, StringType


# ── Configuration ─────────────────────────────────────────────────────────────

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8000")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "changeme123456")

TENANT_ID = "samsung"
OBJECT_NAME = "Inventory"
SOURCE_SYSTEM = "sap_erp"


# ── Schema Registry Client ─────────────────────────────────────────────────────

def get_storage_path(layer, region=None, country=None):
    """Get scoped storage path from Schema Registry."""
    params = {"layer": layer}
    if SOURCE_SYSTEM and layer == "bronze":
        params["source_system"] = SOURCE_SYSTEM
    if region:
        params["region"] = region
    if country:
        params["country"] = country

    now = datetime.utcnow()
    params.update({
        "year": now.strftime("%Y"),
        "month": now.strftime("%m"),
        "day": now.strftime("%d"),
    })

    url = f"{SCHEMA_REGISTRY_URL}/api/v1/tenants/{TENANT_ID}/objects/{OBJECT_NAME}/path"
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json()["path"]


def get_field_mappings():
    """
    Get SAP → Samsung field mappings from Schema Registry.
    Returns dict: {source_field: target_field}
    """
    url = f"{SCHEMA_REGISTRY_URL}/api/v1/tenants/{TENANT_ID}/objects/{OBJECT_NAME}/mappings"
    r = requests.get(url, params={"source_system": SOURCE_SYSTEM}, timeout=10)
    r.raise_for_status()
    mappings = r.json()
    return {m["source_field"]: m["target_field"] for m in mappings}


def get_object_fields():
    """Get Samsung business object field definitions."""
    url = f"{SCHEMA_REGISTRY_URL}/api/v1/tenants/{TENANT_ID}/objects/{OBJECT_NAME}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return {f["name"]: f for f in r.json()["fields"]}


# ── Spark Session ──────────────────────────────────────────────────────────────

def create_spark_session():
    return (
        SparkSession.builder
        .appName("SilverTransform-Samsung-Inventory")
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
    print(f"\n🚀 Silver Transform Job starting...")
    print(f"   Tenant: {TENANT_ID}")
    print(f"   Object: {OBJECT_NAME}")
    print(f"   Source: {SOURCE_SYSTEM}\n")

    # Get mappings from Schema Registry
    print("📋 Fetching field mappings from Schema Registry...")
    field_mappings = get_field_mappings()
    object_fields = get_object_fields()

    print(f"   Found {len(field_mappings)} field mappings:")
    for src, tgt in field_mappings.items():
        print(f"   {src} → {tgt}")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Read entire Bronze layer for this tenant/object
    bronze_base = f"s3a://bronze/tenants/{TENANT_ID}/{SOURCE_SYSTEM}/inventory/"
    print(f"\n📥 Reading Bronze layer: {bronze_base}")

    bronze_df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .parquet(bronze_base)
    )

    total_bronze = bronze_df.count()
    print(f"   Found {total_bronze} Bronze records\n")

    if total_bronze == 0:
        print("⚠️  No Bronze records found. Run bronze_landing.py first.")
        spark.stop()
        return

    # Apply field mappings — rename SAP fields to Samsung business object fields
    print("🔄 Applying field mappings...")
    mapped_df = bronze_df

    for source_field, target_field in field_mappings.items():
        if source_field in bronze_df.columns:
            mapped_df = mapped_df.withColumnRenamed(source_field, target_field)

    # Apply data types based on object field definitions
    print("🔄 Applying data types...")
    for field_name, field_def in object_fields.items():
        if field_name in mapped_df.columns:
            data_type = field_def["data_type"]
            if data_type == "integer":
                mapped_df = mapped_df.withColumn(
                    field_name, col(field_name).cast(IntegerType())
                )
            elif data_type == "float":
                mapped_df = mapped_df.withColumn(
                    field_name, col(field_name).cast(FloatType())
                )
            elif data_type == "timestamp":
                mapped_df = mapped_df.withColumn(
                    field_name, to_timestamp(col(field_name))
                )

    # Calculate available quantity
    mapped_df = mapped_df.withColumn(
        "available_qty",
        coalesce(col("available_qty"), col("quantity") - col("reserved_qty"))
    )

    # Filter out nulls on required fields
    print("🔄 Filtering invalid records...")
    clean_df = mapped_df.filter(
        col("product_sku").isNotNull() &
        col("warehouse_code").isNotNull() &
        col("quantity").isNotNull()
    )

    # Deduplicate — keep one record per product/warehouse combination
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, desc

    window = Window.partitionBy(
        "product_sku", "warehouse_code"
    ).orderBy(desc("_extracted_at"))

    deduped_df = (
        clean_df
        .withColumn("_rank", row_number().over(window))
        .filter(col("_rank") == 1)
        .drop("_rank")
    )

    # Add Silver metadata
    silver_df = deduped_df.withColumn(
        "_silver_processed_at", current_timestamp()
    ).withColumn(
        "_silver_job", lit("silver_transform")
    ).withColumn(
        "_object_name", lit(OBJECT_NAME.lower())
    )

    # Get unique countries in this dataset
    countries = (
        silver_df
        .select("_region", "_country")
        .distinct()
        .collect()
    )

    print(f"\n   Writing to {len(countries)} country partitions...\n")

    total_written = 0
    for row in countries:
        region = row["_region"]
        country = row["_country"]

        silver_path = get_storage_path("silver", region, country)
        s3a_path = silver_path.replace("s3://", "s3a://")

        country_df = silver_df.filter(
            (col("_region") == region) &
            (col("_country") == country)
        )

        count = country_df.count()
        print(f"   ✅ Writing {count} records → {silver_path}")

        country_df.write.mode("overwrite").parquet(s3a_path)
        total_written += count

    print(f"\n🎉 Silver transformation complete!")
    print(f"   Total records written: {total_written}")
    print(f"   Field mappings applied: {len(field_mappings)}")
    print(f"   Layer: Silver (validated, mapped)")
    print(f"   Format: Parquet\n")

    spark.stop()


if __name__ == "__main__":
    run()
