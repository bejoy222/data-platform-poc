"""
Samsung Inventory Pipeline DAG
"""
from datetime import datetime, timedelta
import subprocess
import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "platform-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

SPARK_PACKAGES_KAFKA = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

SPARK_PACKAGES_S3 = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

SPARK_CONFS = (
    "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
    "--conf spark.hadoop.fs.s3a.access.key=admin "
    "--conf spark.hadoop.fs.s3a.secret.key=changeme123456 "
    "--conf spark.hadoop.fs.s3a.path.style.access=true "
    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
    "--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
    "--conf spark.executorEnv.KAFKA_BOOTSTRAP=kafka:29092 "
    "--conf spark.executorEnv.SCHEMA_REGISTRY_URL=http://schema-registry:8000 "
    "--conf spark.executorEnv.MINIO_ENDPOINT=http://minio:9000"
)


def run_sap_simulator(**context):
    """Publish SAP inventory data to Kafka."""
    import json
    import random
    import time
    from datetime import datetime, timedelta
    from kafka import KafkaProducer

    PRODUCTS = [
        {"MATNR": "SAM-S26-256-BLK"},
        {"MATNR": "SAM-S26-512-WHT"},
        {"MATNR": "SAM-S26U-512-BLK"},
        {"MATNR": "SAM-S26U-1TB-GRN"},
        {"MATNR": "SAM-TAB-S10-256"},
        {"MATNR": "SAM-WATCH-7-BLK"},
        {"MATNR": "SAM-BUDS-3-WHT"},
        {"MATNR": "SAM-TV-65-QLED"},
        {"MATNR": "SAM-FRIDGE-XL"},
        {"MATNR": "SAM-WM-FRONT"},
    ]

    WAREHOUSES = [
        {"LGORT": "DE-BER-01", "country": "germany", "region": "europe"},
        {"LGORT": "DE-MUN-01", "country": "germany", "region": "europe"},
        {"LGORT": "UK-LON-01", "country": "uk",      "region": "europe"},
        {"LGORT": "UK-MAN-01", "country": "uk",      "region": "europe"},
        {"LGORT": "FR-PAR-01", "country": "france",  "region": "europe"},
        {"LGORT": "ES-MAD-01", "country": "spain",   "region": "europe"},
        {"LGORT": "IT-MIL-01", "country": "italy",   "region": "europe"},
    ]

    SCENARIOS = {
        ("SAM-S26U-512-BLK", "ES-MAD-01"): {"LABST": 45,    "EINME": 40},
        ("SAM-S26U-1TB-GRN", "IT-MIL-01"): {"LABST": 0,     "EINME": 0},
        ("SAM-TV-65-QLED",   "DE-BER-01"): {"LABST": 45000, "EINME": 200},
        ("SAM-FRIDGE-XL",    "UK-LON-01"): {"LABST": 52000, "EINME": 300},
    }

    producer = KafkaProducer(
        bootstrap_servers="kafka:29092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    total = 0
    for product in PRODUCTS:
        for warehouse in WAREHOUSES:
            key = (product["MATNR"], warehouse["LGORT"])
            if key in SCENARIOS:
                s = SCENARIOS[key]
                qty = s["LABST"]
                reserved = s["EINME"]
            else:
                qty = random.randint(500, 15000)
                reserved = random.randint(0, min(500, qty))

            record = {
                "MATNR": product["MATNR"],
                "LGORT": warehouse["LGORT"],
                "LABST": qty,
                "EINME": reserved,
                "LBKUM": max(0, qty - reserved),
                "DLINL": datetime.utcnow().isoformat(),
                "_source_system": "sap_erp",
                "_tenant_id": "samsung",
                "_region": warehouse["region"],
                "_country": warehouse["country"],
                "_extracted_at": datetime.utcnow().isoformat(),
                "_batch_id": f"AIRFLOW-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
            }
            producer.send("samsung.sap.inventory",
                         key=f"{record['MATNR']}-{record['LGORT']}",
                         value=record)
            total += 1

    producer.flush()
    producer.close()
    print(f"✅ SAP simulator complete — {total} records published to Kafka")


def run_spark_job(script_name, packages, **context):
    """Submit a Spark job via docker exec."""
    cmd = (
        f"docker exec spark-master /opt/spark/bin/spark-submit "
        f"--master spark://spark-master:7077 "
        f"--packages '{packages}' "
        f"{SPARK_CONFS} "
        f"/opt/spark/work-dir/{script_name}"
    )

    print(f"🚀 Submitting: {script_name}")

    result = subprocess.run(
        cmd, shell=True, capture_output=True, text=True
    )

    output = result.stdout + result.stderr
    print(output[-3000:] if len(output) > 3000 else output)

    if result.returncode != 0:
        raise Exception(f"Spark job {script_name} failed")

    print(f"✅ {script_name} complete")


with DAG(
    dag_id="samsung_inventory_pipeline",
    default_args=default_args,
    description="Samsung inventory pipeline: Bronze → Silver → Gold",
    schedule_interval="0 */6 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["samsung", "inventory", "platform"],
) as dag:

    t1_simulate = PythonOperator(
        task_id="simulate_sap_data",
        python_callable=run_sap_simulator,
    )

    t2_bronze = PythonOperator(
        task_id="bronze_landing",
        python_callable=run_spark_job,
        op_kwargs={
            "script_name": "bronze_landing.py",
            "packages": SPARK_PACKAGES_KAFKA,
        },
    )

    t3_silver = PythonOperator(
        task_id="silver_transform",
        python_callable=run_spark_job,
        op_kwargs={
            "script_name": "silver_transform.py",
            "packages": SPARK_PACKAGES_S3,
        },
    )

    t4_gold = PythonOperator(
        task_id="gold_aggregate",
        python_callable=run_spark_job,
        op_kwargs={
            "script_name": "gold_aggregate.py",
            "packages": SPARK_PACKAGES_S3,
        },
    )

    t1_simulate >> t2_bronze >> t3_silver >> t4_gold
