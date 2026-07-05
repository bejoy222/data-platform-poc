"""
SAP ERP Data Simulator
Generates realistic Samsung inventory records using SAP field names
and publishes them to Kafka topics.

SAP field names used (matching our Schema Registry source mappings):
  MATNR → product_sku
  LGORT → warehouse_code
  LABST → quantity (unrestricted stock)
  EINME → reserved_qty
  LBKUM → available_qty
  DLINL → last_counted
"""
import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer


# ── Samsung Product Catalogue ─────────────────────────────────────────────────

PRODUCTS = [
    {"MATNR": "SAM-S26-256-BLK", "name": "Galaxy S26 256GB Black"},
    {"MATNR": "SAM-S26-512-WHT", "name": "Galaxy S26 512GB White"},
    {"MATNR": "SAM-S26U-512-BLK", "name": "Galaxy S26 Ultra 512GB Black"},
    {"MATNR": "SAM-S26U-1TB-GRN", "name": "Galaxy S26 Ultra 1TB Green"},
    {"MATNR": "SAM-TAB-S10-256", "name": "Galaxy Tab S10 256GB"},
    {"MATNR": "SAM-WATCH-7-BLK", "name": "Galaxy Watch 7 Black"},
    {"MATNR": "SAM-BUDS-3-WHT", "name": "Galaxy Buds 3 White"},
    {"MATNR": "SAM-TV-65-QLED", "name": "Samsung 65 QLED TV"},
    {"MATNR": "SAM-FRIDGE-XL", "name": "Samsung XL Refrigerator"},
    {"MATNR": "SAM-WM-FRONT", "name": "Samsung Front Load Washer"},
]

# ── Samsung European Warehouses ───────────────────────────────────────────────

WAREHOUSES = [
    {"LGORT": "DE-BER-01", "country": "germany", "region": "europe", "name": "Berlin DC"},
    {"LGORT": "DE-MUN-01", "country": "germany", "region": "europe", "name": "Munich DC"},
    {"LGORT": "UK-LON-01", "country": "uk",      "region": "europe", "name": "London DC"},
    {"LGORT": "UK-MAN-01", "country": "uk",      "region": "europe", "name": "Manchester DC"},
    {"LGORT": "FR-PAR-01", "country": "france",  "region": "europe", "name": "Paris DC"},
    {"LGORT": "ES-MAD-01", "country": "spain",   "region": "europe", "name": "Madrid DC"},
    {"LGORT": "IT-MIL-01", "country": "italy",   "region": "europe", "name": "Milan DC"},
]


def make_inventory_record(product, warehouse):
    """
    Generate a realistic SAP inventory record.
    Uses SAP field names to simulate real ERP output.
    """
    quantity = random.randint(0, 25000)
    reserved = random.randint(0, max(0, quantity - 100))
    available = quantity - reserved

    # Some products are critically low to make AI queries interesting
    if random.random() < 0.15:  # 15% chance of low stock
        quantity = random.randint(0, 500)
        reserved = random.randint(0, quantity)
        available = quantity - reserved

    # Some products are overstocked
    if random.random() < 0.10:  # 10% chance of overstock
        quantity = random.randint(20000, 50000)
        reserved = random.randint(0, 1000)
        available = quantity - reserved

    last_counted = datetime.utcnow() - timedelta(
        hours=random.randint(0, 72)
    )

    return {
        # SAP field names (raw — as they arrive from SAP ERP)
        "MATNR": product["MATNR"],
        "LGORT": warehouse["LGORT"],
        "LABST": quantity,
        "EINME": reserved,
        "LBKUM": available,
        "DLINL": last_counted.isoformat(),
        # Metadata added by integration layer
        "_source_system": "sap_erp",
        "_tenant_id": "samsung",
        "_region": warehouse["region"],
        "_country": warehouse["country"],
        "_extracted_at": datetime.utcnow().isoformat(),
        "_batch_id": f"SAP-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
    }


def make_warehouse_record(warehouse):
    """Generate a SAP warehouse master record."""
    return {
        "LGORT": warehouse["LGORT"],
        "LGOBE": warehouse["name"],
        "LAND1": warehouse["country"].upper(),
        "REGIO": warehouse["region"],
        "LGTYP": random.choice(["DC", "RETAIL", "SERVICE"]),
        "CAPAC": round(random.uniform(5000, 50000), 2),
        "_source_system": "sap_erp",
        "_tenant_id": "samsung",
        "_region": warehouse["region"],
        "_country": warehouse["country"],
        "_extracted_at": datetime.utcnow().isoformat(),
    }


def make_forecast_record(product, warehouse):
    """Generate a demand forecast record."""
    period = datetime.utcnow().strftime("%Y-%m")
    base_demand = random.randint(500, 5000)

    return {
        "MATNR": product["MATNR"],
        "LGORT": warehouse["LGORT"],
        "REGION": warehouse["region"],
        "COUNTRY": warehouse["country"],
        "PERIOD": period,
        "PRED_DEMAND": base_demand,
        "CONFIDENCE": round(random.uniform(0.65, 0.95), 2),
        "MODEL_VER": "v2.1",
        "_source_system": "forecast_engine",
        "_tenant_id": "samsung",
        "_extracted_at": datetime.utcnow().isoformat(),
    }


def run_simulator(
    bootstrap_servers: str = "192.168.0.10:9092",
    num_batches: int = 5,
    delay_seconds: float = 2.0
):
    """
    Run the SAP simulator.
    Publishes inventory, warehouse, and forecast records to Kafka.
    """
    print(f"\n🚀 SAP Simulator starting...")
    print(f"   Kafka: {bootstrap_servers}")
    print(f"   Batches: {num_batches}")
    print(f"   Products: {len(PRODUCTS)}")
    print(f"   Warehouses: {len(WAREHOUSES)}")
    print(f"   Records per batch: {len(PRODUCTS) * len(WAREHOUSES)}\n")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    total_inventory = 0
    total_warehouse = 0
    total_forecast = 0

    for batch in range(1, num_batches + 1):
        print(f"📦 Batch {batch}/{num_batches}")

        # Publish warehouse records (once per batch)
        for warehouse in WAREHOUSES:
            record = make_warehouse_record(warehouse)
            producer.send(
                "samsung.sap.warehouse",
                key=record["LGORT"],
                value=record
            )
            total_warehouse += 1

        # Publish inventory records (all product/warehouse combinations)
        for product in PRODUCTS:
            for warehouse in WAREHOUSES:
                record = make_inventory_record(product, warehouse)
                producer.send(
                    "samsung.sap.inventory",
                    key=f"{record['MATNR']}-{record['LGORT']}",
                    value=record
                )
                total_inventory += 1

        # Publish forecast records
        for product in PRODUCTS:
            for warehouse in WAREHOUSES:
                record = make_forecast_record(product, warehouse)
                producer.send(
                    "samsung.sap.forecast",
                    key=f"{record['MATNR']}-{record['LGORT']}",
                    value=record
                )
                total_forecast += 1

        producer.flush()
        print(f"   ✅ Batch {batch} published")

        if batch < num_batches:
            print(f"   ⏳ Waiting {delay_seconds}s...")
            time.sleep(delay_seconds)

    producer.close()

    print(f"\n🎉 Simulation complete!")
    print(f"   Inventory records: {total_inventory}")
    print(f"   Warehouse records: {total_warehouse}")
    print(f"   Forecast records:  {total_forecast}")
    print(f"   Total records:     {total_inventory + total_warehouse + total_forecast}\n")


if __name__ == "__main__":
    run_simulator()
