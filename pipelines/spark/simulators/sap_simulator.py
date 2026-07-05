"""
SAP ERP Data Simulator — with guaranteed interesting stock positions
"""
import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer


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

WAREHOUSES = [
    {"LGORT": "DE-BER-01", "country": "germany", "region": "europe", "name": "Berlin DC"},
    {"LGORT": "DE-MUN-01", "country": "germany", "region": "europe", "name": "Munich DC"},
    {"LGORT": "UK-LON-01", "country": "uk",      "region": "europe", "name": "London DC"},
    {"LGORT": "UK-MAN-01", "country": "uk",      "region": "europe", "name": "Manchester DC"},
    {"LGORT": "FR-PAR-01", "country": "france",  "region": "europe", "name": "Paris DC"},
    {"LGORT": "ES-MAD-01", "country": "spain",   "region": "europe", "name": "Madrid DC"},
    {"LGORT": "IT-MIL-01", "country": "italy",   "region": "europe", "name": "Milan DC"},
]

# Guaranteed interesting stock scenarios for AI queries
SCENARIOS = {
    # Critical: Galaxy S26 Ultra nearly out of stock in Spain and Italy
    ("SAM-S26U-512-BLK", "ES-MAD-01"): {"LABST": 45,   "EINME": 40},
    ("SAM-S26U-512-BLK", "IT-MIL-01"): {"LABST": 120,  "EINME": 100},
    ("SAM-S26U-1TB-GRN", "ES-MAD-01"): {"LABST": 30,   "EINME": 25},
    ("SAM-S26U-1TB-GRN", "IT-MIL-01"): {"LABST": 0,    "EINME": 0},
    # Overstocked: TVs in Germany
    ("SAM-TV-65-QLED",  "DE-BER-01"): {"LABST": 45000, "EINME": 200},
    ("SAM-TV-65-QLED",  "DE-MUN-01"): {"LABST": 38000, "EINME": 150},
    # Overstocked: Fridges in UK
    ("SAM-FRIDGE-XL",   "UK-LON-01"): {"LABST": 52000, "EINME": 300},
    ("SAM-FRIDGE-XL",   "UK-MAN-01"): {"LABST": 41000, "EINME": 200},
}


def make_inventory_record(product, warehouse):
    scenario_key = (product["MATNR"], warehouse["LGORT"])
    if scenario_key in SCENARIOS:
        s = SCENARIOS[scenario_key]
        quantity = s["LABST"]
        reserved = s["EINME"]
    else:
        quantity = random.randint(500, 15000)
        reserved = random.randint(0, min(500, quantity))

    available = max(0, quantity - reserved)
    last_counted = datetime.utcnow() - timedelta(hours=random.randint(0, 48))

    return {
        "MATNR": product["MATNR"],
        "LGORT": warehouse["LGORT"],
        "LABST": quantity,
        "EINME": reserved,
        "LBKUM": available,
        "DLINL": last_counted.isoformat(),
        "_source_system": "sap_erp",
        "_tenant_id": "samsung",
        "_region": warehouse["region"],
        "_country": warehouse["country"],
        "_extracted_at": datetime.utcnow().isoformat(),
        "_batch_id": f"SAP-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
    }


def make_warehouse_record(warehouse):
    return {
        "LGORT": warehouse["LGORT"],
        "LGOBE": warehouse["name"],
        "LAND1": warehouse["country"].upper(),
        "REGIO": warehouse["region"],
        "LGTYP": "DC",
        "CAPAC": round(random.uniform(10000, 50000), 2),
        "_source_system": "sap_erp",
        "_tenant_id": "samsung",
        "_region": warehouse["region"],
        "_country": warehouse["country"],
        "_extracted_at": datetime.utcnow().isoformat(),
    }


def run_simulator(
    bootstrap_servers="192.168.0.10:9092",
    num_batches=3,
    delay_seconds=1.0
):
    print(f"\n🚀 SAP Simulator starting (with interesting scenarios)...")
    print(f"   Kafka: {bootstrap_servers}")
    print(f"   Batches: {num_batches}\n")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    total = 0
    for batch in range(1, num_batches + 1):
        print(f"📦 Batch {batch}/{num_batches}")
        for warehouse in WAREHOUSES:
            producer.send("samsung.sap.warehouse",
                         key=warehouse["LGORT"],
                         value=make_warehouse_record(warehouse))
        for product in PRODUCTS:
            for warehouse in WAREHOUSES:
                record = make_inventory_record(product, warehouse)
                producer.send("samsung.sap.inventory",
                             key=f"{record['MATNR']}-{record['LGORT']}",
                             value=record)
                total += 1
        producer.flush()
        print(f"   ✅ Batch {batch} published")
        if batch < num_batches:
            time.sleep(delay_seconds)

    producer.close()
    print(f"\n🎉 Done! {total} inventory records published\n")


if __name__ == "__main__":
    run_simulator()
