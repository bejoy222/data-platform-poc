"""
Seed script — Samsung tenant full data model.
Run this once after the Schema Registry is up.
"""
import httpx
import json

BASE_URL = "http://localhost:8000/api/v1"


def post(path, data):
    r = httpx.post(f"{BASE_URL}{path}", json=data)
    if r.status_code not in (200, 201):
        print(f"⚠️  {path}: {r.status_code} — {r.text}")
        return None
    result = r.json()
    print(f"✅ {path}: {result.get('id') or result.get('name') or 'ok'}")
    return result


def get(path):
    r = httpx.get(f"{BASE_URL}{path}")
    return r.json()


def seed():
    print("\n🚀 Seeding Samsung tenant data...\n")

    # ── Org Units ──────────────────────────────────────────────────────────
    print("--- Organisational Hierarchy ---")

    org_units = [
        # Global
        {"id": "samsung-global", "name": "Samsung Global", "level": "global"},

        # Regions
        {"id": "samsung-europe", "name": "Samsung Europe", "level": "region",
         "parent_id": "samsung-global", "region": "europe"},
        {"id": "samsung-apac", "name": "Samsung Asia Pacific", "level": "region",
         "parent_id": "samsung-global", "region": "asia-pacific"},
        {"id": "samsung-americas", "name": "Samsung Americas", "level": "region",
         "parent_id": "samsung-global", "region": "americas"},

        # European countries
        {"id": "samsung-germany", "name": "Samsung Germany", "level": "country",
         "parent_id": "samsung-europe", "region": "europe", "country": "germany"},
        {"id": "samsung-uk", "name": "Samsung UK", "level": "country",
         "parent_id": "samsung-europe", "region": "europe", "country": "uk"},
        {"id": "samsung-france", "name": "Samsung France", "level": "country",
         "parent_id": "samsung-europe", "region": "europe", "country": "france"},
        {"id": "samsung-spain", "name": "Samsung Spain", "level": "country",
         "parent_id": "samsung-europe", "region": "europe", "country": "spain"},
        {"id": "samsung-italy", "name": "Samsung Italy", "level": "country",
         "parent_id": "samsung-europe", "region": "europe", "country": "italy"},
    ]

    for unit in org_units:
        post(f"/tenants/samsung/org-units", unit)

    # ── Business Objects ───────────────────────────────────────────────────
    print("\n--- Business Objects ---")

    objects = [
        {
            "name": "Product",
            "description": "Samsung product catalogue",
            "fields": [
                {"name": "sku", "data_type": "string", "is_required": True, "is_unique_key": True, "description": "Stock keeping unit"},
                {"name": "name", "data_type": "string", "is_required": True},
                {"name": "category", "data_type": "string", "is_required": True},
                {"name": "brand", "data_type": "string", "is_required": True},
                {"name": "model_year", "data_type": "integer", "is_required": False},
                {"name": "unit_cost", "data_type": "float", "is_required": False},
            ]
        },
        {
            "name": "Warehouse",
            "description": "Samsung warehouse locations",
            "fields": [
                {"name": "code", "data_type": "string", "is_required": True, "is_unique_key": True},
                {"name": "name", "data_type": "string", "is_required": True},
                {"name": "country", "data_type": "string", "is_required": True},
                {"name": "region", "data_type": "string", "is_required": True},
                {"name": "type", "data_type": "string", "is_required": True, "description": "DC, retail, service"},
                {"name": "capacity_m3", "data_type": "float", "is_required": False},
                {"name": "is_active", "data_type": "boolean", "is_required": False},
            ]
        },
        {
            "name": "Inventory",
            "description": "Stock levels per product per warehouse",
            "fields": [
                {"name": "product_sku", "data_type": "string", "is_required": True, "is_unique_key": True},
                {"name": "warehouse_code", "data_type": "string", "is_required": True, "is_unique_key": True},
                {"name": "quantity", "data_type": "integer", "is_required": True},
                {"name": "reserved_qty", "data_type": "integer", "is_required": False, "default_value": "0"},
                {"name": "available_qty", "data_type": "integer", "is_required": False},
                {"name": "last_counted", "data_type": "timestamp", "is_required": False},
                {"name": "coverage_days", "data_type": "float", "is_required": False},
            ]
        },
        {
            "name": "Shipment",
            "description": "In-transit stock movements",
            "fields": [
                {"name": "id", "data_type": "string", "is_required": True, "is_unique_key": True},
                {"name": "origin_warehouse", "data_type": "string", "is_required": True},
                {"name": "destination_warehouse", "data_type": "string", "is_required": True},
                {"name": "product_sku", "data_type": "string", "is_required": True},
                {"name": "quantity", "data_type": "integer", "is_required": True},
                {"name": "container_id", "data_type": "string", "is_required": False},
                {"name": "eta", "data_type": "timestamp", "is_required": False},
                {"name": "status", "data_type": "string", "is_required": False},
            ]
        },
        {
            "name": "PurchaseOrder",
            "description": "Orders placed with suppliers",
            "fields": [
                {"name": "po_number", "data_type": "string", "is_required": True, "is_unique_key": True},
                {"name": "supplier_id", "data_type": "string", "is_required": True},
                {"name": "product_sku", "data_type": "string", "is_required": True},
                {"name": "quantity", "data_type": "integer", "is_required": True},
                {"name": "expected_delivery", "data_type": "timestamp", "is_required": False},
                {"name": "status", "data_type": "string", "is_required": False},
                {"name": "country", "data_type": "string", "is_required": True},
            ]
        },
        {
            "name": "Forecast",
            "description": "Demand forecasts per product per region",
            "fields": [
                {"name": "product_sku", "data_type": "string", "is_required": True, "is_unique_key": True},
                {"name": "region", "data_type": "string", "is_required": True, "is_unique_key": True},
                {"name": "country", "data_type": "string", "is_required": True, "is_unique_key": True},
                {"name": "period", "data_type": "string", "is_required": True, "is_unique_key": True},
                {"name": "predicted_demand", "data_type": "integer", "is_required": True},
                {"name": "confidence", "data_type": "float", "is_required": False},
                {"name": "model_version", "data_type": "string", "is_required": False},
            ]
        },
    ]

    for obj in objects:
        post(f"/tenants/samsung/objects", obj)

    # ── Source Mappings ────────────────────────────────────────────────────
    print("\n--- Source Mappings (SAP ERP → Inventory) ---")

    sap_mappings = [
        {"source_system": "sap_erp", "source_field": "MATNR", "target_field": "product_sku"},
        {"source_system": "sap_erp", "source_field": "LGORT", "target_field": "warehouse_code"},
        {"source_system": "sap_erp", "source_field": "LABST", "target_field": "quantity"},
        {"source_system": "sap_erp", "source_field": "EINME", "target_field": "reserved_qty"},
        {"source_system": "sap_erp", "source_field": "LBKUM", "target_field": "available_qty"},
        {"source_system": "sap_erp", "source_field": "DLINL", "target_field": "last_counted"},
    ]

    for mapping in sap_mappings:
        post(f"/tenants/samsung/objects/Inventory/mappings", mapping)

    # ── Verify Everything ──────────────────────────────────────────────────
    print("\n--- Verification ---")

    tenants = get("/tenants/")
    print(f"✅ Tenants registered: {len(tenants)}")

    org_units_registered = get("/tenants/samsung/org-units")
    print(f"✅ Org units registered: {len(org_units_registered)}")

    objects_registered = get("/tenants/samsung/objects")
    print(f"✅ Business objects registered: {len(objects_registered)}")
    for obj in objects_registered:
        print(f"   → {obj['name']} ({len(obj['fields'])} fields)")

    # ── Test Path Resolver ─────────────────────────────────────────────────
    print("\n--- Storage Path Resolution ---")

    paths_to_test = [
        {"layer": "bronze", "object_name": "Inventory", "source_system": "sap_erp",
         "region": "europe", "country": "germany"},
        {"layer": "silver", "object_name": "Inventory",
         "region": "europe", "country": "uk"},
        {"layer": "gold", "object_name": "Inventory",
         "region": "europe"},
    ]

    for p in paths_to_test:
        params = "&".join([f"{k}={v}" for k, v in p.items()])
        r = httpx.get(f"{BASE_URL}/tenants/samsung/objects/Inventory/path?{params}")
        if r.status_code == 200:
            print(f"✅ {p['layer'].upper()}: {r.json()['path']}")
        else:
            print(f"⚠️  {p['layer']}: {r.text}")

    print("\n🎉 Samsung seeding complete!\n")


if __name__ == "__main__":
    seed()
