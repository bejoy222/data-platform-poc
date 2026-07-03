"""
Unit tests for the Storage Path Resolver.
This is the most critical service in the platform.
A bug here breaks tenant isolation.
"""
import pytest
from src.services.path_resolver import StoragePathResolver


@pytest.fixture
def resolver():
    return StoragePathResolver()


def test_silver_path_with_country(resolver):
    path = resolver.resolve(
        tenant_id="samsung",
        layer="silver",
        object_name="Inventory",
        region="europe",
        country="germany"
    )
    assert path == "s3://silver/tenants/samsung/inventory/region=europe/country=germany/"


def test_bronze_path_includes_source_system(resolver):
    path = resolver.resolve(
        tenant_id="samsung",
        layer="bronze",
        object_name="Inventory",
        source_system="sap_erp",
        region="europe",
        country="germany"
    )
    assert "sap_erp" in path
    assert path == "s3://bronze/tenants/samsung/sap_erp/inventory/region=europe/country=germany/"


def test_gold_path_region_only(resolver):
    path = resolver.resolve(
        tenant_id="samsung",
        layer="gold",
        object_name="Inventory",
        region="europe"
    )
    assert "country" not in path
    assert path == "s3://gold/tenants/samsung/inventory/region=europe/"


def test_global_path_no_scope(resolver):
    path = resolver.resolve(
        tenant_id="samsung",
        layer="gold",
        object_name="Inventory"
    )
    assert path == "s3://gold/tenants/samsung/inventory/"


def test_tenant_isolation(resolver):
    """Critical: two tenants must never share a path prefix."""
    samsung_path = resolver.resolve("samsung", "silver", "Inventory")
    medsupply_path = resolver.resolve("medsupply", "silver", "Inventory")
    assert samsung_path != medsupply_path
    assert "samsung" in samsung_path
    assert "medsupply" in medsupply_path
    assert not samsung_path.startswith(medsupply_path)
    assert not medsupply_path.startswith(samsung_path)


def test_path_always_ends_with_slash(resolver):
    path = resolver.resolve("samsung", "silver", "Inventory")
    assert path.endswith("/")


def test_object_name_lowercased(resolver):
    path = resolver.resolve("samsung", "silver", "Inventory")
    assert "Inventory" not in path
    assert "inventory" in path


def test_invalid_layer_raises_error(resolver):
    with pytest.raises(ValueError):
        resolver.resolve("samsung", "platinum", "Inventory")


def test_time_partitions(resolver):
    path = resolver.resolve(
        tenant_id="samsung",
        layer="silver",
        object_name="Inventory",
        region="europe",
        country="germany",
        year="2024",
        month="07",
        day="02"
    )
    assert "year=2024" in path
    assert "month=07" in path
    assert "day=02" in path
