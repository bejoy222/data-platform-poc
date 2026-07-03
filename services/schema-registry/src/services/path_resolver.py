from typing import Optional
from src.core.config import settings


class StoragePathResolver:
    """
    Resolves s3:// storage paths for tenant data.
    This is the core service that enforces tenant isolation.
    No service should ever construct a storage path directly.
    Always call this resolver.
    """

    LAYER_BUCKETS = {
        "bronze": settings.BRONZE_BUCKET,
        "silver": settings.SILVER_BUCKET,
        "gold": settings.GOLD_BUCKET,
    }

    def resolve(
        self,
        tenant_id: str,
        layer: str,
        object_name: str,
        source_system: Optional[str] = None,
        region: Optional[str] = None,
        country: Optional[str] = None,
        year: Optional[str] = None,
        month: Optional[str] = None,
        day: Optional[str] = None,
    ) -> str:
        """
        Resolve a scoped s3:// path for the given tenant and scope.

        Examples:
            Bronze:
            s3://bronze/tenants/samsung/sap_erp/inventory/region=europe/country=germany/year=2024/month=07/day=02/

            Silver:
            s3://silver/tenants/samsung/inventory/region=europe/country=germany/year=2024/month=07/day=02/

            Gold:
            s3://gold/tenants/samsung/inventory_health/region=europe/year=2024/month=07/
        """
        if layer not in self.LAYER_BUCKETS:
            raise ValueError(f"Invalid layer: {layer}. Must be one of {list(self.LAYER_BUCKETS.keys())}")

        bucket = self.LAYER_BUCKETS[layer]
        parts = [f"s3://{bucket}", "tenants", tenant_id]

        # Bronze includes source system
        if layer == "bronze" and source_system:
            parts.append(source_system)

        parts.append(object_name.lower())

        # Add scope partitions
        if region:
            parts.append(f"region={region.lower()}")
        if country:
            parts.append(f"country={country.lower()}")

        # Add time partitions
        if year:
            parts.append(f"year={year}")
        if month:
            parts.append(f"month={month}")
        if day:
            parts.append(f"day={day}")

        return "/".join(parts) + "/"


# Singleton instance
path_resolver = StoragePathResolver()
