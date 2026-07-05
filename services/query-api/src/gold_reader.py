"""
Reads Gold layer Parquet files from MinIO using PyArrow.
No Spark needed for queries — PyArrow reads Parquet directly.
Fast, lightweight, perfect for API responses.
"""
import boto3
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from io import BytesIO
from src.config import settings


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.MINIO_ENDPOINT,
        aws_access_key_id=settings.MINIO_ACCESS_KEY,
        aws_secret_access_key=settings.MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def list_parquet_files(bucket: str, prefix: str) -> list:
    """List all Parquet files under a prefix."""
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = []
    if "Contents" in response:
        for obj in response["Contents"]:
            if obj["Key"].endswith(".parquet"):
                files.append(obj["Key"])
    return files


def read_gold_table(tenant_id: str, table_name: str, filters: dict = None) -> pd.DataFrame:
    """
    Read a Gold layer table from MinIO into a Pandas DataFrame.
    
    Args:
        tenant_id: Tenant identifier (e.g. 'samsung')
        table_name: Gold table name (e.g. 'inventory_health', 'stock_risk', 'regional_summary')
        filters: Optional dict of column filters e.g. {'_country': 'germany'}
    
    Returns:
        Pandas DataFrame with the table data
    """
    s3 = get_s3_client()
    prefix = f"tenants/{tenant_id}/{table_name}/"
    
    files = list_parquet_files("gold", prefix)
    
    if not files:
        return pd.DataFrame()
    
    dfs = []
    for file_key in files:
        response = s3.get_object(Bucket="gold", Key=file_key)
        data = response["Body"].read()
        table = pq.read_table(BytesIO(data))
        dfs.append(table.to_pandas())
    
    if not dfs:
        return pd.DataFrame()
    
    df = pd.concat(dfs, ignore_index=True)
    
    # Apply filters
    if filters:
        for col, val in filters.items():
            if col in df.columns:
                df = df[df[col] == val]
    
    return df


def get_inventory_health(tenant_id: str = "samsung", country: str = None) -> pd.DataFrame:
    filters = {}
    if country:
        filters["_country"] = country
    return read_gold_table(tenant_id, "inventory_health", filters)


def get_stock_risk(tenant_id: str = "samsung") -> pd.DataFrame:
    return read_gold_table(tenant_id, "stock_risk")


def get_regional_summary(tenant_id: str = "samsung", region: str = None) -> pd.DataFrame:
    filters = {}
    if region:
        filters["_region"] = region
    return read_gold_table(tenant_id, "regional_summary", filters)
