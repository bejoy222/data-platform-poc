from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel


# --- Tenant Schemas ---

class TenantCreate(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    sla_tier: str = "standard"


class TenantResponse(BaseModel):
    id: str
    name: str
    description: Optional[str]
    sla_tier: str
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True


# --- OrgUnit Schemas ---

class OrgUnitCreate(BaseModel):
    id: str
    name: str
    level: str
    parent_id: Optional[str] = None
    region: Optional[str] = None
    country: Optional[str] = None


class OrgUnitResponse(BaseModel):
    id: str
    tenant_id: str
    name: str
    level: str
    parent_id: Optional[str]
    region: Optional[str]
    country: Optional[str]
    is_active: bool

    class Config:
        from_attributes = True


# --- ObjectField Schemas ---

class ObjectFieldCreate(BaseModel):
    name: str
    data_type: str
    is_required: bool = False
    is_unique_key: bool = False
    default_value: Optional[str] = None
    description: Optional[str] = None


class ObjectFieldResponse(BaseModel):
    id: int
    name: str
    data_type: str
    is_required: bool
    is_unique_key: bool
    default_value: Optional[str]
    description: Optional[str]

    class Config:
        from_attributes = True


# --- BusinessObject Schemas ---

class BusinessObjectCreate(BaseModel):
    name: str
    description: Optional[str] = None
    fields: List[ObjectFieldCreate] = []


class BusinessObjectResponse(BaseModel):
    id: int
    tenant_id: str
    name: str
    description: Optional[str]
    is_active: bool
    created_at: datetime
    fields: List[ObjectFieldResponse] = []

    class Config:
        from_attributes = True


# --- SourceMapping Schemas ---

class SourceMappingCreate(BaseModel):
    source_system: str
    source_field: str
    target_field: str
    transform: Optional[str] = None


class SourceMappingResponse(BaseModel):
    id: int
    object_id: int
    source_system: str
    source_field: str
    target_field: str
    transform: Optional[str]

    class Config:
        from_attributes = True


# --- Storage Path Schema ---

class StoragePathResponse(BaseModel):
    tenant_id: str
    object_name: str
    layer: str
    region: Optional[str]
    country: Optional[str]
    path: str
