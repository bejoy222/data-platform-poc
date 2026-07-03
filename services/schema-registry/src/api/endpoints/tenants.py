from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from src.db.database import get_db
from src.models.tenant import Tenant, OrgUnit, BusinessObject, ObjectField, SourceMapping
from src.schemas.tenant import (
    TenantCreate, TenantResponse,
    OrgUnitCreate, OrgUnitResponse,
    BusinessObjectCreate, BusinessObjectResponse,
    SourceMappingCreate, SourceMappingResponse,
    StoragePathResponse
)
from src.services.path_resolver import path_resolver

router = APIRouter(prefix="/tenants", tags=["tenants"])


# --- Tenant Endpoints ---

@router.post("/", response_model=TenantResponse, status_code=status.HTTP_201_CREATED)
def create_tenant(tenant: TenantCreate, db: Session = Depends(get_db)):
    """Register a new tenant on the platform."""
    existing = db.query(Tenant).filter(Tenant.id == tenant.id).first()
    if existing:
        raise HTTPException(status_code=400, detail=f"Tenant '{tenant.id}' already exists")

    db_tenant = Tenant(**tenant.model_dump())
    db.add(db_tenant)
    db.commit()
    db.refresh(db_tenant)
    return db_tenant


@router.get("/", response_model=List[TenantResponse])
def list_tenants(db: Session = Depends(get_db)):
    """List all registered tenants."""
    return db.query(Tenant).filter(Tenant.is_active == True).all()


@router.get("/{tenant_id}", response_model=TenantResponse)
def get_tenant(tenant_id: str, db: Session = Depends(get_db)):
    """Get a specific tenant by ID."""
    tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail=f"Tenant '{tenant_id}' not found")
    return tenant


# --- OrgUnit Endpoints ---

@router.post("/{tenant_id}/org-units", response_model=OrgUnitResponse, status_code=status.HTTP_201_CREATED)
def create_org_unit(tenant_id: str, org_unit: OrgUnitCreate, db: Session = Depends(get_db)):
    """Register an organisational unit for a tenant."""
    tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail=f"Tenant '{tenant_id}' not found")

    db_org_unit = OrgUnit(tenant_id=tenant_id, **org_unit.model_dump())
    db.add(db_org_unit)
    db.commit()
    db.refresh(db_org_unit)
    return db_org_unit


@router.get("/{tenant_id}/org-units", response_model=List[OrgUnitResponse])
def list_org_units(tenant_id: str, db: Session = Depends(get_db)):
    """List all org units for a tenant."""
    return db.query(OrgUnit).filter(
        OrgUnit.tenant_id == tenant_id,
        OrgUnit.is_active == True
    ).all()


# --- Business Object Endpoints ---

@router.post("/{tenant_id}/objects", response_model=BusinessObjectResponse, status_code=status.HTTP_201_CREATED)
def create_object(tenant_id: str, obj: BusinessObjectCreate, db: Session = Depends(get_db)):
    """Register a business object for a tenant."""
    tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail=f"Tenant '{tenant_id}' not found")

    db_object = BusinessObject(
        tenant_id=tenant_id,
        name=obj.name,
        description=obj.description
    )
    db.add(db_object)
    db.flush()

    # Add fields
    for field in obj.fields:
        db_field = ObjectField(object_id=db_object.id, **field.model_dump())
        db.add(db_field)

    db.commit()
    db.refresh(db_object)
    return db_object


@router.get("/{tenant_id}/objects", response_model=List[BusinessObjectResponse])
def list_objects(tenant_id: str, db: Session = Depends(get_db)):
    """List all business objects for a tenant."""
    return db.query(BusinessObject).filter(
        BusinessObject.tenant_id == tenant_id,
        BusinessObject.is_active == True
    ).all()


@router.get("/{tenant_id}/objects/{object_name}", response_model=BusinessObjectResponse)
def get_object(tenant_id: str, object_name: str, db: Session = Depends(get_db)):
    """Get a specific business object."""
    obj = db.query(BusinessObject).filter(
        BusinessObject.tenant_id == tenant_id,
        BusinessObject.name == object_name
    ).first()
    if not obj:
        raise HTTPException(status_code=404, detail=f"Object '{object_name}' not found for tenant '{tenant_id}'")
    return obj


# --- Source Mapping Endpoints ---

@router.post("/{tenant_id}/objects/{object_name}/mappings", response_model=SourceMappingResponse, status_code=status.HTTP_201_CREATED)
def create_mapping(tenant_id: str, object_name: str, mapping: SourceMappingCreate, db: Session = Depends(get_db)):
    """Add a source field mapping to a business object."""
    obj = db.query(BusinessObject).filter(
        BusinessObject.tenant_id == tenant_id,
        BusinessObject.name == object_name
    ).first()
    if not obj:
        raise HTTPException(status_code=404, detail=f"Object '{object_name}' not found")

    db_mapping = SourceMapping(object_id=obj.id, **mapping.model_dump())
    db.add(db_mapping)
    db.commit()
    db.refresh(db_mapping)
    return db_mapping


@router.get("/{tenant_id}/objects/{object_name}/mappings", response_model=List[SourceMappingResponse])
def list_mappings(tenant_id: str, object_name: str, source_system: str = None, db: Session = Depends(get_db)):
    """Get source mappings for a business object."""
    obj = db.query(BusinessObject).filter(
        BusinessObject.tenant_id == tenant_id,
        BusinessObject.name == object_name
    ).first()
    if not obj:
        raise HTTPException(status_code=404, detail=f"Object '{object_name}' not found")

    query = db.query(SourceMapping).filter(SourceMapping.object_id == obj.id)
    if source_system:
        query = query.filter(SourceMapping.source_system == source_system)
    return query.all()


# --- Storage Path Resolver Endpoint ---

@router.get("/{tenant_id}/objects/{object_name}/path", response_model=StoragePathResponse)
def resolve_path(
    tenant_id: str,
    object_name: str,
    layer: str,
    region: str = None,
    country: str = None,
    source_system: str = None,
    year: str = None,
    month: str = None,
    day: str = None,
    db: Session = Depends(get_db)
):
    """
    Resolve the s3:// storage path for a tenant object.
    This is the core isolation enforcement endpoint.
    All services must call this before reading or writing data.
    """
    tenant = db.query(Tenant).filter(Tenant.id == tenant_id).first()
    if not tenant:
        raise HTTPException(status_code=404, detail=f"Tenant '{tenant_id}' not found")

    path = path_resolver.resolve(
        tenant_id=tenant_id,
        layer=layer,
        object_name=object_name,
        source_system=source_system,
        region=region,
        country=country,
        year=year,
        month=month,
        day=day
    )

    return StoragePathResponse(
        tenant_id=tenant_id,
        object_name=object_name,
        layer=layer,
        region=region,
        country=country,
        path=path
    )
