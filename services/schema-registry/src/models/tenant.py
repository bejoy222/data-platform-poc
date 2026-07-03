from datetime import datetime
from sqlalchemy import Column, String, DateTime, Boolean, Text, ForeignKey, Integer
from sqlalchemy.orm import relationship
from src.db.database import Base


class Tenant(Base):
    __tablename__ = "tenants"

    id = Column(String(50), primary_key=True)
    name = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)
    sla_tier = Column(String(20), default="standard")
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    org_units = relationship("OrgUnit", back_populates="tenant", cascade="all, delete-orphan")
    objects = relationship("BusinessObject", back_populates="tenant", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Tenant(id={self.id}, name={self.name})>"


class OrgUnit(Base):
    __tablename__ = "org_units"

    id = Column(String(100), primary_key=True)
    tenant_id = Column(String(50), ForeignKey("tenants.id"), nullable=False)
    name = Column(String(200), nullable=False)
    level = Column(String(20), nullable=False)  # global, region, country, warehouse
    parent_id = Column(String(100), ForeignKey("org_units.id"), nullable=True)
    region = Column(String(100), nullable=True)
    country = Column(String(100), nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    tenant = relationship("Tenant", back_populates="org_units")
    children = relationship("OrgUnit", backref="parent", remote_side=[id])

    def __repr__(self):
        return f"<OrgUnit(id={self.id}, level={self.level})>"


class BusinessObject(Base):
    __tablename__ = "business_objects"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tenant_id = Column(String(50), ForeignKey("tenants.id"), nullable=False)
    name = Column(String(100), nullable=False)
    description = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    tenant = relationship("Tenant", back_populates="objects")
    fields = relationship("ObjectField", back_populates="business_object", cascade="all, delete-orphan")
    source_mappings = relationship("SourceMapping", back_populates="business_object", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<BusinessObject(tenant={self.tenant_id}, name={self.name})>"


class ObjectField(Base):
    __tablename__ = "object_fields"

    id = Column(Integer, primary_key=True, autoincrement=True)
    object_id = Column(Integer, ForeignKey("business_objects.id"), nullable=False)
    name = Column(String(100), nullable=False)
    data_type = Column(String(50), nullable=False)  # string, integer, float, timestamp, boolean
    is_required = Column(Boolean, default=False)
    is_unique_key = Column(Boolean, default=False)
    default_value = Column(String(200), nullable=True)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    business_object = relationship("BusinessObject", back_populates="fields")

    def __repr__(self):
        return f"<ObjectField(name={self.name}, type={self.data_type})>"


class SourceMapping(Base):
    __tablename__ = "source_mappings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    object_id = Column(Integer, ForeignKey("business_objects.id"), nullable=False)
    source_system = Column(String(100), nullable=False)  # sap_erp, wms, sales
    source_field = Column(String(100), nullable=False)
    target_field = Column(String(100), nullable=False)
    transform = Column(String(200), nullable=True)  # optional transformation expression
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    business_object = relationship("BusinessObject", back_populates="source_mappings")

    def __repr__(self):
        return f"<SourceMapping({self.source_field} → {self.target_field})>"
