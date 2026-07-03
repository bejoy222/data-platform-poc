# ADR-001: Tenancy Model — Schema Per Tenant

## Status
Accepted

## Date
2024-07-03

## Context
The platform serves multiple enterprise customers (tenants), each needing
isolated data and the ability to define their own data structures. Three
isolation models were considered:

- **Database per tenant:** separate database instance per customer
- **Schema per tenant:** shared database, separate schema per customer
- **Row-level isolation:** shared tables with tenant_id column

The platform targets up to 50 tenants initially, scaling further in production.

## Decision
Schema-per-tenant isolation within a shared platform infrastructure.

Each tenant receives:
- Their own path prefix in object storage (s3://layer/tenants/{tenant_id}/)
- Their own MinIO access credentials scoped to their prefix
- Their own business object definitions in the Schema Registry
- Their own Airflow DAG group
- Their own organisational hierarchy (regions → countries → org units)

## Alternatives Considered

### Database per tenant
Rejected. Cost and operational overhead scales linearly with tenant count.
50 tenants means 50 database instances. Unmanageable at POC scale on
limited hardware, and expensive in AWS production.

### Row-level isolation
Rejected. Insufficient security boundary. A single query bug or missing
WHERE clause could expose cross-tenant data. Not acceptable for enterprise
customers with regulatory obligations.

## Consequences
- Schema Registry becomes mandatory — all services must consult it
- Tenant onboarding is a registration exercise, not a deployment exercise
- Adding a 51st tenant takes the same time as adding the 1st
- GDPR deletion is straightforward — recursive delete on tenant path prefix
- Cross-tenant queries are prevented by design (correct behaviour)
- Cross-org-unit queries within a tenant are explicitly supported via
  the organisational hierarchy model

## References
- ADR-005: Organisational Hierarchy (Samsung subsidiary model)
- Schema Registry Service design document
