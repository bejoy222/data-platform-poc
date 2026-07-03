# ADR-003: Medallion Architecture (Bronze / Silver / Gold)

## Status
Accepted

## Date
2024-07-03

## Context
The platform ingests data from multiple heterogeneous sources per tenant.
Raw data arrives in various formats, schemas, and quality levels. A storage
and processing pattern was needed that supports:
- Immutable raw data preservation
- Progressive data refinement
- Schema Registry integration
- Reprocessing capability when schema definitions change
- Query performance at the Gold layer

## Decision
Adopt the Medallion Architecture with three layers:

### Bronze (Raw)
- Exact copy of source data as received
- Immutable — never modified after landing
- Schema-agnostic — accepts any structure
- Partitioned by: tenant / source / region / country / year / month / day
- Enables full reprocessing if schema changes

### Silver (Processed)
- Validated, typed, deduplicated
- Schema Registry field mappings applied here
- Source fields mapped to tenant business objects
- Tenant-isolated by path prefix
- Partitioned by: tenant / object / region / country / year / month / day

### Gold (Curated)
- Tenant business objects fully joined and aggregated
- Optimised for query performance
- Powers all API and AI natural language queries
- Partitioned by: tenant / view / region / year / month

## S3 Path Convention
## Alternatives Considered

### Single layer (raw + processed combined)
Rejected. No ability to reprocess when schema changes. No clear data
quality boundary. Debugging data issues becomes very difficult.

### Two layers (raw + processed)
Rejected. Missing the query-optimised Gold layer means Spark must do
expensive joins at query time. Unacceptable for AI query response times.

## Consequences
- Three Airflow DAG stages per data source (Bronze → Silver → Gold)
- Slightly more pipeline complexity
- Significantly better data governance and debuggability
- Full reprocessing from Bronze possible at any time
- Query performance predictable and tuneable at Gold layer
- Direct equivalent to S3 + Glue Catalog + Athena in AWS production

## References
- ADR-004: AWS Production Target
- Data Lake and Storage Design document
