# ADR-004: AWS as Production Target

## Status
Accepted

## Date
2024-07-03

## Context
The POC runs on local hardware (two laptops + Synology NAS). The production
target is AWS, either via Samsung's own AWS-based cloud platform or directly
on AWS. Every architectural decision must account for this migration path.

## Decision
Design every POC component as its AWS managed service equivalent.
All storage references use s3:// paths exclusively.
All credentials follow IAM-style scoping from day one.

## Component Mapping

| POC | AWS Production | Migration Effort |
|-----|---------------|-----------------|
| Apache Kafka | Amazon MSK | Config change only |
| Apache Airflow | Amazon MWAA | Config change only |
| Apache Spark | Amazon EMR | Config change only |
| MinIO (S3-compatible) | Amazon S3 | URL + credentials only |
| Apache Parquet | Parquet on S3 | Zero change |
| FastAPI (Docker) | Amazon ECS Fargate | Already containerised |
| Schema Registry (Docker) | Amazon ECS Fargate | Already containerised |
| Streamlit (Docker) | ECS / Amplify | Already containerised |
| Docker Compose | Amazon EKS | Compose → Helm charts |
| Terraform | Terraform (same) | Zero change |
| GitHub Actions | GitHub Actions (same) | Zero change |
| NFS (DS220+) | Amazon EFS | Mount path change only |

## Non-Negotiable Rules Derived From This Decision
1. Every storage path uses s3:// — no local file paths in any service code
2. MinIO endpoint URL is always injected via environment variable
3. All credentials injected via environment — never hardcoded
4. All services containerised from first line of code
5. Docker Compose files structured for mechanical translation to Helm charts

## Consequences
- MinIO is mandatory in POC — no shortcuts to local filesystem
- Slightly more initial setup complexity
- Zero re-architecture cost when moving to AWS
- Production migration is ops work, not engineering work
- Samsung-specific AWS constraints (VPC, IAM boundaries, data residency)
  to be confirmed before production migration planning

## References
- ADR-002: Stack Selection
- ADR-003: Medallion Architecture
- AWS Production Path mapping document
