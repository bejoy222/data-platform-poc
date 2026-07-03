# ADR-002: Technology Stack — Production Tools in POC

## Status
Accepted

## Date
2024-07-03

## Context
The POC runs on two Windows laptops (64GB and 32GB RAM) using WSL2.
A lighter-weight stack was available and technically sufficient for a POC:
- Prefect instead of Airflow
- DuckDB instead of Spark
- Redpanda instead of Kafka

The question was whether to optimise for POC simplicity or production
learning fidelity.

## Decision
Use the full production stack (Kafka, Airflow, Spark) in the POC.

## Reasoning
Production path uses Kafka, Airflow, and Spark on AWS MSK, MWAA, and EMR.
Building the POC with different tools creates a hidden translation tax:
- Engineers learn the wrong tools
- Configuration patterns don't transfer
- Failure modes experienced in POC don't apply to production
- Re-learning required at the worst possible time (production pressure)

Operational pain encountered on constrained hardware teaches tuning skills
that transfer directly to production. This is a feature, not a bug.

## Hardware Reality Check
- Laptop 1: 64GB RAM — sufficient for Kafka + Spark Master + Airflow Scheduler
- Laptop 2: 32GB RAM — sufficient for Spark Worker + Airflow Worker
- Memory pressure is not a concern at these specs with correct configuration

## Alternatives Considered

### Prefect + DuckDB + Redpanda
Rejected. Easier to run but creates learning debt. Skills acquired do not
transfer to production stack. POC success would not de-risk production delivery.

## Consequences
- Explicit Spark executor memory limits required in all configurations
- Longer initial setup time vs lightweight alternatives
- Engineers gain genuine production skills during POC phase
- All performance tuning done in POC transfers directly to AWS EMR

## References
- ADR-004: AWS Production Target
- Infrastructure and Environment design document
