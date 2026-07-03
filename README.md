# Multi-Tenant Data Platform — POC

A proof-of-concept multi-tenant data platform where each customer defines
their own data structures. The platform ingests from any source, stores in
a governed medallion data lake, and serves AI-powered natural language queries.

## POC Use Case
Samsung Global Inventory Optimisation across European subsidiaries.

## Architecture
- **Ingest:** Apache Kafka
- **Orchestrate:** Apache Airflow
- **Process:** Apache Spark
- **Store:** MinIO (S3-compatible) + Apache Parquet
- **Serve:** FastAPI + LangChain + Streamlit

## Quick Start

```bash
# Clone the repo
git clone git@github.com:bejoy222/data-platform-poc.git
cd data-platform-poc

# Set up environment
make setup

# Edit .env with your values
vim .env

# Start all services
make up
```

## Documentation
- [Architecture Decision Records](docs/adr/)
- [Sprint Plan](docs/runbooks/)
- [Full Project Document](docs/)

## Development
All changes via pull requests. No direct pushes to main.

```bash
# Run linters before raising a PR
make lint

# Run tests
make test
```

## Node Layout
| Node | IP | Role |
|---|---|---|
| Laptop 1 (64GB) | 192.168.0.10 | Kafka, Spark Master, Airflow Scheduler, Schema Registry, MinIO |
| Laptop 2 (32GB) | 192.168.0.11 | Spark Worker, Airflow Worker, FastAPI, Streamlit |
| DS220+ NAS | 192.168.0.20 | NFS storage backend |
