# ADR-005: Organisational Hierarchy — Hybrid Query Scoping

## Status
Accepted

## Date
2024-07-03

## Context
The platform's initial tenant is Samsung, which operates through
approximately 50 country subsidiaries across multiple regions. The use
case requires answering questions at different organisational levels:

- Country level: "Why are UK inventory costs increasing?"
- Regional level: "Which products are overstocked across Europe?"
- Global level: "Which products are overstocked globally?"

Two extreme models were considered:
- Each subsidiary as a completely separate tenant
- All subsidiaries as a flat shared namespace

## Decision
Samsung is one tenant. Subsidiaries are organisational units (org units)
within that tenant. Queries are scoped by the requesting user's role to
country, region, or global level.

### Organisational Hierarchy
### Role-Based Scope Enforcement
| Role | Default Scope | Maximum Scope |
|------|--------------|---------------|
| Warehouse Manager | Single warehouse | Country |
| Country Manager | Country | Country |
| Regional Manager | Region | Region |
| Supply Chain Manager | Region | Global |
| Executive | Global | Global |

### Storage Partitioning
Data partitioned by region then country within tenant path:
Spark partition pruning enforces scope at read time — data outside
the user's scope is never loaded into memory.

## Alternatives Considered

### Each subsidiary as a separate tenant
Rejected. Cross-subsidiary queries ("European overstock report") become
architecturally impossible — they require querying across tenant isolation
boundaries, which the security model explicitly prevents.

### Flat shared namespace (no hierarchy)
Rejected. No mechanism for role-based scope enforcement. A warehouse
manager could query global data. Not acceptable for enterprise deployment.

## Consequences
- Storage path gains region= and country= partition levels
- Schema Registry stores full org hierarchy per tenant
- Query API resolves user scope before any Spark job executes
- A country manager physically cannot issue a global query
- Same data, same pipeline, same schema serves all scope levels
- Cross-subsidiary aggregation (global view) works natively via
  Spark reading the full tenant path without partition filter

## References
- ADR-001: Tenancy Model
- Data Lake and Storage Design document
- Role-Based Access Control design
