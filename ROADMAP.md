# Roadmap

Each phase follows the same protocol: architecture discussion → alternatives and trade-offs
→ approval → production-quality implementation with tests. No phase is complete until its
exit criteria are demonstrably met.

| # | Phase | Exit criteria | Status |
|---|---|---|---|
| 0 | Architecture and ADRs | Foundation architecture documented, key decisions recorded | ✅ Complete |
| 1 | Domain, persistence, control-plane CRUD | Pipelines, versions and connector instances round-trip through the API; Testcontainers suite green; ArchUnit boundaries enforced | ⏳ Awaiting approval |
| 2 | Connector SPI, plugin runtime, TCK | A connector jar built outside the repo loads and runs without a platform rebuild; PostgreSQL and CSV connectors pass the TCK | |
| 3 | Engine core | Manual run PostgreSQL → CSV over 10M rows with `readFetchSize=100, writeBatchSize=1000`; heap stays within `maxInFlightBatches × maxBatchBytes`; `kill -9` mid-run resumes from the last batch checkpoint with no duplicates or gaps | |
| 4 | Messaging, delay queue, scheduler | Cron-triggered run fires; a failing record retries 60s → 5m → 30m → 2h → DLQ; two-collection timer design verified end to end; reconciliation sweeper recovers timers after a simulated oplog gap | |
| 5 | Transformation engine | Adversarial sandbox suite passes as a release gate; JSONata mapper and filter nodes operational | |
| 6 | Streaming mode | The same pipeline definition runs in batch and streaming modes with no change to its definition | |
| 7 | Observability | Grafana dashboards for consumer lag, throughput, transformation latency and error rates; OpenTelemetry traces span control plane to sink | |
| 8 | React console — core | A real migration is configured, launched, monitored and debugged entirely from the browser | |
| 9 | Pipeline designer | A pipeline is built end-to-end with React Flow, spec-driven connector forms and the Monaco transform editor, with dry-run preview. No YAML | |
| 10 | Scale hardening | 100M-row load completed with documented throughput figures; rate limiting, parallelism and resume verified under load; Kubernetes manifests | |
| 11 | Connector breadth | MongoDB, Elasticsearch, REST, Salesforce Bulk API v2, SFTP, Excel, Databricks — each passing the TCK | |
| 12 | CDC, versioning, access control | Change data capture; pipeline version diff and rollback; RBAC wired to company SSO; multi-tenancy hardening | |

## Deferred by decision

Security and authentication are deferred pending company SSO. A `SecretsProvider` SPI and a
tenant boundary nevertheless exist in the domain model from Phase 1 — retrofitting
multi-tenancy is a rewrite, not a feature.

## Architecture

- [`docs/architecture/README.md`](docs/architecture/README.md) — high-level design
- [`docs/architecture/run-lifecycle.md`](docs/architecture/run-lifecycle.md) — low-level design:
  what happens when a run starts, what is stored where, how work is distributed
- [`docs/adr/`](docs/adr/) — the decision records
- [`docs/examples/`](docs/examples/) — working pipelines, with the output of an actual run

Where the two architecture documents disagree with the code, the code wins and the divergence is
called out in place. ADR-0013 and ADR-0014 both amend ADR-0001, and the high-level design was
written before them.
