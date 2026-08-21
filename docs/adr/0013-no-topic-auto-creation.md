# ADR-0013: Topics are pre-provisioned; the platform never creates them

**Status:** Accepted (2026-08-07). Amends [ADR-0001](0001-dual-mode-channel.md).

## Context

In the target environment, application service accounts have no authority to create Kafka topics.
Topics are provisioned in advance by the platform team on request.

This is a common and reasonable posture — topic sprawl is real, partition counts are a capacity
decision, and retention has a cost — but it invalidates part of ADR-0001, which specified that
streaming mode would use "one durable topic per DAG edge". Creating a topic per edge at publish
time is not possible here, and would not have been desirable at scale in any case: a thousand
pipelines averaging four edges is four thousand topics for someone else to operate.

## Decision

### 1. The platform never creates a topic

There is no call to `AdminClient.createTopics` anywhere in the codebase, and no configuration
flag that enables one. `auto.create.topics.enable=false` is set on the local broker so development
matches production, and an ArchUnit rule fails the build if the method is referenced at all —
including in tests, where a convenience helper would otherwise be added first and depended on later.

### 2. Intermediate edges are always in-process

ADR-0001's transport selection is revised:

| Edge | Transport |
|---|---|
| A Kafka connector configured as a source or sink | The user's own pre-existing topic |
| Every intermediate stage-to-stage edge | `InProcessChannel`, in batch **and** streaming mode |
| Platform control traffic | A fixed set of pre-provisioned topics |

A streaming pipeline is therefore `Kafka topic → in-process transforms → sink`, not a chain of
topics. Replay is retained where it matters — at the source, from a consumer offset — and lost
between transformation stages, which in practice is recovered by replaying the source anyway.

The reasoning in ADR-0001 for rejecting broker-per-record is unchanged and this narrows the
remaining case. A normal migration such as PostgreSQL to Databricks puts **no records through
Kafka at all**.

### 3. A fixed platform topic list

Six topics, provisioned once. The list does not grow as pipelines are added, which is the property
that makes this workable operationally.

| Topic | Purpose | Suggested partitions | Retention | Cleanup |
|---|---|---|---|---|
| `dmp.run.commands.v1` | start, stop, pause commands | 12 | 7d | delete |
| `dmp.run.splits.v1` | split assignment to workers | 48 | 7d | delete |
| `dmp.run.events.v1` | run lifecycle events | 12 | 30d | delete |
| `dmp.delay.fired.v1` | delay-queue firing (ADR-0002) | 12 | 7d | delete |
| `dmp.dlq.v1` | dead-lettered records | 12 | 30d | delete |
| `dmp.audit.v1` | FULL audit tier (Phase 11 only) | 24 | 7d | delete |

`dmp.run.splits.v1` is given the most partitions because worker parallelism is bounded by its
partition count — that is the ceiling on how many splits can be assigned concurrently across the
fleet. It is the one number worth revisiting before a large deployment.

The `.v1` suffix exists so an incompatible message-schema change becomes a new topic requested
alongside the old one, rather than a coordinated stop-the-world cutover.

### 4. Missing topics fail loudly, at two points

**At publish.** Every Kafka connector's topic is checked with `describeTopics`. A missing topic
refuses publication with `KAFKA_TOPIC_NOT_FOUND`, naming both the topic and the node that
references it. This puts the failure on a screen someone is looking at.

**At run start.** Checked again, before any split is planned and before any connector submits an
external job. A topic can be deleted between publish and the 03:00 run, and the difference between
failing in VALIDATED and failing after a Salesforce bulk job has been created is a leaked quota.

The message states the remedy, not just the fault:

```
Topic 'sales.orders.v1' does not exist on this cluster.
Referenced by node 'kafka-source' in pipeline 'Orders → Databricks'.
Ask your platform team to create it, then retry.
```

Both checks require only `DESCRIBE` on the topic, which is the minimum the consumer needs anyway,
so no additional grant is involved.

## Consequences

**Positive**
- Matches the operating model rather than fighting it.
- No topic sprawl: the platform's topic count is constant, not a function of pipeline count.
- Failures surface at design time or at run start, never mid-migration.
- Partition counts and retention stay a capacity decision made by the people accountable for the
  cluster.
- Removes an entire class of accident in which a typo in a topic name silently creates a topic and
  the data goes somewhere nobody is looking.

**Negative**
- Creating a Kafka-sourced pipeline requires a prior request to the platform team. Mitigated by
  the publish-time check, which turns "it silently produced nothing" into an actionable message
  before anything is scheduled.
- No per-stage replay between transformation nodes in streaming mode. Recovered by replaying from
  the source offset, at the cost of re-running transforms already performed.
- A pipeline needing durable staging between two stages must have that topic provisioned and named
  explicitly. Supported, but opt-in and never implicit.
- Local development requires the six platform topics to exist. Created by a one-shot Compose
  service rather than by the application, so the production code path stays identical.

## Alternatives rejected

- **Create topics when permitted, fail otherwise.** Two code paths, and the one exercised in
  development is the one that does not run in production — which is how a creation call reaches
  production and fails at 03:00.
- **Multiplex all edges onto a few shared topics, routing by header.** Preserves per-edge
  durability with a fixed topic count, but every consumer reads and discards other pipelines'
  traffic, tenant isolation becomes an application concern on a shared log, and one large pipeline
  can starve every other. A worse failure mode than the one being avoided.
- **Request a topic automatically through the platform team's provisioning API.** Viable later if
  such an API exists, but it makes publication asynchronous and dependent on an external approval,
  which is a significant change to the editing flow for a benefit that a clear error message mostly
  provides.
