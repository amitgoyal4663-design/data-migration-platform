# Data Migration Platform

Moves data between systems that were never designed to talk to each other, and keeps an account of
every record it moved.

A migration is not a script that ran. It is a claim — *these forty thousand policies are now in
Salesforce* — and the only useful version of this software is one that can be asked to prove it.
So a run is divided into chunks that resume where they stopped, every record is indexed by its own
key, every failure keeps the payload that caused it, and the numbers at the end are reconciled from
two independent counts that have to agree.

Built to compete with NiFi, Airbyte, Fivetran and Talend on the part those tools leave to you: what
happened to record 44219.

---

## Run it

**One prerequisite, one command.** Docker Desktop with 8 GB — no Java, no Maven, no Node. Every
build happens inside a container.

```bash
git clone https://github.com/amitgoyal4663-design/data-migration-platform.git
cd data-migration-platform
make stack
```

First run is about ten minutes, nearly all of it compiling the backend and console. Later runs
start in under a minute.

| | |
|---|---|
| Console | http://localhost:3000 |
| API docs | http://localhost:8080/swagger-ui.html |
| Search and logs | http://localhost:5601 |

`make down` stops it and keeps the data. `make reset` deletes the data. `make help` lists the rest.

**No credentials are needed.** A mock Databricks warehouse with 100,000 orders is bundled, so a
full migration runs end to end on a laptop with nothing to sign up for.

[QUICKSTART.md](QUICKSTART.md) has the detail — what each container is for, the first pipeline to
build, and the two failures worth knowing in advance.

---

## What it does

**Connectors** — MongoDB, PostgreSQL, MySQL, Oracle, SQL Server, Databricks, Salesforce, Kafka,
REST and CSV. Each declares its own configuration as a JSON Schema, and the console renders the
form from that at run time: a new connector jar is a complete, validated UI with no frontend
change.

**Chunked, resumable execution.** A run is planned into chunks over key ranges — never `OFFSET`,
which makes chunk 400 scan and discard the rows before it. Each chunk is claimed by one worker
under a lease, checkpoints as it goes, and resumes from its cursor rather than from the beginning.
A pod dying costs one chunk's current batch.

**Transforms** — JavaScript on GraalJS, per record or per batch, plus declarative mapper and
validation nodes for the cases that do not need code. A validation node reports *every* rule a
record broke, not the first.

**Named queries.** One connection can offer several ways to find records — "By date range" for the
nightly load, "By policy number" for the support desk holding a policy number and no idea when it
was last touched. Support picks a name and fills in a box; they never write a query.

**An account of every record.** Written, rejected and failed records are indexed by key with the
payload that caused each failure. A run ends with a reconciliation: two independent counts, four
cross-checks, and an explicit line for anything unaccounted for. *Read minus written* is data loss,
and it is on the screen rather than in a log.

**One dashboard.** Watchlist health judged against each pipeline's own history — a median of its
last ten runs, not a threshold somebody guessed. Filters, a volume view for whoever asks how much
moved this week, and the actions to do something about what it reports.

---

## Layout

```
backend/
  dmp-domain            entities and invariants — no framework, no I/O
  dmp-application       use cases and the ports they need
  dmp-engine            planning, claiming, executing, reconciling
  dmp-connector-api     what a connector implements
  connectors/           mongodb, jdbc, databricks, salesforce, kafka, rest, file
  dmp-persistence-*     postgres for definitions, mongo for runs
  dmp-recordlog-*       opensearch for the record index and stage log
  dmp-transform-*       graaljs
  apps/dmp-app          the Spring Boot application
frontend/dmp-console    React, TypeScript, MUI
deploy/compose          the whole stack
mocks/databricks        a real SQL warehouse mock, so no account is needed
docs/                   architecture, decision records, worked examples
```

Ports and adapters throughout: the domain has no Spring, no Jackson and no database. Which store
sits behind a repository is a decision the domain cannot see.

---

## Building without Docker

```bash
make build    # backend and console
make test     # unit tests, no containers
make verify   # plus integration tests (Testcontainers)
```

Needs JDK 21 and Node 20.

---

## Status

Working software under active development. [ROADMAP.md](ROADMAP.md) is what is done and what is
next; [docs/adr](docs/adr) records the decisions and what they cost.
