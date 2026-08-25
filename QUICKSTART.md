# Running this on another machine

One prerequisite and one command.

## Prerequisite

**Docker Desktop**, with at least **8 GB** given to it (Settings → Resources). OpenSearch alone
wants 2 GB, and the stack is nine containers.

Nothing else. No Java, no Maven, no Node, no Python — every build happens inside a container.

## The command

```bash
git clone https://github.com/amitgoyal4663-design/data-migration-platform.git
cd data-migration-platform
make stack
```

The first run takes roughly ten minutes, almost all of it compiling the backend and the console
inside their build images. Later runs start in under a minute.

When it finishes:

| | |
|---|---|
| Console | http://localhost:3000 |
| API docs | http://localhost:8080/swagger-ui.html |
| Health | http://localhost:8080/actuator/health |
| OpenSearch Dashboards | http://localhost:5601 |

`make down` stops it and keeps the data. `make reset` stops it and deletes the data.

## What comes up

Nine containers: Postgres (definitions), MongoDB (runs and chunks), OpenSearch (the record index
and the stage log), Redis (rate limits), Kafka (run events), the API, the console, and two
helpers described below.

**`kafka-init`** creates the topics and then exits. It exists because the platform deliberately
*cannot* create a topic — in the environments this is built for, topics are provisioned by another
team on request, and a job whose topic is missing must stop and say so rather than quietly
conjuring one. Broker-side auto-creation is off. This service is that other team, for a laptop.

**`databricks-mock`** is a fake SQL warehouse whose SQL really executes, against a SQLite database
seeded with 100,000 orders, plus a fake customer API that stores what it is sent into MongoDB. It
is there so the stack demonstrates something the minute it is up, without anyone needing a
Databricks account. Nothing in the platform depends on it — delete the service and the platform is
unchanged.

## What to migrate, on a machine with no accounts

The stack comes with both ends of a migration already running, so the first pipeline can be built
in the console without signing up for anything:

| Source you can point at | Where it is | What is in it |
|---|---|---|
| PostgreSQL `demo.orders` | `postgres:5432`, user/password `dmp` | 50,000 orders |
| Mock Databricks warehouse | `http://databricks-mock:8099`, any token | 100,000 orders, real SQL |
| MongoDB | `mongodb://mongo:27017/dmp?replicaSet=rs0` | empty, ready to write |
| Mock customer API | `POST http://databricks-mock:8099/ingest` | stores into MongoDB |
| Kafka | `kafka:19092`, topic `orders.v1` | created by `kafka-init` |

**Use the service names, not `localhost`.** The API runs in a container, so `localhost` there is
the API itself. This is the single most common mistake when setting the first connection up.

## Credentials

**None are required** for any of the above.

For a real Salesforce org, copy `deploy/compose/.env.example` to `deploy/compose/.env` and fill it
in. The definition store holds `env:SF_CLIENT_ID` and never the value, so a pipeline can be
exported, reviewed and committed without carrying a secret with it — the value arrives through
that file and nowhere else.

Without it, the Salesforce connector fails its own connection test with a message naming the
missing field. Nothing else is affected.

## If something is wrong

```bash
make ps       # which containers are up
make logs     # follow all of them
```

The two failures worth knowing in advance:

**The console loads but every call fails.** The API is still starting — it waits for Postgres,
MongoDB, OpenSearch and the Kafka topics before it opens a port. Give it a minute; `make ps` shows
`dmp-app` as `healthy` when it is genuinely ready.

**A container is killed during the first build.** Docker Desktop is short of memory. Raise it to
8 GB and run `make stack` again; the build resumes from the layers it already has.
