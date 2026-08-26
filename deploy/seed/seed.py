#!/usr/bin/env python3
"""Everything a new machine needs to have something to look at.

Creates the connections, the pipelines and one run, so that opening the console for the first time
shows a working platform rather than eight empty screens and a Create button.

Two rules it follows, and they are the reason this is safe to run on any stack:

**Nothing here invents data.** Every pipeline reads something that genuinely exists — the hundred
thousand orders in the bundled warehouse mock, and this platform's own audit log. Nothing writes
rows into a database so that a demo has something to show.

**It is idempotent.** Anything already present by name is left exactly as it is. Run it twice, run
it after building your own pipelines, run it on a stack that has been up for a month: it adds what
is missing and touches nothing else.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

BASE = os.environ.get("DMP_API", "http://app:8080") + "/api/v1"
TENANT = os.environ.get("DMP_TENANT", "default")


def call(method, path, body=None, quiet=False):
    request = urllib.request.Request(
        BASE + path,
        data=json.dumps(body).encode() if body is not None else None,
        method=method)
    request.add_header("X-Tenant-Id", TENANT)
    request.add_header("X-Actor", "seed")
    request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            raw = response.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        if not quiet:
            print(f"  ! {method} {path} -> {e.code} {e.read().decode()[:300]}")
        raise


def wait_for_api():
    """The API opens its port only once every store it needs is ready, so this is the whole check."""
    for attempt in range(120):
        try:
            call("GET", "/connectors", quiet=True)
            return
        except Exception:
            if attempt == 0:
                print("Waiting for the API...")
            time.sleep(5)
    sys.exit("The API never became reachable at " + BASE)


def existing(path, name):
    """Whatever is already called this, or None. The reason running twice is harmless."""
    page = call("GET", path + "?size=200")
    for item in page.get("content", page if isinstance(page, list) else []):
        if item.get("name") == name:
            return item
    return None


# --------------------------------------------------------------------------- connections

# Every one of these points at something the stack already runs, so none of them needs a credential
# and none of them can fail for a reason the person reading this can do nothing about.
CONNECTIONS = [
    {
        "name": "Warehouse (orders)",
        "description": "The bundled Databricks mock: 100,000 orders, no account needed",
        "connectorType": "databricks",
        "direction": "SOURCE",
        "config": {
            "host": "http://databricks-mock:8099",
            "warehouseId": "w1",
            "disposition": "INLINE",
            "pollSeconds": 2,
            # The point of named queries, in the smallest example that shows it: the nightly load
            # and the question a support desk arrives with, against one connection.
            "queries": [
                {"name": "By date range",
                 "sql": "SELECT * FROM orders WHERE placed_at >= :from AND placed_at < :to"},
                {"name": "By order number",
                 "sql": "SELECT * FROM orders WHERE order_id IN (:orderNos)"},
                {"name": "By region",
                 "sql": "SELECT * FROM orders WHERE region = :region"},
            ],
        },
        "secretRefs": {"token": "env:DBX_TOKEN"},
    },
    {
        "name": "Platform audit log",
        "description": "This platform's own audit trail, in Postgres — real data, nothing seeded",
        "connectorType": "jdbc-postgres",
        "direction": "SOURCE",
        "config": {
            "url": "jdbc:postgresql://postgres:5432/dmp",
            "schema": "public",
            "table": "audit_log",
            "queries": [
                {"name": "By date range", "where": "occurred_at >= :from AND occurred_at < :to"},
                {"name": "By action", "where": "action IN (:actions)"},
                {"name": "By resource",
                 "where": "resource_type = :resourceType AND resource_id = :resourceId"},
            ],
        },
        "secretRefs": {"username": "env:DMP_POSTGRES_USER",
                       "password": "env:DMP_POSTGRES_PASSWORD"},
    },
    {
        "name": "Order store (MongoDB)",
        "description": "Where migrated orders land. UPSERT, so a retry does not duplicate.",
        "connectorType": "mongodb",
        "direction": "SINK",
        "config": {
            "connectionString": "mongodb://mongo:27017/dmp?replicaSet=rs0",
            "collection": "seed_orders",
            "writeMode": "UPSERT",
            "keyField": "orderNumber",
        },
        "secretRefs": {},
    },
    {
        "name": "Order stream (Kafka)",
        "description": "The topic downstream systems read. It must already exist.",
        "connectorType": "kafka",
        "direction": "SINK",
        "config": {"bootstrapServers": "kafka:19092", "topic": "orders.v1"},
        "secretRefs": {},
    },
    {
        "name": "Order API (REST)",
        "description": "A destination that answers over HTTP, served by the bundled mock",
        "connectorType": "rest",
        "direction": "SINK",
        "config": {"url": "http://databricks-mock:8099/ingest", "writeMethod": "POST"},
        "secretRefs": {},
    },
]

POLICIES = {
    "chunkingPolicy": {"readFetchSize": 1000, "maxBatchBytes": 8388608, "flushInterval": 5.0,
                       "maxInFlightBatches": 2, "checkpointEveryNBatches": 0},
    "executionPolicy": {"maxConcurrentChunks": 2, "maxChunksPerPod": 8, "chunkLease": 300.0,
                        "maxAttemptsPerChunk": 3, "rowsPerChunk": 2000, "maxFailedPercent": 100,
                        "stopRunOnChunkFailure": False, "sequential": False},
    "auditPolicy": {"level": "INDEXED", "redactedFields": [], "redactionMode": "HASH",
                    "retention": 2592000.0, "samplesPerSignature": 10, "maxPayloadBytes": 32768,
                    "indexPayloads": True, "captureRejectedPayloads": True,
                    "stageLog": {"reads": True, "transforms": True, "writes": True, "bodies": True}},
    "deliveryPolicy": {"groupSize": 500, "splitScript": None, "wholeBatch": False,
                       "perRecord": False},
}

# The mapper and the validation rules are on one pipeline only, deliberately. Somebody opening this
# for the first time should see both what a plain copy looks like and what a shaped one does, and
# putting the same six mappings on every pipeline would teach neither.
SHAPE_ORDERS = {
    "id": "map", "type": "MAPPER", "name": "To the destination's shape",
    "connectorInstanceId": None,
    "config": {"keepUnmapped": False, "mappings": [
        {"from": "order_id", "to": "orderNumber", "required": True},
        {"from": "customer_id", "to": "customer"},
        {"from": "amount", "to": "amount", "type": "NUMBER"},
        {"from": "region", "to": "region", "default": "UNKNOWN", "case": "UPPER"},
        {"from": "status", "to": "stage",
         "values": {"NEW": "Open", "PAID": "Closed Won", "SHIPPED": "Closed Won",
                    "CANCELLED": "Closed Lost"},
         "otherwise": "Unknown"},
        {"from": "placed_at", "to": "placedAt"},
        {"to": "source", "default": "MIGRATION"},
    ]},
}

CHECK_ORDERS = {
    "id": "check", "type": "VALIDATION", "name": "Business rules",
    "connectorInstanceId": None,
    "config": {"onFail": "REJECT", "report": "ALL", "rules": [
        {"name": "an order must have a number", "field": "orderNumber", "check": "REQUIRED"},
        {"name": "amount must be positive", "field": "amount", "check": "MIN", "value": 1},
    ]},
}


def pipeline(name, description, nodes, edges, watched=True):
    found = existing("/pipelines", name)
    if found:
        print(f"  = {name}")
        return found["id"]

    created = call("POST", "/pipelines", {"name": name, "description": description})
    draft = call("POST", f"/pipelines/{created['id']}/versions", {"changeNote": "seeded"})
    call("PUT", f"/pipelines/{created['id']}/versions/{draft['id']}/definition",
         {"definition": {"nodes": nodes, "edges": edges}})
    call("PUT", f"/pipelines/{created['id']}/versions/{draft['id']}/policies", POLICIES)

    result = call("POST", f"/pipelines/{created['id']}/versions/{draft['id']}/validate", {})
    if not result.get("valid"):
        print(f"  ! {name} did not validate: {result.get('issues')}")
        return created["id"]

    call("POST", f"/pipelines/{created['id']}/versions/{draft['versionNumber']}/publish",
         {"changeNote": "seeded"})
    if watched:
        call("POST", f"/pipelines/{created['id']}/monitor?watched=true")
    print(f"  + {name}")
    return created["id"]


def main():
    wait_for_api()

    print("Connections")
    ids = {}
    for connection in CONNECTIONS:
        found = existing("/connector-instances", connection["name"])
        if found:
            ids[connection["name"]] = found["id"]
            print(f"  = {connection['name']}")
            continue
        created = call("POST", "/connector-instances", connection)
        ids[connection["name"]] = created["id"]
        status = call("POST", f"/connector-instances/{created['id']}/test", quiet=True)
        print(f"  + {connection['name']}  ({status.get('status', '?')})")

    warehouse = ids["Warehouse (orders)"]
    audit = ids["Platform audit log"]

    print("Pipelines")
    store = pipeline(
        "Orders to the store",
        "Warehouse orders, reshaped and checked, into MongoDB",
        [{"id": "src", "type": "SOURCE", "name": "Warehouse",
          "connectorInstanceId": warehouse, "config": {}},
         SHAPE_ORDERS, CHECK_ORDERS,
         {"id": "dst", "type": "SINK", "name": "Order store",
          "connectorInstanceId": ids["Order store (MongoDB)"], "config": {}}],
        [{"id": "e1", "from": "src", "to": "map"},
         {"id": "e2", "from": "map", "to": "check"},
         {"id": "e3", "from": "check", "to": "dst"}])

    pipeline(
        "Orders to the stream",
        "The same orders, unchanged, onto a Kafka topic",
        [{"id": "src", "type": "SOURCE", "name": "Warehouse",
          "connectorInstanceId": warehouse, "config": {}},
         {"id": "dst", "type": "SINK", "name": "Order stream",
          "connectorInstanceId": ids["Order stream (Kafka)"], "config": {}}],
        [{"id": "e1", "from": "src", "to": "dst"}])

    pipeline(
        "Orders to the API",
        "The same orders again, to a destination that answers over HTTP",
        [{"id": "src", "type": "SOURCE", "name": "Warehouse",
          "connectorInstanceId": warehouse, "config": {}},
         {"id": "dst", "type": "SINK", "name": "Order API",
          "connectorInstanceId": ids["Order API (REST)"], "config": {}}],
        [{"id": "e1", "from": "src", "to": "dst"}])

    pipeline(
        "Audit log to the stream",
        "This platform's own audit trail, out of Postgres onto Kafka",
        [{"id": "src", "type": "SOURCE", "name": "Audit log",
          "connectorInstanceId": audit, "config": {}},
         {"id": "dst", "type": "SINK", "name": "Order stream",
          "connectorInstanceId": ids["Order stream (Kafka)"], "config": {}}],
        [{"id": "e1", "from": "src", "to": "dst"}],
        watched=False)

    # One real run, so the dashboard has something to judge and the record search has something to
    # find. Fifty orders by number: enough to be a migration, small enough that nobody waits.
    if not call("GET", f"/runs?pipelineId={store}&size=1").get("content"):
        orders = [f"DBX-{100000 + i}" for i in range(50)]
        run = call("POST", f"/pipelines/{store}/runs",
                   {"query": "By order number", "parameters": {"orderNos": orders}})
        print(f"\nStarted a run of 'Orders to the store' — {run['id']}")

    print("""
Ready. http://localhost:3000

  Dashboard    every job, judged against its own history
  Pipelines    four of them, published and runnable
  Connectors   five connections, all pointing at something this stack already runs

Press Run now on any pipeline. 'Find records' offers the named queries — try
'By order number' with a few of DBX-100000, DBX-100001, DBX-100002.""")


if __name__ == "__main__":
    main()
