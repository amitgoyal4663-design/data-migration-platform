#!/usr/bin/env python3
"""Fake Databricks SQL API backed by SQLite, plus a REST endpoint that writes to MongoDB.

Two halves of one migration, so a pipeline can be run end to end without any third-party account:
the source is a fake warehouse whose SQL really executes, and the sink is a fake customer API that
really stores what it is sent.

    python3 databricks_mock.py            # seeds 5000 orders, chunks of 500
    python3 databricks_mock.py 200 50     # 200 orders, chunks of 50

Point a Databricks connector at http://localhost:8099 — any token, any warehouse id.

The SQL you send is really executed, against orders.db beside this file. A WHERE clause filters,
a projection changes the columns, a bad query fails the statement. Generating rows procedurally
would have made every query return the same thing, which is the one behaviour worth not faking.
"""

import json
import os
import re
import sqlite3
import sys
import uuid
from pymongo import MongoClient
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROWS = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
CHUNK = int(sys.argv[2]) if len(sys.argv) > 2 else 500
PORT = 8099
# Loopback when run on a laptop: a mock has no business being reachable from the network,
# and macOS asks the user for permission the first time something binds broadly. A
# container overrides it, because there a published port is the only way in.
BIND = os.environ.get("MOCK_BIND", "127.0.0.1")
# How many polls a statement stays PENDING before it succeeds. One imitates a warehouse waking up;
# raise it to watch the platform wait through a genuinely slow query.
PENDING_POLLS = int(os.environ.get("MOCK_PENDING_POLLS", "1"))

# Result chunks that refuse before they work, as "index x times": MOCK_FAIL_CHUNKS="3x2,7x99"
# makes chunk 3 fail twice and then succeed, and chunk 7 fail far more often than the pipeline
# will retry it. A destination that is briefly unavailable and one that is permanently broken lead
# to opposite outcomes in a migration, and neither can be demonstrated by a mock that always works.
def _failures():
    spec = os.environ.get("MOCK_FAIL_CHUNKS", "").strip()
    plan = {}
    for part in filter(None, (p.strip() for p in spec.split(","))):
        index, _, times = part.partition("x")
        plan[int(index)] = int(times or 1)
    return plan


FAIL_CHUNKS = _failures()
# How many times each chunk has already been refused, so "fail twice then work" is possible.
REFUSED = {}

# Faults for the one-statement-per-chunk mode, keyed by the OFFSET in the query rather than by a
# result-chunk index — that is the only thing identifying a chunk when every chunk has its own
# statement.
#
#   MOCK_SLOW_OFFSETS="2000"          the query at OFFSET 2000 outlasts the wait and must be polled
#   MOCK_FAIL_OFFSETS="3000x2,4000x99"  it is refused that many times before it works
SLOW_OFFSETS = {int(o) for o in filter(None, (
    o.strip() for o in os.environ.get("MOCK_SLOW_OFFSETS", "").split(",")))}


def _offset_failures():
    plan = {}
    for part in filter(None, (p.strip() for p in
                              os.environ.get("MOCK_FAIL_OFFSETS", "").split(","))):
        offset, _, times = part.partition("x")
        plan[int(offset)] = int(times or 1)
    return plan


FAIL_OFFSETS = _offset_failures()
REFUSED_OFFSETS = {}


def offset_of(sql):
    match = re.search(r"OFFSET\s+(\d+)", sql or "", re.IGNORECASE)
    return int(match.group(1)) if match else None
DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "orders.db")

# The "customer's system": where POST /ingest puts what it is given.
MONGO_URI = os.environ.get("MOCK_MONGO_URI", "mongodb://localhost:27018/?directConnection=true")
MONGO = MongoClient(MONGO_URI)
INGESTED = MONGO["dmp"]["dbx_ingest"]

# statement id -> {"columns": [...], "rows": [[...]]} or {"error": "..."}
RESULTS = {}
POLLS = {}


def seed():
    fresh = not os.path.exists(DB)
    db = sqlite3.connect(DB)
    if fresh:
        db.execute("""CREATE TABLE orders (
            order_id TEXT PRIMARY KEY, customer_id TEXT, region TEXT,
            status TEXT, amount REAL, placed_at TEXT)""")
        regions = ["APAC", "EMEA", "AMER"]
        statuses = ["NEW", "PAID", "SHIPPED", "CANCELLED"]
        db.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?)", [
            (f"DBX-{100000 + i}", f"CUST-{i % 997:04d}", regions[i % 3], statuses[i % 4],
             round(50 + (i % 500) * 1.37, 2), f"2026-07-{(i % 28) + 1:02d}T00:00:00Z")
            for i in range(ROWS)
        ])
        db.commit()
    count = db.execute("SELECT count(*) FROM orders").fetchone()[0]
    db.close()
    return count, fresh


def run(sql):
    """Executes the statement and keeps its whole result. Real warehouses stream; this does not."""
    try:
        db = sqlite3.connect(DB)
        cursor = db.execute(sql)
        columns = [c[0] for c in cursor.description or []]
        # Every value is a string: that is how the Statement Execution API returns a JSON_ARRAY
        # result, whatever the column's declared type.
        rows = [["" if v is None else str(v) for v in row] for row in cursor.fetchall()]
        db.close()
        return {"columns": columns, "rows": rows}
    except Exception as e:
        return {"error": str(e)}


def manifest(result):
    rows, total = result["rows"], len(result["rows"])
    bounds = [(o, min(CHUNK, total - o)) for o in range(0, total, CHUNK)] or [(0, 0)]
    return {
        "schema": {"column_count": len(result["columns"]),
                   "columns": [{"name": c, "type_name": "STRING", "position": i}
                               for i, c in enumerate(result["columns"])]},
        "total_chunk_count": len(bounds),
        "total_row_count": total,
        "truncated": False,
        "chunks": [{"chunk_index": i, "row_offset": o, "row_count": n}
                   for i, (o, n) in enumerate(bounds)],
    }


class Handler(BaseHTTPRequestHandler):

    def log_message(self, *args):
        print(" ", self.command, self.path.split("?")[0])

    def fail(self, status, code, message):
        payload = json.dumps({"error_code": code, "message": message}).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def send(self, body):
        payload = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        path = self.path.split("?")[0]

        if "/sql/warehouses/" in path:
            return self.send({"id": path.rsplit("/", 1)[-1], "state": "RUNNING"})

        chunk = re.search(r"/sql/statements/([^/]+)/result/chunks/(\d+)", path)
        if chunk:
            index = int(chunk.group(2))
            budget = FAIL_CHUNKS.get(index, 0)
            already = REFUSED.get(index, 0)
            if already < budget:
                REFUSED[index] = already + 1
                print(f"  chunk {index}: refusing with 503 "
                      f"(failure {already + 1} of {budget})")
                # A real transient failure, not an empty result: 503 is what a warehouse returns
                # while it is failing over, and it is the case the platform must retry rather than
                # treat as "this chunk had no rows".
                return self.fail(503, "SERVICE_UNAVAILABLE",
                                 f"Result chunk {index} is temporarily unavailable")
            result = RESULTS.get(chunk.group(1), {"rows": []})
            start = index * CHUNK
            return self.send({"data_array": result["rows"][start:start + CHUNK]})

        statement = re.search(r"/sql/statements/([^/]+)$", path)
        if statement:
            sid = statement.group(1)
            result = RESULTS.get(sid, {"error": "no such statement"})

            if "error" in result:
                return self.send({"statement_id": sid, "status": {
                    "state": "FAILED",
                    "error": {"error_code": "SQL_ERROR", "message": result["error"]}}})

            # PENDING for the first few polls, imitating a warehouse that has to wake up and then
            # run the query. Raise MOCK_PENDING_POLLS to watch the platform wait: the run sits in
            # PREPARING, no worker is held, and nothing is read until the state turns SUCCEEDED.
            seen = POLLS.get(sid, 0)
            POLLS[sid] = seen + 1
            if seen < PENDING_POLLS:
                print(f"  statement {sid[:8]} still PENDING (poll {seen + 1} of {PENDING_POLLS})")
                return self.send({"statement_id": sid, "status": {"state": "PENDING"}})
            if seen == PENDING_POLLS:
                print(f"  statement {sid[:8]} SUCCEEDED after {PENDING_POLLS} pending poll(s)")

            # INLINE returns the first chunk of rows in the statement response itself, which is
            # what a per-chunk query relies on: one round trip, no result left on the warehouse.
            # EXTERNAL_LINKS callers ignore this and ask for /result/chunks/N instead.
            return self.send({"statement_id": sid, "status": {"state": "SUCCEEDED"},
                              "manifest": manifest(result),
                              "result": {"chunk_index": 0, "row_offset": 0,
                                         "row_count": min(CHUNK, len(result["rows"])),
                                         "data_array": result["rows"][:CHUNK]}})

        self.send({})

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}"

        if self.path.split("?")[0].endswith("/ingest"):
            return self.ingest(raw)

        body = json.loads(raw)
        sql = body.get("statement", "SELECT * FROM orders")

        # Refused outright, before the query is even run — a warehouse failing over, or a
        # statement the workspace will not accept right now.
        offset = offset_of(sql)
        budget = FAIL_OFFSETS.get(offset, 0)
        already = REFUSED_OFFSETS.get(offset, 0)
        if budget and already < budget:
            REFUSED_OFFSETS[offset] = already + 1
            print(f"  OFFSET {offset}: refusing with 503 (failure {already + 1} of {budget})")
            return self.fail(503, "SERVICE_UNAVAILABLE",
                             f"The warehouse is temporarily unavailable")

        sid = str(uuid.uuid4())
        RESULTS[sid] = run(sql)
        print(f"  ran: {sql[:80]} -> {len(RESULTS[sid].get('rows', []))} rows"
              f"{' [' + RESULTS[sid]['error'] + ']' if 'error' in RESULTS[sid] else ''}")

        # A submission that asked to wait gets its answer here, in one request. Every real caller
        # of this API uses that form for a query it expects to be quick, and a mock that always
        # answered PENDING made a one-call integration impossible to test.
        wait = body.get("wait_timeout", "0s")
        waits = wait not in ("0s", "0", 0, None)
        result = RESULTS[sid]
        # A query that outlasts the wait: the workspace keeps running it and hands back an id, and
        # the caller has to poll. The case the single-call form has to degrade to gracefully.
        if offset in SLOW_OFFSETS:
            print(f"  OFFSET {offset}: outlasting the {wait} wait — will need polling")
            return self.send({"statement_id": sid, "status": {"state": "PENDING"}})
        if waits and "error" not in result:
            POLLS[sid] = PENDING_POLLS  # already settled; a later poll returns the same answer
            print(f"  statement {sid[:8]} SUCCEEDED inside the {wait} wait")
            return self.send({"statement_id": sid, "status": {"state": "SUCCEEDED"},
                              "manifest": manifest(result),
                              "result": {"chunk_index": 0, "row_offset": 0,
                                         "row_count": min(CHUNK, len(result["rows"])),
                                         "data_array": result["rows"][:CHUNK]}})

        self.send({"statement_id": sid, "status": {"state": "PENDING"}})

    def ingest(self, raw):
        """The customer's API. Takes a batch and stores it, exactly as a real one would.

        The whole batch or none of it, which is how an ordinary API answers: one status for the
        call, no verdict per record. Only a few destinations — a Salesforce bulk job, a loader that
        returns a row-by-row report — say more than that, and building the mock around the rare
        case made the common one untestable.
        """
        payload = json.loads(raw)
        records = payload if isinstance(payload, list) else payload.get("records", [payload])
        if records:
            INGESTED.insert_many(records, ordered=False)
        print(f"  ingest: {len(records)} record(s), {INGESTED.estimated_document_count()} stored")
        self.send({"accepted": len(records)})

    def do_DELETE(self):
        self.send({})


count, fresh = seed()
print(f"  POST /ingest -> mongodb dmp.dbx_ingest")
print(f"http://localhost:{PORT} — orders.db has {count} rows"
      f"{' (seeded)' if fresh else ' (existing; delete orders.db to reseed)'}, chunks of {CHUNK}")
ThreadingHTTPServer((BIND, PORT), Handler).serve_forever()
