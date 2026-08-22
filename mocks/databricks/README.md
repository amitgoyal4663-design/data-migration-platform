Fake Databricks SQL API, so the connector can run without an account.

    python3 databricks_mock.py            # 5000 rows, chunks of 500
    python3 databricks_mock.py 200 50     # 200 rows, chunks of 50

Then point a Databricks connector at http://localhost:8099 — any token, any warehouse id.

The chunk size is what the run's chunks are planned from: the connector reads the result manifest
rather than doing arithmetic, so changing it changes how the migration is divided.

Not part of the platform. Nothing builds it and no test imports it.
