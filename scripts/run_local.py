"""Run the full TCA pipeline locally on DuckDB: ingest -> validate -> transform -> reconcile -> report."""
import duckdb

from tca_pipeline import ingest_local, reconcile, report, transform, validate

con = duckdb.connect()
print("ingest:", ingest_local(con))
print("validate:", validate(con))
print("transform: trade_tca rows =", transform(con))
print("reconcile:", reconcile(con))
print("\nBroker scorecard (positive = cost to the client)\n")
print(report(con).to_string(index=False))
