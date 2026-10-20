"""Pipeline steps shared by the local runner and the Airflow DAG.

Locally the steps run on DuckDB, which supports the Snowflake SQL used here
(CREATE OR REPLACE TABLE, QUALIFY, window functions). In Airflow the same SQL
files run on Snowflake.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
SQL = ROOT / "sql" / "snowflake"
REPORTS = ROOT / "reports"

# Checks that should stop the pipeline if they exceed these thresholds.
# Invalid rows are filtered in the transform, so small counts are tolerated;
# a spike means an upstream problem worth stopping for.
BLOCKING_THRESHOLDS = {
    "duplicate_exec_id": 100,
    "non_positive_exec_price": 50,
    "non_positive_exec_qty": 50,
    "orphan_executions": 50,
    "exec_before_arrival": 0,
    "overfilled_orders": 0,
}


def statements(path: Path):
    """Split a .sql file into statements, ignoring comment-only chunks."""
    text = "\n".join(l for l in path.read_text().splitlines() if not l.strip().startswith("--"))
    return [s.strip() for s in text.split(";") if s.strip()]


def ingest_local(con):
    """Create raw tables from the CSVs (stands in for the S3 -> Snowflake COPY step)."""
    con.execute("CREATE SCHEMA IF NOT EXISTS raw; CREATE SCHEMA IF NOT EXISTS analytics;")
    for t in ["orders", "executions", "market_prices"]:
        con.execute(f"CREATE OR REPLACE TABLE raw.{t} AS SELECT * FROM read_csv_auto('{DATA / (t + '.csv')}', header=true)")
    return {t: con.execute(f"SELECT COUNT(*) FROM raw.{t}").fetchone()[0] for t in ["orders", "executions", "market_prices"]}


def validate(con):
    """Run validation checks; raise if a blocking threshold is exceeded."""
    results = dict(con.execute(s).fetchone() for s in statements(SQL / "02_validate.sql"))
    failures = {k: v for k, v in results.items() if v > BLOCKING_THRESHOLDS.get(k, 0)}
    if failures:
        raise ValueError(f"Validation failed: {failures}")
    return results


def transform(con):
    for s in statements(SQL / "03_transform.sql"):
        con.execute(s)
    return con.execute("SELECT COUNT(*) FROM analytics.trade_tca").fetchone()[0]


def reconcile(con):
    """Reconcile shares: every clean executed share must land in trade_tca exactly once."""
    clean = con.execute("SELECT SUM(exec_qty) FROM analytics.clean_executions").fetchone()[0]
    modeled = con.execute("SELECT SUM(filled_qty) FROM analytics.trade_tca").fetchone()[0]
    if clean != modeled:
        raise ValueError(f"Reconciliation failed: {clean} clean shares vs {modeled} in trade_tca")
    return {"clean_shares": int(clean), "modeled_shares": int(modeled)}


def report(con):
    REPORTS.mkdir(exist_ok=True)
    for t in ["trade_tca", "daily_tca_summary"]:
        con.execute(f"COPY (SELECT * FROM analytics.{t} ORDER BY 1, 2) TO '{REPORTS / (t + '.csv')}' (HEADER)")
    broker = con.execute("""
        SELECT broker,
               COUNT(*) AS orders,
               ROUND(SUM(executed_notional) / 1e6, 1) AS notional_mm,
               ROUND(SUM(arrival_slippage_bps * executed_notional) / SUM(executed_notional), 2) AS arrival_slippage_bps,
               ROUND(SUM(vwap_slippage_bps * executed_notional) / SUM(executed_notional), 2) AS vwap_slippage_bps,
               ROUND(SUM(implementation_shortfall) / SUM(order_qty * arrival_price) * 10000, 2) AS is_bps,
               ROUND(SUM(implementation_shortfall), 0) AS is_cost_usd
        FROM analytics.trade_tca GROUP BY broker ORDER BY is_bps
    """).df()
    broker.to_csv(REPORTS / "broker_scorecard.csv", index=False)
    return broker
