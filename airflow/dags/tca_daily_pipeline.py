"""Daily Transaction Cost Analysis pipeline.

ingest -> validate -> transform -> reconcile -> report

TCA_TARGET=local      runs every step on DuckDB with the CSVs in data/ (no cloud accounts needed)
TCA_TARGET=snowflake  uploads the CSVs to S3, COPYs them into Snowflake, and runs the same SQL there
                      (needs apache-airflow-providers-snowflake and -amazon, and connections
                       `snowflake_tca` and `aws_tca` configured in Airflow)
"""
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task

PROJECT = Path(os.environ.get("TCA_PROJECT_DIR", Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(PROJECT / "scripts"))
import tca_pipeline as tca  # noqa: E402

TARGET = os.environ.get("TCA_TARGET", "local")
DUCKDB_FILE = str(PROJECT / "tca_local.duckdb")
S3_BUCKET = os.environ.get("TCA_S3_BUCKET", "")
log = logging.getLogger(__name__)


def alert_on_failure(context):
    """Failure callback: log a clear alert line. Swap for Slack/email/PagerDuty in production."""
    ti = context["task_instance"]
    log.error("TCA ALERT: task %s failed in run %s (try %s)", ti.task_id, context["run_id"], ti.try_number)


def connect():
    if TARGET == "snowflake":
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        return SnowflakeHook(snowflake_conn_id="snowflake_tca").get_conn().cursor()
    import duckdb
    return duckdb.connect(DUCKDB_FILE)


def run_sql_file(cur, path):
    for stmt in tca.statements(path):
        cur.execute(stmt)


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
}


@dag(
    dag_id="tca_daily_pipeline",
    schedule="30 17 * * 1-5",          # weekdays after the US market close
    start_date=datetime(2025, 9, 1),
    catchup=False,
    default_args=default_args,
    tags=["tca", "snowflake"],
)
def tca_daily_pipeline():

    @task
    def ingest():
        if TARGET == "snowflake":
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            s3 = S3Hook(aws_conn_id="aws_tca")
            for name in ["orders", "executions", "market_prices"]:
                s3.load_file(str(tca.DATA / f"{name}.csv"), key=f"tca/raw/{name}.csv", bucket_name=S3_BUCKET, replace=True)
            cur = connect()
            run_sql_file(cur, tca.SQL / "01_load_from_s3.sql")
            return "loaded from S3"
        return tca.ingest_local(connect())

    @task
    def validate():
        if TARGET == "snowflake":
            cur = connect()
            results = {}
            for stmt in tca.statements(tca.SQL / "02_validate.sql"):
                cur.execute(stmt)
                name, n = cur.fetchone()
                results[name] = n
            failures = {k: v for k, v in results.items() if v > tca.BLOCKING_THRESHOLDS.get(k, 0)}
            if failures:
                raise ValueError(f"Validation failed: {failures}")
            return results
        return tca.validate(connect())

    @task
    def transform():
        if TARGET == "snowflake":
            run_sql_file(connect(), tca.SQL / "03_transform.sql")
            return "transformed"
        return tca.transform(connect())

    @task
    def reconcile():
        if TARGET == "snowflake":
            cur = connect()
            cur.execute("SELECT (SELECT SUM(exec_qty) FROM analytics.clean_executions),"
                        "       (SELECT SUM(filled_qty) FROM analytics.trade_tca)")
            clean, modeled = cur.fetchone()
            if clean != modeled:
                raise ValueError(f"Reconciliation failed: {clean} vs {modeled}")
            return {"clean_shares": int(clean), "modeled_shares": int(modeled)}
        return tca.reconcile(connect())

    @task
    def report():
        if TARGET == "snowflake":
            return "daily_tca_summary refreshed in Snowflake; served by the C# API"
        return tca.report(connect()).to_dict(orient="records")

    ingest() >> validate() >> transform() >> reconcile() >> report()


tca_daily_pipeline()
