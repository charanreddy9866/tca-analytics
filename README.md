# Transaction Cost Analysis (TCA) Data Platform

A data pipeline that measures **trade execution quality** for US equity orders. It ingests orders, fills and market prices, validates and reconciles them, computes transaction-cost metrics (slippage, VWAP slippage, implementation shortfall), and serves the results through a C# API.

**Stack:** Snowflake SQL · Apache Airflow · Liquibase · C# / ASP.NET Core · Python · Amazon S3 · DuckDB (local runs)

> The dataset is synthetic (`scripts/generate_data.py`) so the project can be shared publicly: one month of one-minute market data for 14 symbols, 1,500 parent orders and ~15,900 fills, with a few bad records injected for the validation checks.

## Architecture

```
data/*.csv ──► Amazon S3 ──► Snowflake RAW ──► validate ──► transform ──► reconcile ──► Snowflake ANALYTICS ──► C# API
               (S3Hook)     (COPY INTO via     (6 checks,    (TCA SQL)    (share-level                          /api/trades/{id}/tca
                             Liquibase stage)   blocking                   check)                               /api/reports/daily
                                                thresholds)                                                     /api/reports/brokers
            └──────────────────────── orchestrated by the Airflow DAG `tca_daily_pipeline` ───────────────────────┘
```

| Component | Path | What it does |
|---|---|---|
| Schema migrations | `liquibase/` | Version-controlled Snowflake DDL with rollbacks: schemas, raw tables (clustered market data), CSV file format, S3 stage |
| Load | `sql/snowflake/01_load_from_s3.sql` | Idempotent truncate-and-load from the S3 stage with `COPY INTO` |
| Validation | `sql/snowflake/02_validate.sql` | Duplicate fills, invalid prices/quantities, orphan fills, fills before arrival, over-filled orders |
| Transform | `sql/snowflake/03_transform.sql` | Clean fills, order-level fills, interval VWAP, daily close, trade-level TCA, daily summary |
| Orchestration | `airflow/dags/tca_daily_pipeline.py` | ingest → validate → transform → reconcile → report; weekday schedule after market close, retries, failure alerting |
| API | `api/TcaApi/` | ASP.NET Core minimal API; reads Snowflake through the Snowflake SQL API with bind variables, or local CSVs |
| Tests | `tests/` | Unit tests for the TCA math (including the worked example) and the validation gate |

## TCA metrics

`side_sign` is +1 for buys and −1 for sells, so a positive number is always a cost to the client.

| Metric | Formula |
|---|---|
| Slippage per share | `side_sign × (avg_exec_price − arrival_price)` |
| Slippage cost | `slippage per share × filled_qty` |
| Arrival slippage (bps) | `slippage per share / arrival_price × 10,000` |
| VWAP slippage (bps) | `side_sign × (avg_exec_price − interval_vwap) / interval_vwap × 10,000` |
| Commission cost | `commission_bps / 10,000 × executed notional` |
| Opportunity cost | `side_sign × (close_price − arrival_price) × unfilled_qty` |
| Implementation shortfall | `slippage cost + commission cost + opportunity cost` |

**Worked example (covered by a unit test):** a buy of 1,000 shares with an arrival price of $100.00 filled at $100.15 has slippage of $0.15 per share, a $150 slippage cost and 15 bps.

## Data quality and reconciliation

Validation runs before any transformation. Each check has a threshold; exceeding it fails the Airflow task and triggers the alert callback. Small counts of bad rows are filtered out in the transform.

| Check | Rows in sample data | Blocking above |
|---|---|---|
| Duplicate fill IDs | 8 | 100 |
| Zero/negative price | 3 | 50 |
| Zero/negative quantity | 3 | 50 |
| Fills for unknown orders | 4 | 50 |
| Fills before order arrival | 0 | 0 |
| Over-filled orders | 0 | 0 |

After the transform, a **reconciliation** step checks that every clean executed share lands in `trade_tca` exactly once (9,665,290 shares in, 9,665,290 out on the sample data).

## Results on the sample data

- **1,500 orders, $1.84B executed notional**, implementation shortfall of **5.7 bps** ($1.10M) overall.
- **Broker scorecard** (implementation shortfall): Broker B 2.1 bps, Broker A 3.3 bps, Broker C 6.2 bps, Broker D 11.2 bps.
- **By strategy:** POV 3.5 bps and Arrival Price 3.9 bps cost less than TWAP 5.8 bps and VWAP 9.4 bps.

## Run it locally (no cloud accounts needed)

```bash
pip install duckdb pandas pytest
python scripts/generate_data.py          # data/*.csv
cd scripts && python run_local.py        # ingest -> validate -> transform -> reconcile -> report
cd .. && python -m pytest tests          # TCA math and validation tests
```

**Airflow (local mode):**
```bash
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/airflow/dags TCA_TARGET=local
airflow db migrate
airflow dags test tca_daily_pipeline 2025-09-30
```

**C# API (local mode, serves reports/*.csv):**
```bash
cd api/TcaApi && dotnet run
curl localhost:5000/api/trades/1/tca
curl "localhost:5000/api/reports/daily?date=2025-09-15"
curl "localhost:5000/api/reports/brokers?from=2025-09-01&to=2025-09-30"
```

## Run it on Snowflake and AWS

1. Create an S3 bucket and a Snowflake storage integration for it (account admin).
2. Copy `liquibase/liquibase.properties.example` to `liquibase.properties`, fill in the connection and parameters, then run `liquibase update`.
3. In Airflow, install `apache-airflow-providers-snowflake` and `apache-airflow-providers-amazon`, create the connections `snowflake_tca` and `aws_tca`, and set `TCA_TARGET=snowflake` and `TCA_S3_BUCKET`.
4. Run the API with `DataSource=Snowflake` and the `Snowflake:Account` / `Snowflake:Token` settings (key-pair JWT or OAuth token) in user secrets or environment variables.
