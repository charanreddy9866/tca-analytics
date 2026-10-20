-- Load raw CSVs from the S3 stage into Snowflake (stage created by Liquibase changeset 006).
-- Truncate-and-load keeps the job idempotent: rerunning a day does not duplicate rows.

TRUNCATE TABLE raw.orders;
COPY INTO raw.orders        FROM @raw.tca_s3_stage/orders.csv        ON_ERROR = 'ABORT_STATEMENT';

TRUNCATE TABLE raw.executions;
COPY INTO raw.executions    FROM @raw.tca_s3_stage/executions.csv    ON_ERROR = 'ABORT_STATEMENT';

TRUNCATE TABLE raw.market_prices;
COPY INTO raw.market_prices FROM @raw.tca_s3_stage/market_prices.csv ON_ERROR = 'ABORT_STATEMENT';
