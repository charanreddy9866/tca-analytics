--liquibase formatted sql

--changeset charan:001-create-schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;
--rollback DROP SCHEMA IF EXISTS analytics;
--rollback DROP SCHEMA IF EXISTS raw;

--changeset charan:002-create-raw-orders
CREATE TABLE raw.orders (
    order_id        NUMBER(38,0)  NOT NULL,
    trade_date      DATE          NOT NULL,
    symbol          VARCHAR(16)   NOT NULL,
    side            VARCHAR(4)    NOT NULL,
    order_qty       NUMBER(18,0)  NOT NULL,
    arrival_ts      TIMESTAMP_NTZ NOT NULL,
    arrival_price   NUMBER(18,4)  NOT NULL,
    broker          VARCHAR(32),
    strategy        VARCHAR(32),
    commission_bps  NUMBER(8,2),
    CONSTRAINT pk_orders PRIMARY KEY (order_id)
);
--rollback DROP TABLE raw.orders;

--changeset charan:003-create-raw-executions
CREATE TABLE raw.executions (
    exec_id     NUMBER(38,0),
    order_id    NUMBER(38,0),
    exec_ts     TIMESTAMP_NTZ,
    exec_qty    NUMBER(18,0),
    exec_price  NUMBER(18,4),
    venue       VARCHAR(16)
);
--rollback DROP TABLE raw.executions;

--changeset charan:004-create-raw-market-prices
CREATE TABLE raw.market_prices (
    symbol       VARCHAR(16)   NOT NULL,
    bar_ts       TIMESTAMP_NTZ NOT NULL,
    close_price  NUMBER(18,4)  NOT NULL,
    volume       NUMBER(18,0)  NOT NULL
)
CLUSTER BY (symbol, TO_DATE(bar_ts));
--rollback DROP TABLE raw.market_prices;
