-- TCA transformations. Snowflake SQL (also runs on DuckDB for local testing).
--
-- Conventions
--   side_sign = +1 for BUY, -1 for SELL, so a positive cost always means the trade did worse than the benchmark.
--   bps       = cost / benchmark notional * 10,000

-- 1. Clean executions: drop duplicates, invalid rows, and fills for unknown orders.
CREATE OR REPLACE TABLE analytics.clean_executions AS
SELECT e.*
FROM raw.executions e
JOIN raw.orders o ON o.order_id = e.order_id
WHERE e.exec_price > 0
  AND e.exec_qty > 0
  AND e.exec_ts >= o.arrival_ts
QUALIFY ROW_NUMBER() OVER (PARTITION BY e.exec_id ORDER BY e.exec_ts) = 1;

-- 2. Order-level fills: filled quantity, average execution price, execution window.
CREATE OR REPLACE TABLE analytics.order_fills AS
SELECT
    order_id,
    SUM(exec_qty)                               AS filled_qty,
    SUM(exec_qty * exec_price) / SUM(exec_qty)  AS avg_exec_price,
    MIN(exec_ts)                                AS first_fill_ts,
    MAX(exec_ts)                                AS last_fill_ts,
    COUNT(*)                                    AS fill_count
FROM analytics.clean_executions
GROUP BY order_id;

-- 3. Interval VWAP benchmark: market VWAP between arrival and the last fill.
CREATE OR REPLACE TABLE analytics.interval_vwap AS
SELECT
    o.order_id,
    SUM(m.close_price * m.volume) / NULLIF(SUM(m.volume), 0) AS interval_vwap
FROM raw.orders o
JOIN analytics.order_fills f ON f.order_id = o.order_id
JOIN raw.market_prices m
  ON m.symbol = o.symbol
 AND m.bar_ts >= o.arrival_ts
 AND m.bar_ts <= f.last_fill_ts
GROUP BY o.order_id;

-- 4. Closing price per symbol per day (used for opportunity cost on unfilled shares).
CREATE OR REPLACE TABLE analytics.daily_close AS
SELECT symbol, CAST(bar_ts AS DATE) AS trade_date, close_price AS close_price
FROM raw.market_prices
QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, CAST(bar_ts AS DATE) ORDER BY bar_ts DESC) = 1;

-- 5. Trade-level TCA.
CREATE OR REPLACE TABLE analytics.trade_tca AS
WITH base AS (
    SELECT
        o.order_id, o.trade_date, o.symbol, o.side, o.broker, o.strategy,
        o.order_qty, o.arrival_ts, o.arrival_price, o.commission_bps,
        CASE WHEN o.side = 'BUY' THEN 1 ELSE -1 END       AS side_sign,
        f.filled_qty, f.avg_exec_price, f.fill_count, f.first_fill_ts, f.last_fill_ts,
        v.interval_vwap,
        c.close_price
    FROM raw.orders o
    JOIN analytics.order_fills  f ON f.order_id = o.order_id
    JOIN analytics.interval_vwap v ON v.order_id = o.order_id
    JOIN analytics.daily_close   c ON c.symbol = o.symbol AND c.trade_date = o.trade_date
)
SELECT
    order_id, trade_date, symbol, side, broker, strategy,
    order_qty, filled_qty,
    filled_qty / order_qty                                                AS fill_rate,
    fill_count, arrival_ts, last_fill_ts,
    arrival_price, avg_exec_price, interval_vwap, close_price,
    filled_qty * avg_exec_price                                           AS executed_notional,

    -- Slippage vs. arrival price (the worked example: (100.15 - 100.00) * 1,000 = $150, 15 bps)
    side_sign * (avg_exec_price - arrival_price)                          AS slippage_per_share,
    side_sign * (avg_exec_price - arrival_price) * filled_qty             AS slippage_cost,
    side_sign * (avg_exec_price - arrival_price) / arrival_price * 10000  AS arrival_slippage_bps,

    -- Slippage vs. interval VWAP
    side_sign * (avg_exec_price - interval_vwap) / interval_vwap * 10000  AS vwap_slippage_bps,

    -- Explicit cost
    commission_bps / 10000 * filled_qty * avg_exec_price                  AS commission_cost,

    -- Opportunity cost on unfilled shares, marked to the day's close
    side_sign * (close_price - arrival_price) * (order_qty - filled_qty)  AS opportunity_cost,

    -- Implementation shortfall = execution cost + commissions + opportunity cost
    side_sign * (avg_exec_price - arrival_price) * filled_qty
      + commission_bps / 10000 * filled_qty * avg_exec_price
      + side_sign * (close_price - arrival_price) * (order_qty - filled_qty)
                                                                          AS implementation_shortfall,
    ( side_sign * (avg_exec_price - arrival_price) * filled_qty
      + commission_bps / 10000 * filled_qty * avg_exec_price
      + side_sign * (close_price - arrival_price) * (order_qty - filled_qty)
    ) / (order_qty * arrival_price) * 10000                               AS implementation_shortfall_bps
FROM base;

-- 6. Daily transaction-cost summary by broker and strategy (notional-weighted).
CREATE OR REPLACE TABLE analytics.daily_tca_summary AS
SELECT
    trade_date,
    broker,
    strategy,
    COUNT(*)                                                              AS orders,
    SUM(executed_notional)                                                AS executed_notional,
    SUM(slippage_cost)                                                    AS slippage_cost,
    SUM(commission_cost)                                                  AS commission_cost,
    SUM(implementation_shortfall)                                         AS implementation_shortfall,
    SUM(arrival_slippage_bps * executed_notional) / SUM(executed_notional) AS wavg_arrival_slippage_bps,
    SUM(vwap_slippage_bps * executed_notional) / SUM(executed_notional)    AS wavg_vwap_slippage_bps,
    SUM(implementation_shortfall) / SUM(order_qty * arrival_price) * 10000 AS implementation_shortfall_bps,
    AVG(fill_rate)                                                        AS avg_fill_rate
FROM analytics.trade_tca
GROUP BY trade_date, broker, strategy;
