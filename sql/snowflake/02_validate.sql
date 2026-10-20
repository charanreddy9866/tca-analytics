-- Data validation on raw executions. Each statement returns (check_name, failing_rows).
-- The Airflow DAG fails the run if any check marked "blocking" returns rows above its threshold.

SELECT 'duplicate_exec_id' AS check_name, COUNT(*) AS failing_rows
FROM (SELECT exec_id FROM raw.executions GROUP BY exec_id HAVING COUNT(*) > 1) d;

SELECT 'non_positive_exec_price' AS check_name, COUNT(*) AS failing_rows
FROM raw.executions WHERE exec_price IS NULL OR exec_price <= 0;

SELECT 'non_positive_exec_qty' AS check_name, COUNT(*) AS failing_rows
FROM raw.executions WHERE exec_qty IS NULL OR exec_qty <= 0;

SELECT 'orphan_executions' AS check_name, COUNT(*) AS failing_rows
FROM raw.executions e
LEFT JOIN raw.orders o ON o.order_id = e.order_id
WHERE o.order_id IS NULL;

SELECT 'exec_before_arrival' AS check_name, COUNT(*) AS failing_rows
FROM raw.executions e
JOIN raw.orders o ON o.order_id = e.order_id
WHERE e.exec_ts < o.arrival_ts;

SELECT 'overfilled_orders' AS check_name, COUNT(*) AS failing_rows
FROM (
    SELECT o.order_id
    FROM raw.orders o
    JOIN (SELECT DISTINCT * FROM raw.executions) e ON e.order_id = o.order_id
    WHERE e.exec_qty > 0 AND e.exec_price > 0
    GROUP BY o.order_id, o.order_qty
    HAVING SUM(e.exec_qty) > o.order_qty
) x;
