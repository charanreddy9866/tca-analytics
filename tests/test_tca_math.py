"""Unit tests for the TCA SQL, run on tiny hand-built datasets."""
import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from tca_pipeline import reconcile, transform  # noqa: E402


def build(orders, execs, bars):
    con = duckdb.connect()
    con.execute("CREATE SCHEMA raw; CREATE SCHEMA analytics;")
    con.execute("""CREATE TABLE raw.orders (order_id INT, trade_date DATE, symbol VARCHAR, side VARCHAR, order_qty INT,
                   arrival_ts TIMESTAMP, arrival_price DOUBLE, broker VARCHAR, strategy VARCHAR, commission_bps DOUBLE)""")
    con.execute("""CREATE TABLE raw.executions (exec_id INT, order_id INT, exec_ts TIMESTAMP, exec_qty INT,
                   exec_price DOUBLE, venue VARCHAR)""")
    con.execute("CREATE TABLE raw.market_prices (symbol VARCHAR, bar_ts TIMESTAMP, close_price DOUBLE, volume INT)")
    con.executemany("INSERT INTO raw.orders VALUES (?,?,?,?,?,?,?,?,?,?)", orders)
    con.executemany("INSERT INTO raw.executions VALUES (?,?,?,?,?,?)", execs)
    con.executemany("INSERT INTO raw.market_prices VALUES (?,?,?,?)", bars)
    transform(con)
    return con


BARS = [("XYZ", "2025-09-02 10:00:00", 100.00, 1000),
        ("XYZ", "2025-09-02 10:05:00", 100.10, 1000),
        ("XYZ", "2025-09-02 16:00:00", 100.50, 1000)]


def row(con, order_id):
    return con.execute("SELECT * FROM analytics.trade_tca WHERE order_id = ?", [order_id]).df().iloc[0]


def test_worked_example_buy():
    """Benchmark $100.00, execution $100.15, 1,000 shares -> $0.15/share, $150, 15 bps."""
    con = build([(1, "2025-09-02", "XYZ", "BUY", 1000, "2025-09-02 10:00:00", 100.00, "B", "VWAP", 0)],
                [(1, 1, "2025-09-02 10:05:00", 1000, 100.15, "NYSE")], BARS)
    r = row(con, 1)
    assert r.slippage_per_share == pytest.approx(0.15)
    assert r.slippage_cost == pytest.approx(150.0)
    assert r.arrival_slippage_bps == pytest.approx(15.0)
    assert r.implementation_shortfall == pytest.approx(150.0)   # fully filled, no commission


def test_sell_side_sign():
    """Selling below arrival is a cost, so it must come out positive."""
    con = build([(2, "2025-09-02", "XYZ", "SELL", 1000, "2025-09-02 10:00:00", 100.00, "B", "VWAP", 0)],
                [(1, 2, "2025-09-02 10:05:00", 1000, 99.90, "NYSE")], BARS)
    r = row(con, 2)
    assert r.slippage_cost == pytest.approx(100.0)
    assert r.arrival_slippage_bps == pytest.approx(10.0)


def test_partial_fill_opportunity_cost_and_commission():
    """500 of 1,000 filled; unfilled 500 marked to the 100.50 close; 2 bps commission."""
    con = build([(3, "2025-09-02", "XYZ", "BUY", 1000, "2025-09-02 10:00:00", 100.00, "B", "VWAP", 2)],
                [(1, 3, "2025-09-02 10:05:00", 500, 100.15, "NYSE")], BARS)
    r = row(con, 3)
    commission = 2 / 10000 * 500 * 100.15
    assert r.fill_rate == pytest.approx(0.5)
    assert r.commission_cost == pytest.approx(commission)
    assert r.opportunity_cost == pytest.approx(0.50 * 500)
    assert r.implementation_shortfall == pytest.approx(0.15 * 500 + commission + 0.50 * 500)


def test_interval_vwap_and_bad_rows_filtered():
    """VWAP over arrival..last fill; duplicate and negative-price fills are excluded and shares reconcile."""
    con = build([(4, "2025-09-02", "XYZ", "BUY", 1000, "2025-09-02 10:00:00", 100.00, "B", "VWAP", 0)],
                [(1, 4, "2025-09-02 10:05:00", 1000, 100.15, "NYSE"),
                 (1, 4, "2025-09-02 10:05:00", 1000, 100.15, "NYSE"),     # duplicate exec_id
                 (2, 4, "2025-09-02 10:05:00", 100, -5.00, "NYSE")],     # invalid price
                BARS)
    r = row(con, 4)
    assert r.filled_qty == 1000
    assert r.interval_vwap == pytest.approx(100.05)
    assert reconcile(con) == {"clean_shares": 1000, "modeled_shares": 1000}


def test_validation_blocks_exec_before_arrival():
    """A fill timestamped before the order arrived is a blocking data error: the pipeline must stop."""
    from tca_pipeline import validate
    con = duckdb.connect()
    con.execute("CREATE SCHEMA raw; CREATE SCHEMA analytics;")
    con.execute("""CREATE TABLE raw.orders (order_id INT, trade_date DATE, symbol VARCHAR, side VARCHAR, order_qty INT,
                   arrival_ts TIMESTAMP, arrival_price DOUBLE, broker VARCHAR, strategy VARCHAR, commission_bps DOUBLE)""")
    con.execute("CREATE TABLE raw.executions (exec_id INT, order_id INT, exec_ts TIMESTAMP, exec_qty INT, exec_price DOUBLE, venue VARCHAR)")
    con.execute("INSERT INTO raw.orders VALUES (1, '2025-09-02', 'XYZ', 'BUY', 1000, '2025-09-02 10:00:00', 100, 'B', 'VWAP', 0)")
    con.execute("INSERT INTO raw.executions VALUES (1, 1, '2025-09-02 09:59:00', 1000, 100.1, 'NYSE')")
    with pytest.raises(ValueError, match="exec_before_arrival"):
        validate(con)
