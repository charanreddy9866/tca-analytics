"""Build dashboard/index.html: a static results page for the TCA pipeline.

Runs the pipeline in memory on DuckDB (ingest -> validate -> transform -> reconcile),
aggregates the results, and injects them as JSON into dashboard/template.html.

Usage (from the repo root): python scripts/build_dashboard.py
"""
import json
from pathlib import Path

import duckdb

import tca_pipeline as tca

ROOT = Path(__file__).resolve().parent.parent
DASH = ROOT / "dashboard"

con = duckdb.connect()
counts = tca.ingest_local(con)
checks = tca.validate(con)
tca.transform(con)
recon = tca.reconcile(con)


def rows(sql):
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, (v.isoformat() if hasattr(v, "isoformat") else v for v in r))) for r in cur.fetchall()]


IS_BPS = "ROUND(SUM(implementation_shortfall) / SUM(order_qty * arrival_price) * 10000, 2)"
WAVG = lambda c: f"ROUND(SUM({c} * executed_notional) / SUM(executed_notional), 2)"

data = {
    "kpis": rows(f"""SELECT COUNT(*) AS orders, SUM(executed_notional) AS notional,
                     SUM(implementation_shortfall) / SUM(order_qty * arrival_price) * 10000 AS is_bps,
                     SUM(implementation_shortfall) AS is_usd, ROUND(AVG(fill_rate) * 100, 1) AS fill_rate,
                     {WAVG('arrival_slippage_bps')} AS arrival_bps, {WAVG('vwap_slippage_bps')} AS vwap_bps
                     FROM analytics.trade_tca""")[0],
    "brokers": rows(f"""SELECT broker, COUNT(*) AS orders, ROUND(SUM(executed_notional) / 1e6, 1) AS notional_mm,
                        {WAVG('arrival_slippage_bps')} AS arrival_bps, {WAVG('vwap_slippage_bps')} AS vwap_bps,
                        {IS_BPS} AS is_bps, ROUND(SUM(implementation_shortfall)) AS is_usd
                        FROM analytics.trade_tca GROUP BY broker ORDER BY is_bps"""),
    "strategies": rows(f"""SELECT strategy, COUNT(*) AS orders, {IS_BPS} AS is_bps, {WAVG('vwap_slippage_bps')} AS vwap_bps
                           FROM analytics.trade_tca GROUP BY strategy ORDER BY is_bps"""),
    "sizes": rows(f"""SELECT CASE WHEN order_qty <= 1000 THEN 'Up to 1K' WHEN order_qty <= 5000 THEN '1K–5K'
                                  WHEN order_qty <= 10000 THEN '5K–10K' ELSE '25K' END AS bucket,
                             MIN(order_qty) AS lo, COUNT(*) AS orders, {IS_BPS} AS is_bps
                      FROM analytics.trade_tca GROUP BY 1 ORDER BY lo"""),
    "daily": rows(f"""SELECT trade_date, broker, {IS_BPS} AS is_bps
                      FROM analytics.trade_tca GROUP BY 1, 2 ORDER BY 1, 2"""),
    "trades": rows("""SELECT order_id, trade_date, symbol, side, broker, strategy, order_qty, filled_qty,
                             ROUND(arrival_price, 4) AS arrival_price, ROUND(avg_exec_price, 4) AS avg_exec_price,
                             ROUND(interval_vwap, 4) AS interval_vwap, ROUND(arrival_slippage_bps, 2) AS arrival_bps,
                             ROUND(vwap_slippage_bps, 2) AS vwap_bps, ROUND(implementation_shortfall, 2) AS is_usd,
                             ROUND(implementation_shortfall_bps, 2) AS is_bps
                      FROM analytics.trade_tca ORDER BY order_id"""),
    "checks": checks,
    "thresholds": tca.BLOCKING_THRESHOLDS,
    "recon": recon,
    "counts": counts,
}

body = (DASH / "template.html").read_text().replace("__DATA__", json.dumps(data, default=float))
html = ('<!doctype html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">\n'
        '<style>body{margin:0}img{max-width:100%}[hidden]{display:none!important}</style>\n</head>\n<body>\n'
        + body + '\n</body>\n</html>\n')
(DASH / "index.html").write_text(html)
print(f"wrote {DASH / 'index.html'} ({len(html) / 1024:.0f} KB)")
