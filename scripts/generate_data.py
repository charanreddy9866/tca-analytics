"""Generate synthetic US equity trading data for transaction cost analysis (TCA).

Outputs (data/), matching the raw tables in liquibase/changelog:
  market_prices.csv  one-minute bars: symbol, bar_ts, close_price, volume
  orders.csv         parent orders with arrival price, broker, strategy, commission
  executions.csv     child fills: exec_ts, exec_qty, exec_price, venue

Prices follow a random walk. Fills are drawn around the prevailing price with a
cost that depends on order size, broker and strategy, so the TCA results have
realistic structure. A small number of bad execution records are injected for
the validation and reconciliation checks.
"""
import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

random.seed(11)
OUT = Path(__file__).resolve().parent.parent / "data"
OUT.mkdir(exist_ok=True)

SYMBOLS = {"AAPL": 228, "MSFT": 410, "NVDA": 118, "AMZN": 178, "GOOGL": 160, "META": 520,
           "JPM": 210, "XOM": 117, "JNJ": 165, "V": 275, "PG": 168, "KO": 70, "CSCO": 48, "INTC": 21}
BROKERS = {"Broker A": 0.7, "Broker B": 0.9, "Broker C": 1.1, "Broker D": 1.4}        # cost multipliers
STRATEGIES = {"VWAP": 1.0, "TWAP": 1.1, "POV": 0.9, "Arrival Price": 0.8}             # cost multipliers
COMMISSION_BPS = {"Broker A": 1.5, "Broker B": 1.0, "Broker C": 1.2, "Broker D": 0.8}
VENUES = ["NYSE", "NASDAQ", "ARCA", "BATS", "IEX", "DARK"]

days, d = [], date(2025, 9, 2)
while len(days) < 21:                                   # one month of trading days
    if d.weekday() < 5:
        days.append(d)
    d += timedelta(days=1)

# Market prices: 09:30-15:59 one-minute bars
prices, px_at = [], {}
for sym, p0 in SYMBOLS.items():
    px = p0
    for day in days:
        for m in range(390):
            ts = datetime.combine(day, datetime.min.time()) + timedelta(hours=9, minutes=30 + m)
            px *= 1 + random.gauss(0, 0.0006)
            vol = int(random.lognormvariate(9, 0.6) * (1.7 if m < 30 or m > 360 else 1.0))
            prices.append({"symbol": sym, "bar_ts": ts.isoformat(sep=" "), "close_price": round(px, 4), "volume": vol})
            px_at[(sym, ts)] = px

orders, execs, exec_id = [], [], 1
for oid in range(1, 1501):
    sym, day = random.choice(list(SYMBOLS)), random.choice(days)
    side = random.choice(["BUY", "SELL"])
    sign = 1 if side == "BUY" else -1
    qty = random.choice([300, 500, 1000, 2100, 5000, 10000, 25000])
    start = random.randint(0, 300)
    arrival = datetime.combine(day, datetime.min.time()) + timedelta(hours=9, minutes=30 + start)
    broker, strat = random.choice(list(BROKERS)), random.choice(list(STRATEGIES))
    orders.append({"order_id": oid, "trade_date": day.isoformat(), "symbol": sym, "side": side,
                   "order_qty": qty, "arrival_ts": arrival.isoformat(sep=" "),
                   "arrival_price": round(px_at[(sym, arrival)], 4), "broker": broker,
                   "strategy": strat, "commission_bps": COMMISSION_BPS[broker]})

    to_fill = int(qty * random.choices([1.0, 0.85, 0.5], [0.85, 0.1, 0.05])[0])
    n_fills = max(1, min(25, qty // 400))
    duration = random.randint(5, 85)
    size_bps = 1.0 + 6 * qty / 25000                    # larger orders cost more
    filled = 0
    for i in range(n_fills):
        q = to_fill - filled if i == n_fills - 1 else to_fill // n_fills
        if q <= 0:
            continue
        minute = int(duration * (i + 1) / n_fills)
        t = arrival + timedelta(minutes=minute, seconds=random.randint(1, 59))
        mkt = px_at[(sym, arrival + timedelta(minutes=minute))]
        cost_bps = random.gauss(size_bps * BROKERS[broker] * STRATEGIES[strat], 2.5)
        execs.append({"exec_id": exec_id, "order_id": oid, "exec_ts": t.isoformat(sep=" "), "exec_qty": q,
                      "exec_price": round(mkt * (1 + sign * cost_bps / 1e4), 4), "venue": random.choice(VENUES)})
        exec_id += 1
        filled += q

# Inject bad records (kept under the pipeline's blocking thresholds)
for _ in range(8):                                       # duplicate fills (same exec_id)
    execs.append(dict(random.choice(execs)))
for e in random.sample(execs, 4):                        # fills for unknown orders
    execs.append({**e, "exec_id": exec_id, "order_id": 99999}); exec_id += 1
for e in random.sample(execs, 3):                        # zero price
    execs.append({**e, "exec_id": exec_id, "exec_price": 0}); exec_id += 1
for e in random.sample(execs, 3):                        # zero quantity
    execs.append({**e, "exec_id": exec_id, "exec_qty": 0}); exec_id += 1


def write(name, rows):
    with open(OUT / f"{name}.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    print(f"{name}: {len(rows):,} rows")


write("market_prices", prices)
write("orders", orders)
write("executions", execs)
