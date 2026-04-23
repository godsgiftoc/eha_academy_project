"""
Generate synthetic sales data into a SQLite database.

  python3 generate_sales.py              # reset source.db with 2000 orders
  python3 generate_sales.py --append 100 # add 100 more orders (for incremental demo)
"""
from __future__ import annotations

import argparse
import random
import sqlite3
from datetime import date, timedelta
from pathlib import Path

REGIONS = ["North", "South", "East", "West", "Central"]
PRODUCTS = ["Widget-A", "Widget-B", "Gadget-X", "Gadget-Y", "Thingamajig"]
START = date(2026, 1, 1)
DAYS = 90

DB_PATH = Path(__file__).parent / "source.db"

# order_id is PK so duplicates at source are impossible; quantity/unit_price
# are nullable so the Silver step has dirty data to filter.
DDL = """
CREATE TABLE IF NOT EXISTS source_orders (
    order_id    TEXT PRIMARY KEY,
    order_date  TEXT NOT NULL,
    region      TEXT NOT NULL,
    product     TEXT NOT NULL,
    quantity    INTEGER,
    unit_price  REAL
)
"""


def _make_row(i: int) -> tuple:
    d = START + timedelta(days=random.randint(0, DAYS - 1))
    # Every 137th row has no quantity, every 211th has no price.
    qty = None if i % 137 == 0 else random.randint(1, 20)
    price = None if i % 211 == 0 else round(random.uniform(5.0, 250.0), 2)
    return (f"ORD-{i:05d}", d.isoformat(), random.choice(REGIONS),
            random.choice(PRODUCTS), qty, price)


def reset(n: int = 2000) -> None:
    DB_PATH.unlink(missing_ok=True)
    random.seed(42)  # deterministic seed data
    with sqlite3.connect(DB_PATH) as con:
        con.execute(DDL)
        con.executemany(
            "INSERT INTO source_orders VALUES (?, ?, ?, ?, ?, ?)",
            (_make_row(i) for i in range(1, n + 1)),
        )
    print(f"Reset {DB_PATH}: {n} rows")


def append(n: int) -> None:
    with sqlite3.connect(DB_PATH) as con:
        con.execute(DDL)
        existing = con.execute("SELECT COUNT(*) FROM source_orders").fetchone()[0]
        # Seed from existing count so repeated --append calls stay reproducible.
        random.seed(existing)
        con.executemany(
            "INSERT OR IGNORE INTO source_orders VALUES (?, ?, ?, ?, ?, ?)",
            (_make_row(i) for i in range(existing + 1, existing + n + 1)),
        )
        total = con.execute("SELECT COUNT(*) FROM source_orders").fetchone()[0]
    print(f"Appended {n} rows to {DB_PATH}: total {total}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--append", type=int, metavar="N",
                   help="add N new orders to existing db (default: reset to 2000)")
    args = p.parse_args()
    if args.append:
        append(args.append)
    else:
        reset()
