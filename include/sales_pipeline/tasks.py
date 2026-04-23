"""
Business logic for the sales_pipeline DAG.

Medallion pattern with idempotent, incremental layers:
  Bronze: append-only from SQLite source (staging + ON CONFLICT DO NOTHING).
  Silver: incremental insert of clean+typed rows not yet in silver.
  Gold:   always rebuilt (it's a small aggregate; recomputation is safe).

Running the DAG twice with no source changes is a true no-op.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd
import sqlalchemy as sa

log = logging.getLogger(__name__)

DATA_DIR = Path("/opt/airflow/data")
REPORTS_DIR = Path("/opt/airflow/reports")


def _source_url() -> str:
    # Default points at the bind-mounted SQLite file inside the container.
    return os.environ.get("SOURCE_URL", f"sqlite:///{DATA_DIR}/source.db")


def _warehouse_url() -> str:
    return os.environ["WAREHOUSE_URL"]


def ingest_bronze() -> None:
    """Append new source rows into bronze; skip order_ids already present."""
    src = sa.create_engine(_source_url())
    tgt = sa.create_engine(_warehouse_url())

    # Full snapshot of source → staging. Pandas handles type inference.
    df = pd.read_sql("SELECT * FROM source_orders", src)
    df.to_sql("stg_sales", _warehouse_url(), if_exists="replace", index=False)
    log.info("Staged %d source rows", len(df))

    with tgt.begin() as conn:
        # First run: create bronze as an empty copy of staging.
        # Unique index on order_id gives ON CONFLICT something to match on.
        conn.execute(sa.text(
            "CREATE TABLE IF NOT EXISTS bronze_sales AS "
            "SELECT * FROM stg_sales WHERE 1=0"
        ))
        conn.execute(sa.text(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_bronze_order_id "
            "ON bronze_sales(order_id)"
        ))

        before = conn.execute(sa.text("SELECT COUNT(*) FROM bronze_sales")).scalar()
        conn.execute(sa.text(
            "INSERT INTO bronze_sales SELECT * FROM stg_sales "
            "ON CONFLICT (order_id) DO NOTHING"
        ))
        after = conn.execute(sa.text("SELECT COUNT(*) FROM bronze_sales")).scalar()

    new = after - before
    if new == 0:
        log.info("No new rows; bronze unchanged (total=%d)", after)
    else:
        log.info("Bronze: +%d new rows (total=%d)", new, after)


def transform_silver() -> None:
    """Insert cleaned + typed rows from bronze into silver (incremental)."""
    tgt = sa.create_engine(_warehouse_url())

    with tgt.begin() as conn:
        # Silver has a strict schema — this is where we commit to types.
        conn.execute(sa.text("""
            CREATE TABLE IF NOT EXISTS silver_sales (
                order_id    TEXT PRIMARY KEY,
                order_date  DATE          NOT NULL,
                region      TEXT          NOT NULL,
                product     TEXT          NOT NULL,
                quantity    INTEGER       NOT NULL,
                unit_price  NUMERIC(10,2) NOT NULL,
                revenue     NUMERIC(14,2) NOT NULL
            )
        """))

        before = conn.execute(sa.text("SELECT COUNT(*) FROM silver_sales")).scalar()
        # Only pull bronze rows not yet in silver, filter nulls, cast, derive.
        conn.execute(sa.text("""
            INSERT INTO silver_sales
                (order_id, order_date, region, product, quantity, unit_price, revenue)
            SELECT
                b.order_id,
                b.order_date::DATE,
                b.region,
                b.product,
                b.quantity::INTEGER,
                b.unit_price::NUMERIC(10,2),
                (b.quantity::NUMERIC * b.unit_price::NUMERIC)::NUMERIC(14,2)
            FROM bronze_sales b
            LEFT JOIN silver_sales s USING (order_id)
            WHERE s.order_id IS NULL
              AND b.quantity   IS NOT NULL
              AND b.unit_price IS NOT NULL
        """))
        after = conn.execute(sa.text("SELECT COUNT(*) FROM silver_sales")).scalar()

    new = after - before
    if new == 0:
        log.info("No new rows; silver unchanged (total=%d)", after)
    else:
        log.info("Silver: +%d new rows (total=%d)", new, after)


def aggregate_gold() -> None:
    """Rebuild gold from silver — aggregate is cheap and always current."""
    tgt = sa.create_engine(_warehouse_url())

    with tgt.begin() as conn:
        conn.execute(sa.text("DROP TABLE IF EXISTS gold_region_kpis"))
        conn.execute(sa.text("""
            CREATE TABLE gold_region_kpis AS
            SELECT
                region,
                COUNT(*)                         AS orders,
                SUM(quantity)                    AS units_sold,
                ROUND(SUM(revenue)::numeric, 2)  AS revenue,
                ROUND(AVG(revenue)::numeric, 2)  AS avg_order_value
            FROM silver_sales
            GROUP BY region
            ORDER BY revenue DESC
        """))
        rows = conn.execute(sa.text(
            "SELECT * FROM gold_region_kpis ORDER BY revenue DESC"
        )).mappings().all()

    log.info("Gold rebuilt: %d region rows", len(rows))
    for r in rows:
        log.info("  %s", dict(r))


def generate_report(run_date: str) -> str:
    """Write a dated CSV summary from gold. Returns the output path."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    gold = pd.read_sql("SELECT * FROM gold_region_kpis", _warehouse_url())
    out = REPORTS_DIR / f"sales_summary_{run_date}.csv"
    gold.to_csv(out, index=False)
    log.info("Report written: %s", out)
    # str() because XCom must be JSON-serializable; PosixPath isn't.
    return str(out)


def on_failure(context: dict) -> None:
    """Log an ALERT line when a task fails after all retries are exhausted."""
    ti = context.get("task_instance")
    log.error("ALERT: task %s (dag=%s) failed on %s",
              ti.task_id, ti.dag_id, context.get("ds"))
