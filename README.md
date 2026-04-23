# EHA Academy — Build & Monitor a Data Pipeline

Hands-on demo that walks through the full medallion architecture
(**Bronze → Silver → Gold → Report**) using Apache Airflow, PostgreSQL,
pandas, and Docker. Everything runs on your laptop; no cloud account needed.

---

## What this project does

A single Airflow DAG (`sales_pipeline`) reads from a **SQLite source** and
writes into **Postgres** using the medallion pattern. All three warehouse
layers are **idempotent** — running the DAG twice with no source changes is
a true no-op.

| # | Task                | Layer  | Behaviour                                                                    |
|---|---------------------|--------|------------------------------------------------------------------------------|
| 1 | `ingest_bronze`     | Bronze | Stage source → `INSERT ... ON CONFLICT DO NOTHING` by `order_id` (append-only) |
| 2 | `transform_silver`  | Silver | Incremental insert of clean + typed rows not already in silver               |
| 3 | `aggregate_gold`    | Gold   | Full rebuild — aggregate is cheap and always current                         |
| 4 | `generate_report`   | Report | Write a dated summary CSV from gold                                           |

A failure callback logs an `ALERT` line in the Task Logs on final failure.

---

## Stack

| Service   | Port   | Purpose                                                               |
|-----------|--------|-----------------------------------------------------------------------|
| Airflow   | 8080   | Orchestration & scheduling                                             |
| Postgres  | 5432   | Two DBs: `airflow` (metadata) and `warehouse` (medallion tables)       |
| pgAdmin   | 5050   | Browse / query the warehouse in a web UI                               |

---

## Quick start

Prerequisites: **Docker Desktop** running.

```bash
python3 data/generate_sales.py      # seed the SQLite source (one-off)
docker compose build                # build the custom Airflow image
docker compose up -d                # start Postgres, Airflow, pgAdmin
```

First boot takes ~1–2 minutes. Once ready:

| URL                   | Login                               |
|-----------------------|-------------------------------------|
| http://localhost:8080 | `admin` / `admin`                   |
| http://localhost:5050 | `admin@example.com` / `admin` (DB password `airflow` when prompted) |

The DAG is unpaused by default. Trigger a run from the UI or CLI:

```bash
docker exec eha_airflow airflow dags trigger sales_pipeline
```

Then verify:

```bash
docker exec eha_postgres psql -U airflow -d warehouse \
  -c "SELECT 'bronze' AS layer, COUNT(*) FROM bronze_sales
      UNION ALL SELECT 'silver', COUNT(*) FROM silver_sales
      UNION ALL SELECT 'gold',   COUNT(*) FROM gold_region_kpis;"
```

A CSV also lands in `reports/sales_summary_YYYY-MM-DD.csv`.

### Demo the incremental behaviour

```bash
docker exec eha_airflow airflow dags trigger sales_pipeline   # first run: bronze +2000
docker exec eha_airflow airflow dags trigger sales_pipeline   # second run: no-op
python3 data/generate_sales.py --append 100                   # simulate new orders
docker exec eha_airflow airflow dags trigger sales_pipeline   # third run: bronze +100
```

Each layer logs whether it added rows or was unchanged — check Task Logs in
the Airflow UI.

---

## Regenerate the source data

```bash
python3 data/generate_sales.py              # reset to 2,000 orders (seed=42)
python3 data/generate_sales.py --append 100 # add 100 more (deterministic)
```

~1 in 137 rows has a null `quantity` and ~1 in 211 has a null `unit_price` —
those are what the Silver step filters out.

---

## Troubleshooting

- **Ports in use?** Change the host-side port mappings in `docker-compose.yml`.
- **Start fresh:** `docker compose down -v && docker compose up -d` (the `-v`
  drops the Postgres + pgAdmin volumes).
- **Don't bump pandas past 2.1.** Airflow 2.9 pins SQLAlchemy to 1.4.x, and
  pandas 2.2+ needs SA ≥ 2.0 for its URI-string code path.
