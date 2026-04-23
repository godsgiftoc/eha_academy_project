# EHA Academy — Build & Monitor a Data Pipeline

Hands-on demo that walks through the full medallion architecture
(**Bronze → Silver → Gold → Report**) using Apache Airflow, PostgreSQL,
pandas, and Docker. Everything runs on your laptop; no cloud account needed.

---

## What this project does

A single Airflow DAG (`sales_pipeline`) runs four sequential tasks against
a **SQLite source database** and writes results into **Postgres** using the
medallion pattern. All three warehouse layers are **idempotent** — running
the DAG twice with no source changes is a true no-op.

| # | Task                | Layer  | Behaviour                                                  | Output                        |
|---|---------------------|--------|------------------------------------------------------------|-------------------------------|
| 1 | `ingest_bronze`     | Bronze | Stage source into `stg_sales`, then `INSERT ... ON CONFLICT DO NOTHING` by `order_id` | table `bronze_sales` |
| 2 | `transform_silver`  | Silver | Incremental insert of clean + typed rows not already in silver                         | table `silver_sales` |
| 3 | `aggregate_gold`    | Gold   | Full rebuild (aggregate is cheap and always current)                                   | table `gold_region_kpis` |
| 4 | `generate_report`   | Report | Write a dated summary CSV from gold                                                    | `reports/sales_summary_<date>.csv` |

A failure callback logs an `ALERT` line in the Task Logs so a failed run is
obvious in the Airflow UI.

---

## Stack

| Service   | Image / Port             | Purpose                                         |
|-----------|--------------------------|-------------------------------------------------|
| Airflow   | `apache/airflow:2.9.0` + pandas, port **8080** | Orchestration + scheduling                      |
| Postgres  | `postgres:15`, port **5432**                    | Hosts two DBs: `airflow` (metadata) and `warehouse` (medallion tables) |
| pgAdmin   | `dpage/pgadmin4:8`, port **5050**               | Browse / query the warehouse in a web UI        |

Separating `airflow` and `warehouse` into two databases keeps pipeline tables
isolated from Airflow's internal state.

---

## Quick start

Prerequisites: **Docker Desktop** running on macOS / Linux / Windows.

```bash
# From the project root
python3 data/generate_sales.py      # seed the SQLite source (one-off)
docker compose build                # build the custom Airflow image
docker compose up -d                # start Postgres, Airflow, pgAdmin
```

First boot takes ~1–2 minutes while Airflow runs DB migrations and creates
the admin user. Once ready, open:

| URL                        | Login                                 |
|----------------------------|---------------------------------------|
| http://localhost:8080      | `admin` / `admin`                     |
| http://localhost:5050      | `admin@example.com` / `admin` (password `airflow` when prompted for the DB) |

The DAG is unpaused by default. Trigger a manual run either from the UI
(**DAGs → sales_pipeline → ▶ Trigger**) or the CLI:

```bash
docker exec eha_airflow airflow dags trigger sales_pipeline
```

After it succeeds you should see:

```bash
docker exec eha_postgres psql -U airflow -d warehouse \
  -c "SELECT 'bronze' AS layer, COUNT(*) FROM bronze_sales
      UNION ALL SELECT 'silver', COUNT(*) FROM silver_sales
      UNION ALL SELECT 'gold',   COUNT(*) FROM gold_region_kpis;"
```

and a new file under `reports/sales_summary_YYYY-MM-DD.csv`.

### Demo the incremental behaviour

```bash
docker exec eha_airflow airflow dags trigger sales_pipeline   # first run: bronze +2000
docker exec eha_airflow airflow dags trigger sales_pipeline   # second run: no-op, everything unchanged
python3 data/generate_sales.py --append 100                    # simulate 100 new orders arriving
docker exec eha_airflow airflow dags trigger sales_pipeline   # third run: bronze +100, silver +~99
```

Check the Task Logs in the Airflow UI — each layer logs whether it added new
rows or was unchanged, so the pattern is obvious at a glance.

---

## Project layout

```
.
├── Dockerfile                     # Airflow image + pandas 2.1.4
├── docker-compose.yml             # Postgres + Airflow + pgAdmin
├── config/
│   ├── init-warehouse.sql         # Creates the `warehouse` DB on first boot
│   └── pgadmin-servers.json       # Pre-registers two servers in pgAdmin
├── dags/
│   └── sales_pipeline.py          # Thin DAG — orchestration only
├── include/
│   └── sales_pipeline/
│       ├── __init__.py
│       └── tasks.py               # Business logic (Airflow-free, easy to unit-test)
├── data/
│   ├── generate_sales.py          # Seed / append to source.db (seeded, reproducible)
│   └── source.db                  # SQLite source: `source_orders` table, PK on order_id
├── reports/                       # Written by the final task (gitignore-worthy)
├── logs/                          # Airflow task logs (bind-mounted)
└── README.md
```

---

## Using pgAdmin

After logging in, expand the left sidebar:

```
Servers
├── EHA Warehouse
│   └── Databases → warehouse → Schemas → public → Tables
│       ├── bronze_sales
│       ├── silver_sales
│       └── gold_region_kpis
└── Airflow Metadata
    └── Databases → airflow  (DAG runs, task instances, XComs, etc.)
```

Right-click any table → **View/Edit Data → All Rows**, or open the **Query
Tool** for ad-hoc SQL. pgAdmin is the feed for any BI tool you plug in next
(Metabase, Superset, Looker Studio, Power BI) — all of them connect to the
same Postgres on `localhost:5432`, database `warehouse`.

---

## Regenerate the source data

`data/source.db` is a seeded SQLite file. Recreate or extend it with:

```bash
python3 data/generate_sales.py              # reset to 2,000 orders (seed=42)
python3 data/generate_sales.py --append 100 # add 100 more (deterministic)
```

The generator writes into a `source_orders` table with `order_id` as the
primary key. ~1 in 137 rows is given a null `quantity` and ~1 in 211 a null
`unit_price` — those are what the Silver step filters out.

---

## Scheduling

The DAG is scheduled `0 8 * * *` (every day at 08:00 UTC) with `catchup=False`,
so it runs forward-only and does **not** backfill historical dates on first
deploy. Change `schedule_interval` in `dags/sales_pipeline.py` to suit your
timezone / cadence.

---

## Monitoring & alerting

Built in:

- **Grid view** — one coloured cell per historical run; click a cell to jump
  to task logs.
- **Graph view** — DAG structure with current task states.
- **Task logs** — full stdout/stderr per attempt (the `ALERT:` line from the
  `on_failure_callback` lands here on failure).
- **Retries** — each task retries once after 1 minute before being marked
  failed (`default_args` in the DAG).

For production you would replace the `on_failure` log line with a Slack
webhook, PagerDuty integration, or Airflow's `EmailOperator`.

---

## Troubleshooting

**`ImportError: Using URI string without sqlalchemy installed`** on the
ingest task. This happens if pandas is upgraded past 2.1. Pandas 2.2+
requires SQLAlchemy ≥ 2.0 for the URI-string code path, but Airflow 2.9 pins
SQLAlchemy to 1.4.x. The Dockerfile pins `pandas==2.1.4` to stay compatible —
don't bump it unless you're also moving to Airflow 2.10+ with SA 2.0.

**pgAdmin container exits immediately with `'admin@...' is not a valid email
address`.** pgAdmin rejects `.local`, `.internal`, and other "special" TLDs.
Use a real-looking domain in `PGADMIN_DEFAULT_EMAIL` (the compose file uses
`admin@example.com`).

**Ports already in use.** If 8080, 5050, or 5432 are taken, edit the `ports:`
mappings in `docker-compose.yml` (only the left-hand side of `host:container`).

**Wipe everything and start fresh:**

```bash
docker compose down -v      # -v also drops the Postgres + pgAdmin volumes
docker compose up -d
```

---

## Where to go next

- Replace `sales.csv` with an API pull in `ingest_fn()`.
- Add a `PostgresOperator` step that runs a `.sql` file instead of pandas.
- Point a BI tool at `gold_region_kpis` and build a dashboard.
- Switch failure logging to a real Slack webhook.
- Add a data-quality step (e.g., Great Expectations) between Silver and Gold.
