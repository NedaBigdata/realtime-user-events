# 🚀 Realtime User Events Pipeline

An end-to-end data engineering pipeline that ingests user data from [randomuser.me](https://randomuser.me), streams it through Kafka (Redpanda), stores it in ClickHouse, and visualizes real-time analytics in Apache Superset.

![Realtime Dashboard](docs/screenshots/01_superset_dashboard.png)

## 📐 Architecture

```
randomuser.me API
      │
      ▼ (Airflow DAG, every 5 min)
┌──────────────┐
│  PostgreSQL  │  users_raw (raw JSONB payloads)
└──────┬───────┘
       │
       ▼ (Producer: polls unprocessed rows)
┌──────────────┐
│   Redpanda   │  topic: user_events (Kafka-compatible)
└──────┬───────┘
       │
       ▼ (Consumer: batch inserts)
┌──────────────┐
│  ClickHouse  │  user_events + Materialized Views (hourly/daily)
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   Superset   │  Realtime dashboard
└──────────────┘
```

## 🛠️ Stack

| Layer | Technology | Purpose |
|---|---|---|
| Orchestration | **Apache Airflow 2.9** | Schedule ingestion every 5 min |
| Source store | **PostgreSQL 16** | Raw JSONB staging + Airflow metadata |
| Message bus | **Redpanda 24.1** | Kafka-compatible streaming |
| Analytics DB | **ClickHouse 24.8** | Columnar store + Materialized Views |
| BI | **Apache Superset 3.1** | Realtime dashboard |
| Deployment | **Docker Compose** | Single-command startup |

## ✨ Key Engineering Decisions

- **ELT pattern**: Raw JSON is stored as `JSONB` in Postgres first, then transformed on the way to ClickHouse. This allows re-processing and backfills.
- **At-least-once delivery**: The consumer commits offsets **after** a successful ClickHouse insert (`enable.auto.commit=False`). AggregatingMergeTree with `countState`/`avgState` is idempotent, so duplicates don't corrupt aggregates.
- **Incremental aggregation**: Hourly and daily rollups are computed by ClickHouse Materialized Views — the application never recomputes aggregates.
- **12-factor config**: All credentials come from `.env` via `os.environ`, no secrets in source code.
- **Restart-safe**: Named volumes for Postgres, Redpanda, ClickHouse, and Airflow logs. `docker compose down && up` resumes without data loss.

## 🚀 Quickstart

### Prerequisites

- Docker & Docker Compose v2
- (For Iran) A working Docker registry mirror — see [Iran/Windows notes](#-iran--windows-notes)

### 1. Clone and configure

```bash
git clone https://github.com/<your-username>/realtime-user-events.git
cd realtime-user-events
cp .env.example .env
# Edit .env and set a strong SUPERSET_SECRET_KEY (openssl rand -hex 32)
```

### 2. Start all services

```bash
docker compose up -d --build
```

The first build takes 5–10 minutes (downloads images + installs PyPI packages from mirror).

### 3. Create the Superset metadata database

```bash
docker exec -it rte_postgres psql -U rte_user -d user_events -c "CREATE DATABASE superset_meta;"
```

### 4. Wait for init containers to finish

```bash
docker compose logs -f airflow-init      # wait for "Admin user created"
docker compose logs -f superset-init     # wait for Exited(0)
```

### 5. Access UIs

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8085 | `admin` / `admin` |
| Superset | http://localhost:8088 | `admin` / `admin` |
| Redpanda Console | http://localhost:8090 | – |
| PostgreSQL | `localhost:5432` | `.env` |
| ClickHouse HTTP | `localhost:8123` | `.env` |

### 6. Trigger the pipeline

In Airflow UI, **unpause** the `randomuser_ingest` DAG. It runs every 5 minutes.

Watch the data flow:

```bash
docker compose logs -f producer consumer
```

### 7. Open the dashboard

Superset → **Dashboards** → **Realtime User Events**

## 📊 What You'll See

### Superset Dashboard
![Dashboard](docs/screenshots/01_superset_dashboard.png)

### Airflow DAG
![Airflow](docs/screenshots/02_airflow_dag.png)

### Redpanda Messages (real-time stream)
![Redpanda](docs/screenshots/04_redpanda_messages.png)

### Consumer lag = 0 (healthy pipeline)
![Consumer Group](docs/screenshots/05_redpanda_consumer_group.png)

## 📁 Project Structure

```
realtime-user-events/
├── airflow/dags/          # Airflow DAG definitions
├── producer/              # Postgres → Kafka producer
├── consumer/              # Kafka → ClickHouse consumer
├── postgres/init/         # Postgres schema (users_raw, etc.)
├── clickhouse/init/       # ClickHouse schema + Materialized Views
├── superset/              # Superset Dockerfile + config
├── docs/screenshots/      # README screenshots
├── docker-compose.yml
└── .env.example
```

## 🧪 Verifying the Pipeline

> Replace `rte_user`/`rte_pass` with the values from your `.env` if you changed them.

```bash
# 1. Count raw events in Postgres
docker exec rte_postgres psql -U rte_user -d user_events -c "SELECT count(*) FROM users_raw;"

# 2. Check Kafka topic has messages (Ctrl+C to stop)
docker exec -it rte_redpanda rpk topic consume user_events

# 3. Count events in ClickHouse
docker exec rte_clickhouse clickhouse-client --user rte_user --password rte_pass \
  --query "SELECT count(*) FROM analytics.user_events"

# 4. Check hourly rollup
docker exec rte_clickhouse clickhouse-client --user rte_user --password rte_pass \
  --query "SELECT * FROM analytics.v_hourly LIMIT 5 FORMAT Pretty"
```

## 🐛 Troubleshooting

<details>
<summary><b>Docker images won't download (Iran sanctions)</b></summary>

Configure Docker registry mirrors in `/etc/docker/daemon.json` (Linux) or Docker Desktop settings. Recommended mirrors for Iran:
- `https://docker.ththt.ir`
- `https://docker.3cn.ir`
- `https://docker.arvancloud.ir`
</details>

<details>
<summary><b>Airflow "Could not find a version that satisfies requests==..."</b></summary>

PyPI is blocked. The Dockerfiles set `PIP_INDEX_URL` to an Iranian mirror. If Liara is slow, try:
- Kargadan: `https://mirror.kargadan.ir/repository/pypi-group/simple/`
- Runflare: `https://mirror-pypi.runflare.com/simple`
</details>

<details>
<summary><b>ClickHouse unhealthy — "Address family for hostname not supported"</b></summary>

Docker bridge network doesn't support IPv6 but ClickHouse binds to `[::]` by default. The fix is in `clickhouse/ipv4-only.xml` (mounted to `/etc/clickhouse-server/config.d/`) which sets `<listen_host>0.0.0.0</listen_host>`. Verify the file has no BOM: `od -An -tx1 -N16 clickhouse/ipv4-only.xml` should start with `3c 63 6c 69`.
</details>

<details>
<summary><b>Airflow task log shows 404</b></summary>

`airflow-webserver` and `airflow-scheduler` must share the same logs volume. We use a named volume `airflow_logs` for both. Also set `AIRFLOW__CORE__HOSTNAME_CALLABLE=socket.gethostname`.
</details>

<details>
<summary><b>Postgres init script didn't run</b></summary>

Scripts in `/docker-entrypoint-initdb.d/` only run **once** on an empty data volume. To re-run:
```bash
docker compose down -v && docker compose up -d
```
</details>

<details>
<summary><b>Superset "Table X does not exist"</b></summary>

Check that `.env`'s `CLICKHOUSE_DB` matches the schema created by `clickhouse/init/01_schema.sql` (default: `analytics`). A mismatch was the most common error during development.
</details>

## 🌍 Iran / Windows Notes

- **Docker registry mirrors**: See [Troubleshooting](#-troubleshooting)
- **PyPI mirror**: Set `PIP_INDEX_URL` in all Dockerfiles (already done)
- **Git Bash path conversion**: Set `export MSYS_NO_PATHCONV=1` in `~/.bashrc`, or use `//var/log/...` in `docker exec` commands.
- **XML config files**: Save with UTF-8 **without BOM** and LF line endings.

## 📈 Production Hardening (out of scope for this demo)

- Enable Talisman + CSP in Superset (`TALISMAN_ENABLED=True`)
- Move Superset rate limiting to Redis (`RATELIMIT_STORAGE_URI=redis://...`)
- Use Airflow with `CeleryExecutor` + Redis + Postgres for scalability
- Add Prometheus metrics for producer/consumer lag
- Use a real Kafka cluster with replication factor ≥ 3
- Move secrets to HashiCorp Vault or AWS Secrets Manager
- Replace `.env` with Kubernetes Secrets or Docker Swarm secrets

## 📚 What I Learned

- Designing an ELT pipeline with proper staging vs. analytics separation
- Streaming semantics: at-least-once delivery, idempotent aggregation
- ClickHouse Materialized Views with `AggregatingMergeTree` and `*State`/`*Merge` functions
- Docker Compose multi-service orchestration with healthchecks and dependency ordering
- Airflow DAG design and 12-factor configuration
- Superset dataset + dashboard modeling
- Operating a full stack in a restricted-network environment (Iran)

## 👤 Author

**Your Name** — [LinkedIn](https://linkedin.com/in/your-handle) · [GitHub](https://github.com/your-username)