# ecommerce-analytics

[![dbt CI](https://github.com/kunchalathejakumar/ecommerce-analytics/actions/workflows/dbt_ci.yml/badge.svg)](https://github.com/kunchalathejakumar/ecommerce-analytics/actions/workflows/dbt_ci.yml)

End-to-end ecommerce analytics pipeline: data ingestion via AWS Glue & S3, transformation with dbt (PostgreSQL), and orchestration with Apache Airflow.

## Stack

| Layer | Tool |
|---|---|
| Ingestion | AWS Glue, S3, Athena |
| Transformation | dbt-postgres 1.10, PostgreSQL 15 |
| Orchestration | Apache Airflow 3 |
| CI | GitHub Actions |

## Project structure

```
ecommerce-analytics/
├── .github/workflows/dbt_ci.yml   # CI pipeline (this badge ↑)
├── dbt_project/                   # dbt project root
│   ├── models/
│   │   ├── staging/               # stg_* views — clean source data
│   │   ├── intermediate/          # int_* views — business logic
│   │   └── marts/                 # mart_* tables — analytics-ready
│   ├── profiles.yml               # connection config (env-var driven)
│   └── packages.yml               # dbt_utils 1.1.1
├── data/raw/                      # sample CSV fixtures
├── ingestion/                     # Python scripts: S3, Glue, Athena
├── airflow/                       # DAG definitions
└── docker/                        # docker-compose for local Postgres
```

## CI — GitHub Actions (`dbt_ci.yml`)

Every pull request to `main` runs the following pipeline:

1. Spin up a **PostgreSQL 15** service container
2. Create `staging`, `intermediate`, and `marts` schemas
3. Load raw CSV fixtures into `staging` source tables
4. `dbt deps` — install dbt_utils
5. `dbt debug` — verify connection
6. `dbt run --select staging` — build staging views
7. `dbt test` — run all schema + data tests
8. Post a **test-results summary comment** on the PR (upserted on re-runs)

### Required GitHub Secrets

| Secret | Value |
|---|---|
| `POSTGRES_HOST` | `localhost` (service container) |
| `POSTGRES_USER` | `admin` |
| `POSTGRES_PASSWORD` | your Postgres password |

Set them at **Settings → Secrets and variables → Actions → New repository secret**.

## Local development

```bash
# Copy .env and start local Postgres
cp .env.example .env          # fill in your values
docker compose -f docker/docker-compose.yml up -d

# Run dbt
cd dbt_project
dbt deps
dbt run
dbt test
```
