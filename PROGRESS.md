# F1 Pipeline — Build Progress

## Stack
- Kafka (KRaft mode) — port 9092
- PostgreSQL 15 — port 5432, db: f1_db, user: f1_user, pass: f1_pass
- Cassandra 4.1 — port 9042, keyspace: f1_data
- Spark 3.4.1 — installed at /opt/spark

## Daily Startup
```bash
cd /mnt/e/My-individual-Projects/f1-pipeline
docker compose -f docker/docker-compose.yml up -d
# Wait 90 seconds for Cassandra
docker ps
```

## Phase 1 — COMPLETE ✅
- docker-compose.yml — Kafka + PostgreSQL
- ingestion/db_setup.py — creates 3 PostgreSQL tables
- ingestion/producer.py — OpenF1 API → Kafka
- ingestion/consumer.py — Kafka → PostgreSQL
- Result: 30 sessions, 99 drivers, 4119 laps in PostgreSQL

## Phase 2 — IN PROGRESS 🔄
- Git: initialized with proper .gitignore ✅
- Cassandra keyspace: f1_data
- Tables: lap_telemetry, race_gaps
- Spark install: /opt/spark
- TODO: processing/spark_streaming.py

## Phase 3 — PENDING
- Airflow DAGs
- Spark batch jobs

## Phase 4 — PENDING
- Apache Superset
- Grafana dashboards

## Phase 5 — PENDING
- README + architecture diagram
- GitHub setup ← currently here
- OCI deployment

## Key File Locations
- Project: /mnt/e/My-individual-Projects/f1-pipeline
- Docker: ./docker/docker-compose.yml
- Ingestion: ./ingestion/
- Processing: ./processing/
- Orchestration: ./orchestration/
- Dashboards: ./dashboards/

## Kafka Topics
- f1_lap_data — all F1 streaming data

## PostgreSQL Tables
- sessions — race/qualifying sessions
- drivers — driver details per session
- lap_times — every lap for every driver

## Cassandra Tables (keyspace: f1_data)
- lap_telemetry — time-series lap data
- race_gaps — real-time driver gaps per lap

## If Starting Fresh Chat
Paste this file content and say:
"Continue building my F1 data engineering project.
Here is my current progress document."