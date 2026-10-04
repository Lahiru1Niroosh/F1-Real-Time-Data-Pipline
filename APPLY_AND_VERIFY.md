# Phase 3 Airflow completion patch

These files update the ingestion producer, Airflow image/DAG, Spark job, and Compose.
They do not contain `.env`, database files, or Docker volumes. Extract into
`E:\My-individual-Projects\f1-pipeline`, replacing matching files.

From PowerShell in the project root:

```powershell
docker compose -f docker/docker-compose.yml config --quiet
docker compose -f docker/docker-compose.yml build airflow-webserver airflow-scheduler
docker compose -f docker/docker-compose.yml stop airflow-webserver airflow-scheduler
docker compose -f docker/docker-compose.yml up -d --no-deps --force-recreate airflow-webserver airflow-scheduler
docker compose -f docker/docker-compose.yml ps
```

If Docker refuses to remove a stopped Airflow container, use
`docker compose -f docker/docker-compose.yml ps -a` to find its actual name,
`docker rm -f <actual-Airflow-container-name>`, then rerun the `up` command.
Do not use `down -v`.

The image build installs Java 17 for PySpark. The task uses `spark-submit`
with the Kafka connector and an AvailableNow trigger: it processes Kafka
records available at the start of the task and exits. Checkpoints persist in
`spark_checkpoints` so retries do not replay old offsets. Kafka inside Docker
uses `kafka:29092`; the Windows host still uses `localhost:9092`.

After Airflow has started, mark the stale August 14 producer task failed in
Grid view. Trigger one new DAG run. Verify all three task states and counts:

```powershell
docker exec f1_postgres psql -U f1_user -d f1_db -c "SELECT COUNT(*) FROM lap_times;"
docker exec f1_cassandra cqlsh -e "USE f1_data; SELECT COUNT(*) FROM lap_telemetry;"
```

Expected counts depend on whether these records were already loaded; an
unchanged count can be correct because PostgreSQL has ON CONFLICT and
Cassandra upserts. Use the Airflow task logs to confirm rows processed.

This patch has static Python and YAML validation only. Docker image build,
API access, and end-to-end run require your local Docker environment.
