from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "lahiru",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="f1_pipeline",
    default_args=default_args,
    description="F1 data pipeline: OpenF1 API -> Kafka -> Postgres -> Spark -> Cassandra",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["f1", "data-engineering"],
) as dag:

    run_producer = BashOperator(
        task_id="run_producer",
        bash_command="cd /opt/airflow/ingestion && python producer.py",
    )

    run_consumer = BashOperator(
        task_id="run_consumer",
        bash_command="cd /opt/airflow/ingestion && python consumer.py",
    )

    run_spark_streaming = BashOperator(
        task_id="run_spark_streaming",
        bash_command=(
            "cd /opt/airflow/processing && "
            "spark-submit --master local[1] --driver-memory 512m "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
            "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.4.1,"
            "org.apache.commons:commons-pool2:2.11.1 "
            "spark_streaming.py"
        ),
    )

    run_producer >> run_consumer >> run_spark_streaming