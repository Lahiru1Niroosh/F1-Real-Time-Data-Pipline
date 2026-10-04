import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, BooleanType
)
from cassandra.cluster import Cluster
from dotenv import load_dotenv

load_dotenv()

def get_cassandra_session():
    cluster = Cluster(
        [os.getenv("CASSANDRA_HOST", "localhost")],
        port=9042,
        connect_timeout=30,
        control_connection_timeout=30
    )
    session = cluster.connect('f1_data')
    session.default_timeout = 60
    return session, cluster

def process_batch(df, epoch_id):
    print(f"\n⚡ Processing batch {epoch_id}...")

    # Filter only lap rows — type == "lap" and lap_duration is valid
    lap_df = df.filter(
        (col("type") == "lap") &
        col("lap_duration").isNotNull() &
        (col("lap_duration") > 0)
    )

    count = lap_df.count()
    print(f"   📊 Lap rows in batch: {count}")

    if count == 0:
        print("   No lap data in this batch")
        return

    cassandra_session, cluster = get_cassandra_session()

    try:
        rows = lap_df.dropDuplicates(
            ["session_key", "driver_number", "lap_number"]
        ).collect()

        fastest_per_lap = {}
        for row in rows:
            key = (row.session_key, row.lap_number)
            if key not in fastest_per_lap or row.lap_duration < fastest_per_lap[key]:
                fastest_per_lap[key] = row.lap_duration

        from cassandra.query import BatchStatement, BatchType, SimpleStatement
        from cassandra import ConsistencyLevel

        lap_insert = cassandra_session.prepare("""
            INSERT INTO lap_telemetry (
                session_key, driver_number, lap_number,
                lap_duration, sector_1, sector_2, sector_3,
                is_pit_out_lap, date_start
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

        gap_insert = cassandra_session.prepare("""
            INSERT INTO race_gaps (
                session_key, lap_number, driver_number,
                lap_duration, gap_to_fastest
            ) VALUES (?, ?, ?, ?, ?)
        """)

        laps_written = 0
        gaps_written = 0
        batch_size = 5

        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i + batch_size]
            batch = BatchStatement(batch_type=BatchType.UNLOGGED)

            for row in chunk:
                try:
                    batch.add(lap_insert, (
                        row.session_key,
                        row.driver_number,
                        row.lap_number,
                        row.lap_duration,
                        row.duration_sector_1,
                        row.duration_sector_2,
                        row.duration_sector_3,
                        row.is_pit_out_lap,
                        str(row.date_start) if row.date_start else None
                    ))
                    laps_written += 1

                    key = (row.session_key, row.lap_number)
                    fastest = fastest_per_lap.get(key)
                    if fastest and row.lap_duration:
                        gap = round(row.lap_duration - fastest, 3)
                        batch.add(gap_insert, (
                            row.session_key,
                            row.lap_number,
                            row.driver_number,
                            row.lap_duration,
                            gap
                        ))
                        gaps_written += 1

                except Exception as e:
                    print(f"   ❌ Row error: {e}")

            try:
                cassandra_session.execute(batch, timeout=60)
            except Exception:
                print("   ❌ Batch write failed; failing Spark task")
                raise

        print(f"   ✅ Laps written to Cassandra: {laps_written}")
        print(f"   ✅ Gaps written to Cassandra: {gaps_written}")

    finally:
        cluster.shutdown()

lap_schema = StructType([
    StructField("type", StringType()),
    StructField("session_key", IntegerType()),
    StructField("driver_number", IntegerType()),
    StructField("lap_number", IntegerType()),
    StructField("lap_duration", DoubleType()),
    StructField("duration_sector_1", DoubleType()),
    StructField("duration_sector_2", DoubleType()),
    StructField("duration_sector_3", DoubleType()),
    StructField("is_pit_out_lap", BooleanType()),
    StructField("date_start", StringType())
])

def main():
    print("🚀 Starting F1 Spark Structured Streaming job...")

    spark = SparkSession.builder \
        .appName("F1StreamingPipeline") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")) \
        .option("subscribe", "f1_lap_data") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    parsed = raw_stream.select(
        from_json(
            col("value").cast("string"),
            lap_schema
        ).alias("data")
    ).select("data.*")

    # AvailableNow reads the offsets available when the task starts, then exits.
    # The persisted checkpoint prevents replay after an Airflow retry or restart.
    query = parsed.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", os.getenv("SPARK_CHECKPOINT_DIR", "/opt/airflow/checkpoints/f1_lap_data")) \
        .trigger(availableNow=True) \
        .start()

    try:
        query.awaitTermination()
        if query.exception() is not None:
            raise RuntimeError(f"Spark streaming query failed: {query.exception()}")
        print("✅ Processed available Kafka offsets and stopped")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()