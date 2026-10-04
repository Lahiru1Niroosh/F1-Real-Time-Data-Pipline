import json
import os
import psycopg2
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

# ─── DATABASE CONNECTION ──────────────────────────────────────
def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

# ─── INSERT HANDLERS ─────────────────────────────────────────

def insert_session(cursor, data):
    cursor.execute("""
        INSERT INTO sessions (
            session_key, session_name, session_type,
            country_name, circuit_name, date_start, year
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (session_key) DO NOTHING;
    """, (
        data.get("session_key"),
        data.get("session_name"),
        data.get("session_type"),
        data.get("country_name"),
        data.get("circuit_name"),
        data.get("date_start"),
        data.get("year")
    ))

def insert_driver(cursor, data):
    cursor.execute("""
        INSERT INTO drivers (
            session_key, driver_number, full_name,
            name_acronym, team_name, country_code
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (session_key, driver_number) DO NOTHING;
    """, (
        data.get("session_key"),
        data.get("driver_number"),
        data.get("full_name"),
        data.get("name_acronym"),
        data.get("team_name"),
        data.get("country_code")
    ))

def insert_lap(cursor, data):
    cursor.execute("""
        INSERT INTO lap_times (
            session_key, driver_number, lap_number,
            lap_duration, duration_sector_1, duration_sector_2,
            duration_sector_3, is_pit_out_lap, date_start
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (session_key, driver_number, lap_number) DO NOTHING;
    """, (
        data.get("session_key"),
        data.get("driver_number"),
        data.get("lap_number"),
        data.get("lap_duration"),
        data.get("duration_sector_1"),
        data.get("duration_sector_2"),
        data.get("duration_sector_3"),
        data.get("is_pit_out_lap"),
        data.get("date_start")
    ))

# ─── MAIN CONSUMER LOOP ──────────────────────────────────────
def run_consumer():
    print("🚀 F1 Kafka Consumer starting...")

    conn = get_connection()
    cursor = conn.cursor()

    consumer = KafkaConsumer(
        os.getenv("KAFKA_TOPIC_LAP"),
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="f1_consumer_group"
    )

    sessions = 0
    drivers = 0
    laps = 0
    empty_polls = 0
    max_empty_polls = 3  # exit after 3 consecutive empty polls (~15s of no data)

    while empty_polls < max_empty_polls:
        records = consumer.poll(timeout_ms=5000, max_records=100)

        if not records:
            empty_polls += 1
            continue

        empty_polls = 0  # reset on any data received

        for tp, messages in records.items():
            for message in messages:
                data = message.value
                msg_type = data.get("type")

                try:
                    if msg_type == "session" or "session_key" in data and "year" in data and "type" not in data:
                        insert_session(cursor, data)
                        conn.commit()
                        sessions += 1
                        print(f"✅ Session: {data.get('country_name')} {data.get('year')}")

                    elif msg_type == "driver":
                        insert_driver(cursor, data)
                        conn.commit()
                        drivers += 1
                        print(f"✅ Driver: {data.get('name_acronym')} — {data.get('team_name')}")

                    elif msg_type == "lap":
                        insert_lap(cursor, data)
                        conn.commit()
                        laps += 1
                        if laps % 100 == 0:
                            print(f"✅ Laps inserted: {laps}")

                except Exception as e:
                    print(f"❌ Error inserting {msg_type}: {e}")
                    conn.rollback()

    consumer.close()
    cursor.close()
    conn.close()

    print(f"\n🏁 Consumer finished.")
    print(f"   Sessions inserted : {sessions}")
    print(f"   Drivers inserted  : {drivers}")
    print(f"   Laps inserted     : {laps}")

if __name__ == "__main__":
    run_consumer()