import requests
import json
import time
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# ─── KAFKA PRODUCER SETUP ────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    acks="all",
    retries=3
)

BASE_URL = os.getenv("OPENF1_BASE_URL")

# ─── API FETCH HELPER ─────────────────────────────────────────
def fetch(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"
    # Fail the Airflow task if an endpoint cannot be fetched; never report
    # success for a partial run. Each request has a bounded wait.
    for attempt in range(3):
        try:
            time.sleep(2.1)
            response = requests.get(url, params=params, timeout=(5, 60))
            if response.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                delay = max(float(response.headers.get("Retry-After", "0") or 0), 2 ** (attempt + 1))
                print(f"API {response.status_code} for {endpoint}; retrying in {delay}s", flush=True)
                time.sleep(delay)
                continue
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            if attempt == 2:
                raise RuntimeError(f"Could not fetch {endpoint} after 3 attempts") from exc
            delay = 2 ** (attempt + 1)
            print(f"Request failed for {endpoint}; retrying in {delay}s: {exc}", flush=True)
            time.sleep(delay)
    raise RuntimeError(f"Could not fetch {endpoint} after 3 attempts")

# ─── PUBLISH TO KAFKA ─────────────────────────────────────────
def publish(topic, data, label="record"):
    producer.send(topic, value=data)
    print(f"📤 Published {label} to topic [{topic}]")

# ─── FETCH AND STREAM SESSIONS ────────────────────────────────
def stream_sessions(year=2024):
    print(f"\n🏁 Fetching sessions for {year}...")
    sessions = fetch("sessions", {"year": year, "session_type": "Race"})
    for session in sessions:
        payload = {
            "session_key":   session.get("session_key"),
            "session_name":  session.get("session_name"),
            "session_type":  session.get("session_type"),
            "country_name":  session.get("country_name"),
            "circuit_name":  session.get("circuit_short_name"),
            "date_start":    session.get("date_start"),
            "year":          session.get("year")
        }
        publish("f1_lap_data", payload, f"session {payload['session_key']}")
    producer.flush()
    return [s["session_key"] for s in sessions]

# ─── FETCH AND STREAM DRIVERS ─────────────────────────────────
def stream_drivers(session_key):
    print(f"\n👤 Fetching drivers for session {session_key}...")
    drivers = fetch("drivers", {"session_key": session_key})
    for driver in drivers:
        payload = {
            "type":           "driver",
            "session_key":    driver.get("session_key"),
            "driver_number":  driver.get("driver_number"),
            "full_name":      driver.get("full_name"),
            "name_acronym":   driver.get("name_acronym"),
            "team_name":      driver.get("team_name"),
            "country_code":   driver.get("country_code")
        }
        publish("f1_lap_data", payload, f"driver {payload['name_acronym']}")
    producer.flush()
    return [d["driver_number"] for d in drivers]

# ─── FETCH AND STREAM LAPS ────────────────────────────────────
def stream_laps(session_key):
    laps = fetch("laps", {"session_key": session_key})
    for lap in laps:
        payload = {
            "type":               "lap",
            "session_key":        lap.get("session_key"),
            "driver_number":      lap.get("driver_number"),
            "lap_number":         lap.get("lap_number"),
            "lap_duration":       lap.get("lap_duration"),
            "duration_sector_1":  lap.get("duration_sector_1"),
            "duration_sector_2":  lap.get("duration_sector_2"),
            "duration_sector_3":  lap.get("duration_sector_3"),
            "is_pit_out_lap":     lap.get("is_pit_out_lap"),
            "date_start":         lap.get("date_start")
        }
        publish("f1_lap_data", payload,
            f"lap {payload['lap_number']} driver {payload['driver_number']}")
    producer.flush()

# ─── MAIN PIPELINE ────────────────────────────────────────────
if __name__ == "__main__":
    print("🚀 F1 Kafka Producer starting...")

    # Stream sessions and get their keys
    session_keys = stream_sessions(year=2024)

    # For each session stream drivers and laps
    for session_key in session_keys:
        stream_drivers(session_key)
        stream_laps(session_key)

    print("\n✅ Producer finished streaming all data")