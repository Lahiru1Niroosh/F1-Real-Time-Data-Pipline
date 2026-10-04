import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

def create_tables():
    conn = get_connection()
    cursor = conn.cursor()

    # Sessions table — one row per race/qualifying/practice session
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            session_key     INTEGER PRIMARY KEY,
            session_name    VARCHAR(100),
            session_type    VARCHAR(50),
            country_name    VARCHAR(100),
            circuit_name    VARCHAR(100),
            date_start      TIMESTAMP,
            year            INTEGER,
            created_at      TIMESTAMP DEFAULT NOW()
        );
    """)

    # Drivers table — one row per driver per session
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS drivers (
            driver_id       SERIAL PRIMARY KEY,
            session_key     INTEGER REFERENCES sessions(session_key),
            driver_number   INTEGER,
            full_name       VARCHAR(100),
            name_acronym    VARCHAR(10),
            team_name       VARCHAR(100),
            country_code    VARCHAR(10),
            created_at      TIMESTAMP DEFAULT NOW(),
            UNIQUE(session_key, driver_number)
        );
    """)

    # Lap times table — one row per lap per driver
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS lap_times (
            lap_id          SERIAL PRIMARY KEY,
            session_key     INTEGER REFERENCES sessions(session_key),
            driver_number   INTEGER,
            lap_number      INTEGER,
            lap_duration    FLOAT,
            duration_sector_1   FLOAT,
            duration_sector_2   FLOAT,
            duration_sector_3   FLOAT,
            is_pit_out_lap  BOOLEAN,
            date_start      TIMESTAMP,
            created_at      TIMESTAMP DEFAULT NOW(),
            UNIQUE(session_key, driver_number, lap_number)
        );
    """)

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Tables created successfully")

if __name__ == "__main__":
    create_tables()