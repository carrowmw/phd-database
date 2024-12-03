# phd_package/database/src/duplicate_database.py

import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database.api.api_client import APIClient
from database.src.models import Base

def create_database_schema(db_path: str) -> sqlite3.Connection:
    """Create the database schema"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create sensors table to store metadata
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sensors (
            sensor_id INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_name TEXT NOT NULL,
            raw_id TEXT,
            broker_name TEXT,
            is_third_party BOOLEAN,
            sensor_height_ground REAL,
            ground_height_sea REAL,
            centroid_latitude REAL,
            centroid_longitude REAL,
            location_wkt TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create variables table to track different measurement types
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS variables (
            variable_id INTEGER PRIMARY KEY AUTOINCREMENT,
            variable_name TEXT NOT NULL UNIQUE,  -- e.g., 'Plates In', 'Journey Time'
            units TEXT,                         -- e.g., 'Vehicles', 'seconds'
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


    # Create measurements table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS measurements (
            measurement_id INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id INTEGER,
            timestamp INTEGER NOT NULL,
            value REAL,
            units TEXT,
            variable_id INTEGER,
            is_suspect BOOLEAN,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id),
            FOREIGN KEY (variable_id) REFERENCES variables(variable_id)
        )
    """)

    # Create index for quick lookup
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_measurements_sensor_time 
            ON measurements(sensor_id, timestamp);
        CREATE INDEX IF NOT EXISTS idx_measurements_variable 
            ON measurements(variable_id);
        CREATE INDEX IF NOT EXISTS idx_sensors_name 
            ON sensors(sensor_name);
    """)

    conn.commit()
    return conn

if __name__ == "__main__":
    construct_table_schemas()
    # from database.src.database import engine
    # create_all_tables(engine)
