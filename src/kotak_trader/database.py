import sqlite3
from pathlib import Path


DB_PATH = Path("data/market_data.db")


def get_connection():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(
        DB_PATH,
        check_same_thread=False
    )

    return connection


def initialize_database():

    connection = get_connection()

    connection.execute("""
        CREATE TABLE IF NOT EXISTS instruments (
            instrument_token TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            exchange_segment TEXT NOT NULL,
            instrument_type TEXT,
            trading_symbol TEXT,
            last_updated TEXT NOT NULL
        )
    """)

    connection.execute("""
        CREATE TABLE IF NOT EXISTS ticks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            received_at TEXT NOT NULL,
            exchange_timestamp TEXT,

            instrument_token TEXT NOT NULL,
            exchange_segment TEXT NOT NULL,

            last_price REAL,

            raw_json TEXT NOT NULL,

            FOREIGN KEY (instrument_token)
                REFERENCES instruments(instrument_token)
        )
    """)

    connection.execute("""
        CREATE INDEX IF NOT EXISTS idx_ticks_instrument_time
        ON ticks(instrument_token, received_at)
    """)

    connection.execute("""
        CREATE TABLE IF NOT EXISTS connection_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            timestamp TEXT NOT NULL,
            event TEXT NOT NULL,
            message TEXT
        )
    """)

    connection.commit()
    connection.close()


def save_instrument(instrument):

    connection = get_connection()

    connection.execute("""
        INSERT OR REPLACE INTO instruments (
            instrument_token,
            symbol,
            exchange_segment,
            instrument_type,
            trading_symbol,
            last_updated
        )
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        instrument["instrument_token"],
        instrument["symbol"],
        instrument["exchange_segment"],
        instrument["instrument_type"],
        instrument["trading_symbol"],
        instrument["last_updated"],
    ))

    connection.commit()
    connection.close()


if __name__ == "__main__":
    initialize_database()
    print(f"Database initialized: {DB_PATH}")