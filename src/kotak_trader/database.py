from __future__ import annotations
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "market_data.db"

def get_connection() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 10000")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA cache_size = -65536")
    return conn

SCHEMA = """
CREATE TABLE IF NOT EXISTS instruments (
    instrument_token TEXT NOT NULL,
    exchange_segment TEXT NOT NULL,
    symbol TEXT,
    trading_symbol TEXT,
    instrument_type TEXT,
    option_type TEXT,
    expiry_date TEXT,
    strike_price REAL,
    lot_size INTEGER,
    tick_size REAL,
    isin TEXT,
    contract_id TEXT,
    instrument_name TEXT,
    source_file TEXT,
    source_updated_at TEXT,
    loaded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (instrument_token, exchange_segment)
);
CREATE INDEX IF NOT EXISTS idx_instruments_symbol ON instruments(symbol);
CREATE INDEX IF NOT EXISTS idx_instruments_trading_symbol ON instruments(trading_symbol);
CREATE INDEX IF NOT EXISTS idx_instruments_expiry ON instruments(expiry_date);
CREATE INDEX IF NOT EXISTS idx_instruments_type
    ON instruments(exchange_segment, instrument_type, option_type);

CREATE TABLE IF NOT EXISTS websocket_messages (
    message_id INTEGER PRIMARY KEY AUTOINCREMENT,
    received_at_ns INTEGER NOT NULL,
    received_at TEXT NOT NULL,
    message_type TEXT NOT NULL,
    tick_count INTEGER NOT NULL DEFAULT 0,
    raw_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ws_messages_received
    ON websocket_messages(received_at_ns);

CREATE TABLE IF NOT EXISTS market_ticks (
    tick_id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER NOT NULL,
    received_at_ns INTEGER NOT NULL,
    feed_timestamp TEXT,
    last_trade_timestamp TEXT,
    instrument_token TEXT NOT NULL,
    exchange_segment TEXT NOT NULL,
    ltp REAL,
    change REAL,
    change_pct REAL,
    volume INTEGER,
    open_interest INTEGER,
    turnover REAL,
    bid_price REAL,
    ask_price REAL,
    bid_quantity INTEGER,
    ask_quantity INTEGER,
    total_buy_quantity INTEGER,
    total_sell_quantity INTEGER,
    average_price REAL,
    FOREIGN KEY (message_id) REFERENCES websocket_messages(message_id)
        ON DELETE CASCADE,
    FOREIGN KEY (instrument_token, exchange_segment)
        REFERENCES instruments(instrument_token, exchange_segment)
);
CREATE INDEX IF NOT EXISTS idx_ticks_token_time
    ON market_ticks(exchange_segment, instrument_token, received_at_ns);
CREATE INDEX IF NOT EXISTS idx_ticks_time ON market_ticks(received_at_ns);
CREATE INDEX IF NOT EXISTS idx_ticks_feed_time ON market_ticks(feed_timestamp);

CREATE TABLE IF NOT EXISTS collector_sessions (
    session_id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    stopped_at TEXT,
    status TEXT NOT NULL,
    messages_received INTEGER NOT NULL DEFAULT 0,
    ticks_received INTEGER NOT NULL DEFAULT 0,
    ticks_written INTEGER NOT NULL DEFAULT 0,
    errors INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS websocket_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_time TEXT NOT NULL,
    event_time_ns INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    message TEXT,
    connection_attempt INTEGER NOT NULL DEFAULT 0,
    market_session INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_ws_events_time
    ON websocket_events(event_time_ns);

CREATE INDEX IF NOT EXISTS idx_ws_events_type
    ON websocket_events(event_type);
"""

def create_database() -> None:
    conn = get_connection()
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()

if __name__ == "__main__":
    create_database()
    print(f"Database ready: {DB_PATH}")
