"""
Standalone OHLCV candle creator for Kotak Trader.

The module is independent of market_data.py. It reads raw ticks and
connection events from SQLite and creates idempotent candles.

CLI examples:

    uv run python -m kotak_trader.candle_creator --date 2026-08-17

    uv run python -m kotak_trader.candle_creator \
        --date 2026-08-17 \
        --start-time 09:15 \
        --end-time 15:30 \
        --candle-minutes 5
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

LOGGER = logging.getLogger("kotak_trader.candle_creator")

DEFAULT_START_TIME = "09:15"
DEFAULT_END_TIME = "15:30"
DEFAULT_CANDLE_MINUTES = 5
DEFAULT_DB_PATH = Path("data/market_data.db")


CREATE_OHLCV_TABLE = """
CREATE TABLE IF NOT EXISTS ohlcv (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    instrument_token TEXT NOT NULL,
    symbol TEXT NOT NULL,

    trading_date TEXT NOT NULL,
    candle_start TEXT NOT NULL,
    candle_end TEXT NOT NULL,
    candle_minutes INTEGER NOT NULL,

    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL DEFAULT 0,

    tick_count INTEGER NOT NULL DEFAULT 0,

    connection_dropped INTEGER NOT NULL DEFAULT 0,
    incomplete_candle INTEGER NOT NULL DEFAULT 0,
    data_quality TEXT NOT NULL DEFAULT 'GOOD',

    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (
        instrument_token,
        candle_start,
        candle_minutes
    )
);

CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_time
ON ohlcv(symbol, candle_start);

CREATE INDEX IF NOT EXISTS idx_ohlcv_date
ON ohlcv(trading_date);
"""


def _parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _make_datetime(
    trading_date: str,
    time_value: str,
) -> datetime:
    if len(time_value) == 5:
        time_value += ":00"
    return datetime.fromisoformat(f"{trading_date} {time_value}")


def _validate(
    trading_date: str,
    start_time: str,
    end_time: str,
    candle_minutes: int,
) -> tuple[datetime, datetime]:
    if candle_minutes <= 0:
        raise ValueError("candle_minutes must be greater than zero")

    start = _make_datetime(trading_date, start_time)
    end = _make_datetime(trading_date, end_time)

    if end <= start:
        raise ValueError("end_time must be later than start_time")

    return start, end


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {row[1] for row in rows}


def _resolve_schema(conn: sqlite3.Connection) -> dict[str, str]:
    """
    Resolve the existing tick/event schema.

    Expected tables:
        market_ticks
        websocket_events

    Several common column names are accepted so this module can be
    connected to the existing Kotak Trader database without changing
    market_data.py.
    """
    tick_cols = _columns(conn, "market_ticks")
    event_cols = _columns(conn, "websocket_events")

    return {
        "tick_time": "feed_timestamp",
        "token": "instrument_token",
        # "symbol": ,
        "price": "ltp",
        "volume": "volume",
        "event_time": "received_at_ns"
        # "event_type": ,
    }


def _load_ticks(
    conn: sqlite3.Connection,
    schema: dict[str, str],
    start: datetime,
    end: datetime,
) -> list[sqlite3.Row]:
    c = schema

    sql = f"""
        SELECT
            "{c['token']}" AS instrument_token,
        strftime(
                    '%Y-%m-%d %H:%M:%S',
                    substr(last_trade_timestamp, 7, 4) || '-' ||
                    substr(last_trade_timestamp, 4, 2) || '-' ||
                    substr(last_trade_timestamp, 1, 2) || ' ' ||
                    substr(last_trade_timestamp, 12, 8)
                ) AS tick_time,
            "{c['price']}" AS price,
            "{c['volume']}" AS volume
        FROM market_ticks
        WHERE "{c['tick_time']}" >= ?
          AND "{c['tick_time']}" < ?
        ORDER BY "{c['token']}", "{c['tick_time']}"
    """

    return conn.execute(
        sql,
        (
            start.isoformat(sep=" "),
            end.isoformat(sep=" "),
        ),
    ).fetchall()


def _load_connection_events(
    conn: sqlite3.Connection,
    schema: dict[str, str],
    end: datetime,
) -> list[sqlite3.Row]:
    c = schema

    sql = f"""
        SELECT
            "{c['event_time']}" AS event_time,
            "{c['event_type']}" AS event_type
        FROM websocket_events
        WHERE "{c['event_time']}" < ?
        ORDER BY "{c['event_time']}"
    """

    return conn.execute(
        sql,
        (end.isoformat(sep=" "),),
    ).fetchall()


def _is_disconnect(event_type: str) -> bool:
    value = event_type.upper().replace("-", "_").replace(" ", "_")

    return value in {
        "DISCONNECTED",
        "DISCONNECT",
        "CONNECTION_DROPPED",
        "CONNECTION_LOST",
        "WEBSOCKET_CLOSED",
        "ERROR",
    }


def _is_connect(event_type: str) -> bool:
    value = event_type.upper().replace("-", "_").replace(" ", "_")

    return value in {
        "CONNECTED",
        "CONNECT",
        "RECONNECTED",
        "RECONNECT",
    }


def _connection_quality(
    events: list[sqlite3.Row],
    candle_start: datetime,
    candle_end: datetime,
) -> tuple[int, int, str]:
    """
    Determine connection quality for one candle.

    connection_dropped:
        1 if a disconnect/error occurred during the candle or the
        connection was already down at its beginning.

    incomplete_candle:
        1 if the connection was still down at candle_end.
    """
    connected = True
    dropped = False

    for event in events:
        event_time = _parse_dt(str(event["event_time"]))
        event_type = str(event["event_type"])

        if event_time <= candle_start:
            if _is_disconnect(event_type):
                connected = False
            elif _is_connect(event_type):
                connected = True
            continue

        if event_time >= candle_end:
            break

        if _is_disconnect(event_type):
            connected = False
            dropped = True
        elif _is_connect(event_type):
            connected = True

    if not connected:
        dropped = True
        incomplete = True
    else:
        incomplete = False

    if incomplete:
        quality = "INCOMPLETE"
    elif dropped:
        quality = "CONNECTION_GAP"
    else:
        quality = "GOOD"

    return int(dropped), int(incomplete), quality


def _aggregate(
    ticks: list[sqlite3.Row],
    candle_start: datetime,
    candle_end: datetime,
) -> dict[tuple[str, str], dict]:
    """
    Aggregate ticks for one candle interval.

    OHLC:
        open  = first tick price
        high  = maximum price
        low   = minimum price
        close = last tick price

    Volume:
        This implementation treats the volume field as cumulative
        traded volume and uses last - first.

        If the existing Kotak tick table stores per-tick volume instead,
        this should be changed to SUM(volume).
    """
    grouped: dict[tuple[str, str], list[sqlite3.Row]] = {}

    for tick in ticks:
        tick_time = _parse_dt(str(tick["tick_time"]))

        if not (candle_start <= tick_time < candle_end):
            continue

        key = (
            str(tick["instrument_token"]),
            str(tick["symbol"]),
        )
        grouped.setdefault(key, []).append(tick)

    result = {}

    for key, rows in grouped.items():
        rows.sort(key=lambda row: _parse_dt(str(row["tick_time"])))

        prices = [float(row["price"]) for row in rows]

        first_volume = rows[0]["volume"]
        last_volume = rows[-1]["volume"]

        if first_volume is None or last_volume is None:
            volume = 0
        else:
            volume = max(0, float(last_volume) - float(first_volume))

        result[key] = {
            "instrument_token": key[0],
            "symbol": key[1],
            "open": prices[0],
            "high": max(prices),
            "low": min(prices),
            "close": prices[-1],
            "volume": volume,
            "tick_count": len(rows),
        }

    return result


def create_ohlcv(
    trading_date: str,
    start_time: str = DEFAULT_START_TIME,
    end_time: str = DEFAULT_END_TIME,
    candle_minutes: int = DEFAULT_CANDLE_MINUTES,
    db_path: str | Path = DEFAULT_DB_PATH,
) -> int:
    """
    Create OHLCV candles for the requested period.

    Parameters
    ----------
    trading_date:
        YYYY-MM-DD.

    start_time:
        HH:MM. Defaults to 09:15.

    end_time:
        HH:MM. Defaults to 15:30.

    candle_minutes:
        Candle width in minutes. Defaults to 5.

    db_path:
        SQLite database path.

    Returns
    -------
    int
        Number of candles inserted/updated.
    """
    start, end = _validate(
        trading_date,
        start_time,
        end_time,
        candle_minutes,
    )

    db_path = Path(db_path)

    if not db_path.exists():
        raise FileNotFoundError(
            f"Database not found: {db_path}. "
            "Pass the correct path with --db."
        )

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        conn.executescript(CREATE_OHLCV_TABLE)
        schema = _resolve_schema(conn)

        ticks = _load_ticks(conn, schema, start, end)
        events = _load_connection_events(conn, schema, end)

        LOGGER.info(
            "Creating %s-minute candles for %s: %s-%s",
            candle_minutes,
            trading_date,
            start_time,
            end_time,
        )
        LOGGER.info("Ticks loaded: %s", len(ticks))

        created = 0
        candle_start = start

        while candle_start < end:
            candle_end = min(
                candle_start + timedelta(minutes=candle_minutes),
                end,
            )

            aggregate = _aggregate(
                ticks,
                candle_start,
                candle_end,
            )

            connection_dropped, incomplete, quality = (
                _connection_quality(
                    events,
                    candle_start,
                    candle_end,
                )
            )

            for data in aggregate.values():
                conn.execute(
                    """
                    INSERT INTO ohlcv (
                        instrument_token,
                        symbol,
                        trading_date,
                        candle_start,
                        candle_end,
                        candle_minutes,
                        open,
                        high,
                        low,
                        close,
                        volume,
                        tick_count,
                        connection_dropped,
                        incomplete_candle,
                        data_quality
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (
                        instrument_token,
                        candle_start,
                        candle_minutes
                    )
                    DO UPDATE SET
                        symbol = excluded.symbol,
                        trading_date = excluded.trading_date,
                        candle_end = excluded.candle_end,
                        open = excluded.open,
                        high = excluded.high,
                        low = excluded.low,
                        close = excluded.close,
                        volume = excluded.volume,
                        tick_count = excluded.tick_count,
                        connection_dropped = excluded.connection_dropped,
                        incomplete_candle = excluded.incomplete_candle,
                        data_quality = excluded.data_quality,
                        created_at = CURRENT_TIMESTAMP
                    """,
                    (
                        data["instrument_token"],
                        data["symbol"],
                        trading_date,
                        candle_start.isoformat(sep=" "),
                        candle_end.isoformat(sep=" "),
                        candle_minutes,
                        data["open"],
                        data["high"],
                        data["low"],
                        data["close"],
                        data["volume"],
                        data["tick_count"],
                        connection_dropped,
                        incomplete,
                        quality,
                    ),
                )
                created += 1

            candle_start = candle_end

        conn.commit()

        LOGGER.info("OHLCV candles created/updated: %s", created)
        return created

    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create OHLCV candles from Kotak Trader tick data."
    )

    parser.add_argument(
        "--date",
        required=True,
        dest="trading_date",
        help="Trading date, e.g. 2026-08-17",
    )

    parser.add_argument(
        "--start-time",
        default=DEFAULT_START_TIME,
        help=f"Start time, default: {DEFAULT_START_TIME}",
    )

    parser.add_argument(
        "--end-time",
        default=DEFAULT_END_TIME,
        help=f"End time, default: {DEFAULT_END_TIME}",
    )

    parser.add_argument(
        "--candle-minutes",
        type=int,
        default=DEFAULT_CANDLE_MINUTES,
        help=f"Candle width in minutes, default: {DEFAULT_CANDLE_MINUTES}",
    )

    parser.add_argument(
        "--db",
        default=str(DEFAULT_DB_PATH),
        help=f"SQLite database path, default: {DEFAULT_DB_PATH}",
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    create_ohlcv(
        trading_date=args.trading_date,
        start_time=args.start_time,
        end_time=args.end_time,
        candle_minutes=args.candle_minutes,
        db_path=args.db,
    )


if __name__ == "__main__":
    main()
