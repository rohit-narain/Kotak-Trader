from __future__ import annotations
import json
import time
from datetime import datetime, timezone
from venv import logger
from kotak_trader.database import get_connection

def utc_now_ns():
    return time.time_ns()

def utc_iso_from_ns(ns):
    return datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc).isoformat()

def as_float(value):
    return None if value in (None, "") else float(value)

def as_int(value):
    return None if value in (None, "") else int(float(value))

def normalize_timestamp(timestamp: str | None) -> str | None:
    """
    Convert Kotak timestamp:
        DD/MM/YYYY HH:MM:SS

    to:
        YYYY-MM-DDTHH:MM:SS
    """
    if not timestamp:
        return None

    try:
        return datetime.strptime(
            timestamp.strip(),
            "%d/%m/%Y %H:%M:%S",
        ).isoformat(timespec="seconds")

    except ValueError:
        logger.warning(
            "Invalid Kotak timestamp: %r",
            timestamp,
        )
        return None



TICK_INSERT = """
INSERT INTO market_ticks (
    message_id, received_at_ns, feed_timestamp, last_trade_timestamp,
    instrument_token, exchange_segment, ltp, change, change_pct,
    volume, open_interest, turnover, bid_price, ask_price,
    bid_quantity, ask_quantity, total_buy_quantity, total_sell_quantity,
    average_price
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

def normalize_tick(tick, message_id, received_at_ns):
    # Kotak can send partial/delta updates. Missing fields stay NULL.
    return (
        message_id, received_at_ns, normalize_timestamp(tick.get("fdtm")), normalize_timestamp(tick.get("ltt")),
        str(tick["tk"]), tick["e"],
        as_float(tick.get("ltp")), as_float(tick.get("cng")),
        as_float(tick.get("nc")), as_int(tick.get("v")),
        as_int(tick.get("oi")), as_float(tick.get("to")),
        as_float(tick.get("bp")), as_float(tick.get("sp")),
        as_int(tick.get("bq")), as_int(tick.get("bs")),
        as_int(tick.get("tbq")), as_int(tick.get("tsq")),
        as_float(tick.get("ap")),
    )

class MarketDataWriter:
    def __init__(self, message_queue, batch_size=2000, flush_interval=1.0):
        self.queue = message_queue
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.messages_written = 0
        self.ticks_written = 0
        self.errors = 0

    def write_batch(self, messages):
        if not messages:
            return

        conn = get_connection()
        try:
            received_times = [utc_now_ns() for _ in messages]
            message_ids = []

            for message, received_ns in zip(messages, received_times):
                raw = json.dumps(message, separators=(",", ":"),
                                 ensure_ascii=False, default=str)
                cur = conn.execute(
                    """INSERT INTO websocket_messages
                       (received_at_ns, received_at, message_type, tick_count, raw_json)
                       VALUES (?, ?, ?, ?, ?)""",
                    (received_ns, utc_iso_from_ns(received_ns),
                     str(message.get("type", "unknown")),
                     len(message.get("data", []))
                     if message.get("type") == "stock_feed" else 0,
                     raw),
                )
                message_ids.append(cur.lastrowid)

            rows = []
            for message, message_id, received_ns in zip(
                messages, message_ids, received_times
            ):
                if message.get("type") != "stock_feed":
                    continue
                for tick in message.get("data", []):
                    if "tk" in tick and "e" in tick:
                        rows.append(normalize_tick(tick, message_id, received_ns))

            if rows:
                conn.executemany(TICK_INSERT, rows)

            conn.commit()
            self.messages_written += len(messages)
            self.ticks_written += len(rows)

        except Exception:
            conn.rollback()
            self.errors += 1
            raise
        finally:
            conn.close()

    def run(self, stop_event):
        batch = []
        last_flush = time.monotonic()

        while not stop_event.is_set() or not self.queue.empty():
            try:
                message = self.queue.get(timeout=0.1)
                batch.append(message)
                self.queue.task_done()
            except Exception:
                message = None

            now = time.monotonic()
            if batch and (
                len(batch) >= self.batch_size
                or now - last_flush >= self.flush_interval
            ):
                self.write_batch(batch)
                batch.clear()
                last_flush = now

        if batch:
            self.write_batch(batch)

def save_message_and_ticks(message):
    writer = MarketDataWriter(None)
    writer.write_batch([message])
