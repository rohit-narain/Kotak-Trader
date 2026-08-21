from __future__ import annotations

import logging
import queue
import signal
import threading
import time
from datetime import datetime, time as dt_time
from zoneinfo import ZoneInfo

from kotak_trader.auth import get_authenticated_client
from kotak_trader.database import create_database, get_connection
from kotak_trader.market_data_loader import MarketDataWriter


# ============================================================
# CONFIGURATION
# ============================================================

QUEUE_MAXSIZE = 50_000

# Number of WebSocket messages written in one DB transaction
BATCH_SIZE = 2_000

# Flush queued messages at least this often
FLUSH_INTERVAL = 1.0

# Reconnect delays
INITIAL_RECONNECT_DELAY = 2
MAX_RECONNECT_DELAY = 60

# Market hours
IST = ZoneInfo("Asia/Kolkata")
MARKET_OPEN = dt_time(9, 15)
MARKET_CLOSE = dt_time(15, 30)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================
# GLOBAL STATE
# ============================================================

# WebSocket -> database queue
message_queue = queue.Queue(maxsize=QUEUE_MAXSIZE)

# Set when the entire collector should stop
stop_event = threading.Event()

# Set when WebSocket connection is lost
connection_lost = threading.Event()

# Connection attempt number
current_connection_attempt = 0

# Protects connection attempt updates
connection_lock = threading.Lock()


# ============================================================
# MARKET HOURS
# ============================================================

def is_market_hours() -> bool:
    """
    Return True when NSE/BSE regular market hours are active.

    This is intentionally based on clock time only.
    Exchange holidays can be added later if required.
    """

    now = datetime.now(IST)

    # Saturday / Sunday
    if now.weekday() >= 5:
        return False

    current_time = now.time()

    return MARKET_OPEN <= current_time <= MARKET_CLOSE


# ============================================================
# DATABASE CONNECTION EVENT LOGGING
# ============================================================

def log_ws_event(
    event_type: str,
    message=None,
    connection_attempt: int = 0,
) -> None:
    """
    Record WebSocket lifecycle events in SQLite.

    Examples:

        CONNECTING
        CONNECTED
        SUBSCRIBED
        ERROR
        CLOSED
        RECONNECTING
        SUPERVISOR_ERROR
    """

    now = datetime.now(IST)

    try:
        conn = get_connection()

        try:
            conn.execute(
                """
                INSERT INTO websocket_events (
                    event_time,
                    event_time_ns,
                    event_type,
                    message,
                    connection_attempt,
                    market_session
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    now.isoformat(),
                    time.time_ns(),
                    event_type,
                    str(message) if message is not None else None,
                    connection_attempt,
                    int(is_market_hours()),
                ),
            )

            conn.commit()

        finally:
            conn.close()

    except Exception:
        # Do NOT allow database logging failure to kill
        # the WebSocket collector.
        logger.exception(
            "Failed to write WebSocket event to database: %s",
            event_type,
        )


# ============================================================
# WEBSOCKET CALLBACKS
# ============================================================

def on_message(message):
    """
    WebSocket message callback.

    IMPORTANT:
    No database I/O occurs here.

    The callback simply places the message onto the queue.
    """

    if not isinstance(message, dict):
        logger.warning(
            "Received unexpected WebSocket message type: %s",
            type(message),
        )
        return

    if message.get("type") != "stock_feed":
        logger.debug(
            "Received non-stock-feed message: %s",
            message,
        )
        return

    try:

        message_queue.put(
            message,
            timeout=2,
        )

    except queue.Full:

        logger.critical(
            "Market-data queue is full. "
            "Database writer may be falling behind."
        )

        log_ws_event(
            "QUEUE_FULL",
            "Market-data queue reached maximum capacity.",
            current_connection_attempt,
        )

        # Do not silently lose data.
        #
        # Stopping the collector causes the problem to become
        # visible instead of continuing with an unknown data gap.
        stop_event.set()


def on_error(error):
    """
    Called when the WebSocket reports an error.

    Do NOT stop the collector here.

    The supervisor thread will reconnect.
    """

    logger.error(
        "========== WEBSOCKET ERROR ==========\n%s",
        error,
    )

    log_ws_event(
        "ERROR",
        error,
        current_connection_attempt,
    )

    connection_lost.set()


def on_close(message):
    """
    Called when the WebSocket connection closes.

    A close is NOT necessarily fatal.

    During market hours the supervisor will reconnect.
    """

    logger.warning(
        "========== WEBSOCKET CLOSED ==========\n%s",
        message,
    )

    log_ws_event(
        "CLOSED",
        message,
        current_connection_attempt,
    )

    connection_lost.set()


def on_open(message):
    """
    Called when a WebSocket connection opens.
    """

    logger.info(
        "========== WEBSOCKET OPENED ==========\n%s",
        message,
    )

    log_ws_event(
        "CONNECTED",
        message,
        current_connection_attempt,
    )


# ============================================================
# INSTRUMENT SUBSCRIPTIONS
# ============================================================

def get_subscription_tokens():
    """
    Return instruments to subscribe to.

    These are the derivative tokens you were testing with.

    Later this function can dynamically query the instrument
    database instead of hard-coding tokens.
    """

    return [
        {
            "exchange_segment": "nse_fo",
            "instrument_token": "48704",
        },
        {
            "exchange_segment": "nse_fo",
            "instrument_token": "58072",
        },
        {
            "exchange_segment": "nse_fo",
            "instrument_token": "68407",
        },
        {
            "exchange_segment": "nse_fo",
            "instrument_token": "61726",
        },
        {
            "exchange_segment": "nse_fo",
            "instrument_token": "61720",
        },
    ]


# ============================================================
# CREATE A NEW WEBSOCKET CONNECTION
# ============================================================

def connect_and_subscribe():
    """
    Create a completely new authenticated client and
    subscribe to all required instruments.

    Every reconnect creates a fresh client.
    """

    global current_connection_attempt

    with connection_lock:
        current_connection_attempt += 1
        attempt = current_connection_attempt

    logger.info(
        f"""==================================================
             Connecting to Kotak WebSocket
             Connection attempt: {attempt}"
             Market hours: {is_market_hours()}"
        ==================================================""")
 
    log_ws_event(
        "CONNECTING",
        f"Connection attempt {attempt}",
        attempt,
    )

    # --------------------------------------------------------
    # Get a fresh authenticated client
    # --------------------------------------------------------

    client = get_authenticated_client()

    if client is None:
        raise RuntimeError(
            "get_authenticated_client() returned None"
        )

    # --------------------------------------------------------
    # Register callbacks
    # --------------------------------------------------------

    client.on_message = on_message
    client.on_error = on_error
    client.on_close = on_close
    client.on_open = on_open

    # --------------------------------------------------------
    # Get subscriptions
    # --------------------------------------------------------

    tokens = get_subscription_tokens()

    logger.info(
        "Subscribing to %d instruments",
        len(tokens),
    )

    logger.info(
        "Tokens: %s",
        tokens,
    )

    # --------------------------------------------------------
    # Subscribe
    #
    # Keep this signature consistent with your working
    # Kotak implementation.
    # --------------------------------------------------------

    client.subscribe(
        instrument_tokens=tokens,
        isDepth=False,
    )

    log_ws_event(
        "SUBSCRIBED",
        f"{len(tokens)} instruments subscribed",
        attempt,
    )

    logger.info(
        "Successfully subscribed to %d instruments",
        len(tokens),
    )

    return client


# ============================================================
# DATABASE WRITER THREAD
# ============================================================

def writer_worker():
    """
    Dedicated database writer.

    The WebSocket thread NEVER writes directly to SQLite.

    This thread:
        Queue
          ->
        batch
          ->
        SQLite transaction
    """

    logger.info(
        "Database writer starting..."
    )

    writer = MarketDataWriter(
        message_queue,
        batch_size=BATCH_SIZE,
        flush_interval=FLUSH_INTERVAL,
    )

    try:

        writer.run(stop_event)

    except Exception:

        logger.exception(
            "Fatal error in database writer"
        )

        log_ws_event(
            "DATABASE_WRITER_ERROR",
            "Database writer terminated unexpectedly.",
            current_connection_attempt,
        )

        stop_event.set()

    finally:

        logger.info(
            "Database writer stopped."
        )

        logger.info(
            "Messages written: %d",
            writer.messages_written,
        )

        logger.info(
            "Ticks written: %d",
            writer.ticks_written,
        )

        logger.info(
            "Writer errors: %d",
            writer.errors,
        )


# ============================================================
# WEBSOCKET SUPERVISOR
# ============================================================

def websocket_supervisor():
    """
    Owns the WebSocket lifecycle.

    Responsibilities:

        1. Connect
        2. Subscribe
        3. Wait for connection loss
        4. Detect disconnect
        5. Log it
        6. Reconnect during market hours
        7. Re-subscribe
        8. Continue collecting
    """

    reconnect_delay = INITIAL_RECONNECT_DELAY

    while not stop_event.is_set():

        # ----------------------------------------------------
        # Reset connection state
        # ----------------------------------------------------

        connection_lost.clear()

        # ----------------------------------------------------
        # Attempt connection
        # ----------------------------------------------------

        try:

            client = connect_and_subscribe()

            logger.info(
                "WebSocket connection established."
            )

            # Successful connection:
            # reset exponential backoff.
            reconnect_delay = INITIAL_RECONNECT_DELAY

        except Exception as exc:

            logger.exception(
                "Failed to establish WebSocket connection."
            )

            log_ws_event(
                "CONNECTION_FAILED",
                repr(exc),
                current_connection_attempt,
            )

            connection_lost.set()

        # ----------------------------------------------------
        # Wait while connection remains alive
        # ----------------------------------------------------

        while (
            not stop_event.is_set()
            and not connection_lost.wait(timeout=1)
        ):

            pass

        # ----------------------------------------------------
        # Collector is shutting down
        # ----------------------------------------------------

        if stop_event.is_set():

            logger.info(
                "Stop event received. "
                "WebSocket supervisor exiting."
            )

            break

        # ----------------------------------------------------
        # WebSocket has disconnected
        # ----------------------------------------------------

        logger.warning(
            "WebSocket connection has been lost."
        )

        # ----------------------------------------------------
        # Market is OPEN
        # ----------------------------------------------------

        if is_market_hours():

            logger.warning(
                "Market is OPEN."
            )

            logger.warning(
                "Automatic reconnection will be attempted."
            )

            log_ws_event(
                "RECONNECTING",
                "Market open - automatic reconnect.",
                current_connection_attempt,
            )

        # ----------------------------------------------------
        # Market is CLOSED
        # ----------------------------------------------------

        else:

            logger.info(
                "Market is currently closed."
            )

            log_ws_event(
                "DISCONNECTED_OUTSIDE_MARKET",
                "Market closed - reconnect delayed.",
                current_connection_attempt,
            )

        # ----------------------------------------------------
        # Wait before reconnecting
        # ----------------------------------------------------

        logger.info(
            "Waiting %d seconds before reconnect...",
            reconnect_delay,
        )

        stop_event.wait(reconnect_delay)

        # ----------------------------------------------------
        # Increase delay for repeated failures.
        #
        # 2 -> 4 -> 8 -> 16 -> 32 -> 60 seconds
        # ----------------------------------------------------

        reconnect_delay = min(
            reconnect_delay * 2,
            MAX_RECONNECT_DELAY,
        )


# ============================================================
# SHUTDOWN
# ============================================================

def shutdown(signum=None, frame=None):
    """
    Graceful shutdown.

    We do NOT immediately kill the writer.

    Setting stop_event causes:
        1. WebSocket supervisor to stop.
        2. Writer to drain remaining queue.
        3. Main thread to wait for writer.
    """

    logger.info(
        "=================================================="
    )

    logger.info(
        "Shutdown requested."
    )

    logger.info(
        "Waiting for queued market data to be written..."
    )

    logger.info(
        "=================================================="
    )

    log_ws_event(
        "SHUTDOWN",
        "Collector shutdown requested.",
        current_connection_attempt,
    )

    stop_event.set()
    connection_lost.set()


# ============================================================
# MAIN
# ============================================================

def main():

    logger.info(
        "=================================================="
    )

    logger.info(
        "KOTAK MARKET DATA COLLECTOR"
    )

    logger.info(
        "=================================================="
    )

    # --------------------------------------------------------
    # Create database/tables if necessary
    # --------------------------------------------------------

    logger.info(
        "Initializing database..."
    )

    create_database()

    # --------------------------------------------------------
    # Register shutdown handlers
    # --------------------------------------------------------

    signal.signal(
        signal.SIGINT,
        shutdown,
    )

    signal.signal(
        signal.SIGTERM,
        shutdown,
    )

    # --------------------------------------------------------
    # Start database writer
    # --------------------------------------------------------

    writer_thread = threading.Thread(
        target=writer_worker,
        name="market-data-writer",
        daemon=False,
    )

    writer_thread.start()

    logger.info(
        "Database writer started."
    )

    # --------------------------------------------------------
    # Start WebSocket supervisor
    # --------------------------------------------------------

    try:

        websocket_supervisor()

    except KeyboardInterrupt:

        logger.info(
            "Keyboard interrupt received."
        )

        shutdown()

    except Exception as exc:

        logger.exception(
            "Fatal error in WebSocket supervisor."
        )

        log_ws_event(
            "SUPERVISOR_ERROR",
            repr(exc),
            current_connection_attempt,
        )

        stop_event.set()

    finally:

        # ----------------------------------------------------
        # Ensure writer eventually stops
        # ----------------------------------------------------

        stop_event.set()

        logger.info(
            "Waiting for database writer to finish..."
        )

        writer_thread.join(
            timeout=30
        )

        if writer_thread.is_alive():

            logger.error(
                "Database writer did not stop within 30 seconds."
            )

        else:

            logger.info(
                "Database writer stopped cleanly."
            )

        logger.info(
            "=================================================="
        )

        logger.info(
            "KOTAK MARKET DATA COLLECTOR STOPPED"
        )

        logger.info(
            "==================================================")


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()