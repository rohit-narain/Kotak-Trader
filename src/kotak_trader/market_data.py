import json

from kotak_trader.auth import get_authenticated_client
from kotak_trader.database import get_connection


def on_message(message):

    print("\n========== TICK ==========")
    print(json.dumps(message, indent=2, default=str))


def on_error(error):

    print("\n========== WEBSOCKET ERROR ==========")
    print(error)


def on_close(message):

    print("\n========== WEBSOCKET CLOSED ==========")
    print(message)


def on_open(message):

    print("\n========== WEBSOCKET OPEN ==========")
    print(message)


def get_instruments():

    connection = get_connection()

    rows = connection.execute("""
        SELECT
            instrument_token,
            symbol,
            exchange_segment
        FROM instruments
        WHERE symbol IN (
            'NIFTY',
            'BANKNIFTY',
            'SENSEX'
        )
        ORDER BY symbol
    """).fetchall()

    connection.close()

    return rows


def main():

    client = get_authenticated_client()

    instruments = get_instruments()

    print("\nInstruments:")
    for instrument in instruments:
        print(instrument)

    instrument_tokens = [
        {
            "instrument_token": row[0],
            "exchange_segment": row[2],
        }
        for row in instruments
    ]

    print("\nSubscribing:")
    print(instrument_tokens)

    client.on_message = on_message
    client.on_error = on_error
    client.on_close = on_close
    client.on_open = on_open

    client.subscribe(
        instrument_tokens=instrument_tokens,
        isIndex=True,
        isDepth=False
    )


if __name__ == "__main__":
    main()