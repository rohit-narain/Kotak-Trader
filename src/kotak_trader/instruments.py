from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from kotak_trader.database import initialize_database, save_instrument


SCRIPT_MASTER_DIR = Path("data/script_master")


INDEX_DEFINITIONS = {
    "NIFTY": {
        "file": "nse_cm-v1.csv",
        "symbol": "NIFTY",
    },
    "BANKNIFTY": {
        "file": "nse_cm-v1.csv",
        "symbol": "BANKNIFTY",
    },
    "SENSEX": {
        "file": "bse_cm-v1.csv",
        "symbol": "SENSEX",
    },
}


def load_script_master(filename):

    path = SCRIPT_MASTER_DIR / filename

    if not path.exists():
        raise FileNotFoundError(
            f"Script master not found: {path}"
        )

    return pd.read_csv(
        path,
        dtype=str
    )


def get_index_instrument(name):

    definition = INDEX_DEFINITIONS[name]

    df = load_script_master(
        definition["file"]
    )

    matches = df[
        df["pSymbolName"]
        .fillna("")
        .str.upper()
        == definition["symbol"]
    ]

    if len(matches) == 0:
        raise ValueError(
            f"Could not find {name}"
        )

    if len(matches) > 1:
        raise ValueError(
            f"Multiple matches found for {name}"
        )

    row = matches.iloc[0]

    return {
        "symbol": name,
        "instrument_token": row["pSymbol"],
        "exchange_segment": row["pExchSeg"],
        "instrument_type": row["pInstType"],
        "trading_symbol": row["pTrdSymbol"],
        "last_updated": datetime.now(
            timezone.utc
        ).isoformat(),
    }


def get_index_instruments():

    return [
        get_index_instrument("NIFTY"),
        get_index_instrument("BANKNIFTY"),
        get_index_instrument("SENSEX"),
    ]


def update_instrument_database():

    initialize_database()

    instruments = get_index_instruments()

    for instrument in instruments:

        save_instrument(instrument)

        print(
            f"{instrument['symbol']:10} "
            f"token={instrument['instrument_token']:>6} "
            f"segment={instrument['exchange_segment']}"
        )


if __name__ == "__main__":
    update_instrument_database()