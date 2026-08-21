from __future__ import annotations
import csv
from pathlib import Path
from kotak_trader.database import get_connection

def clean(value):
    if value is None:
        return None
    value = value.strip()
    return value if value else None

def to_float(value):
    value = clean(value)
    return None if value is None or value == "-1" else float(value)

def to_int(value):
    value = clean(value)
    return None if value is None or value == "-1" else int(float(value))

def normalize_row(row, source_file):
    r = {k.strip(): v for k, v in row.items()}
    g = lambda k: clean(r.get(k))
    return (
        g("pSymbol"), g("pExchSeg"), g("pSymbol"), g("pTrdSymbol"),
        g("pInstType"), g("pOptionType"), g("pExpiryDate"),
        to_float(g("dStrikePrice;")), to_int(g("lLotSize")),
        to_float(g("dTickSize ")), g("pISIN"), g("pContractId"),
        g("pInstName"), source_file, g("pLocalUpdateTime"),
    )

UPSERT = """
INSERT INTO instruments (
    instrument_token, exchange_segment, symbol, trading_symbol,
    instrument_type, option_type, expiry_date, strike_price,
    lot_size, tick_size, isin, contract_id, instrument_name,
    source_file, source_updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(instrument_token, exchange_segment) DO UPDATE SET
    symbol=excluded.symbol, trading_symbol=excluded.trading_symbol,
    instrument_type=excluded.instrument_type, option_type=excluded.option_type,
    expiry_date=excluded.expiry_date, strike_price=excluded.strike_price,
    lot_size=excluded.lot_size, tick_size=excluded.tick_size,
    isin=excluded.isin, contract_id=excluded.contract_id,
    instrument_name=excluded.instrument_name, source_file=excluded.source_file,
    source_updated_at=excluded.source_updated_at, loaded_at=CURRENT_TIMESTAMP
"""

def load_script_master(csv_path):
    csv_path = Path(csv_path)
    conn = get_connection()
    count = 0
    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            rows = []
            for row in reader:
                token = clean(row.get("pSymbol"))
                exchange = clean(row.get("pExchSeg"))
                if not token or not exchange:
                    continue
                rows.append(normalize_row(row, csv_path.name))
                if len(rows) >= 5000:
                    conn.executemany(UPSERT, rows)
                    count += len(rows)
                    rows.clear()
            if rows:
                conn.executemany(UPSERT, rows)
                count += len(rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return count

def load_all_script_masters(directory):
    directory = Path(directory)
    total = 0
    for csv_file in sorted(directory.glob("*.csv")):
        n = load_script_master(csv_file)
        print(f"{csv_file.name}: {n:,}")
        total += n
    print(f"Total loaded/upserted: {total:,}")
    return total

if __name__ == "__main__":
    from kotak_trader.database import create_database
    create_database()
    directory = Path(__file__).resolve().parents[2] / "data" / "script_master"
    load_all_script_masters(directory)
