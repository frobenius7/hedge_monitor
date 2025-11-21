import os
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# Lighter API
LIGHTER_API_KEY = os.getenv("LIGHTER_API_KEY")
LIGHTER_API_URL = "https://mainnet.zklighter.elliot.ai/api/v1/account"
ADDRESSES = [a.strip() for a in os.getenv("LIGHTER_L1_ADDRESSES", "").split(",") if a.strip()]

# Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE_LG", "lighter_balance_logs")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_total_asset_value(address: str) -> float:
    """Берём total_asset_value из accounts[0]."""
    params = {
        #"Key": LIGHTER_API_KEY,
        "by": "l1_address",
        "value": address,
    }

    r = requests.get(LIGHTER_API_URL, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()

    accounts = data.get("accounts", [])
    if not accounts:
        print(f"[WARN] accounts пустой → {address}")
        return 0.0

    value_str = accounts[0].get("total_asset_value", "0")
    try:
        return float(value_str)
    except:
        print(f"[WARN] не могу конвертировать total_asset_value='{value_str}' для {address}")
        return 0.0


def main():
    now = datetime.now(timezone.utc).isoformat()
    rows = []

    for addr in ADDRESSES:
        total = fetch_total_asset_value(addr)
        print(f"{addr}: total_asset_value = {total}")

        rows.append({
            "address": addr,
            "total_usd": total,
            "created_at": now,
        })

    if rows:
        res = supabase.table(SUPABASE_TABLE).insert(rows).execute()
        print("Записано в Supabase:", res)


if __name__ == "__main__":
    main()
