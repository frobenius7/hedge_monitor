import argparse
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
from supabase import create_client, Client
from postgrest.exceptions import APIError

DEBANK_BASE = "https://pro-openapi.debank.com"
ENDPOINT = "/v1/user/all_complex_protocol_list"  # ?id=<address>

def get_env(name: str, required: bool = True, default: Optional[str] = None) -> str:
    val = os.getenv(name, default)
    if required and (val is None or val == ""):
        raise RuntimeError(f"ENV {name} is required")
    return val

def fetch_debank(address: str, api_key: str, max_retries: int = 5, timeout: int = 30) -> List[Dict[str, Any]]:
    url = f"{DEBANK_BASE}{ENDPOINT}"
    params = {"id": address}
    headers = {"Accept": "application/json", "AccessKey": api_key}
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        resp = requests.get(url, params=params, headers=headers, timeout=timeout)
        if resp.status_code == 200:
            return resp.json() or []
        if resp.status_code in (429, 500, 502, 503, 504):
            if attempt == max_retries:
                raise RuntimeError(f"DeBank API failed after {max_retries} retries: {resp.status_code} {resp.text}")
            time.sleep(backoff)
            backoff = min(backoff * 2, 16)
            continue
        raise RuntimeError(f"DeBank API error {resp.status_code}: {resp.text}")

def normalize_rows(address: str, items: List[Dict[str, Any]], fetched_at_iso: str) -> List[Dict[str, Any]]:
    rows = []
    for it in items:
        protocol_id = it.get("id") or it.get("name") or "unknown"
        chain = it.get("chain") or it.get("portfolio_chain") or None
        portfolio_usd = None
        if isinstance(it.get("portfolio_usd_value"), (int, float)):
            portfolio_usd = float(it["portfolio_usd_value"])
        rows.append({
            "address": address.lower(),
            "protocol_id": str(protocol_id),
            "chain": chain,
            "portfolio_usd": portfolio_usd,
            "raw": it,
            "fetched_at": fetched_at_iso,  # ключ снэпшота
        })
    return rows

def write_supabase(sb: Client, table: str, rows: List[Dict[str, Any]], mode: str) -> None:
    """mode: 'append' | 'upsert_snapshot'"""
    if not rows:
        return

    batch_size = 500
    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i+batch_size]
        try:
            if mode == "append":
                # чистый INSERT — ничего не затираем
                sb.table(table).insert(chunk).execute()
            elif mode == "upsert_snapshot":
                # нужен уникальный индекс/ограничение на (address,protocol_id,fetched_at)
                sb.table(table).upsert(
                    chunk,
                    on_conflict="address,protocol_id,fetched_at",
                    ignore_duplicates=False
                ).execute()
            else:
                raise ValueError("Unknown WRITE mode")
        except APIError as e:
            # если остался старый уникальный ключ (address,protocol_id) — подскажем, что делать
            code = getattr(e, "code", None) or (isinstance(e.args and e.args[0], dict) and e.args[0].get("code"))
            details = (e.args and isinstance(e.args[0], dict) and e.args[0].get("details")) or str(e)
            if str(code) == "23505" and "address, protocol_id" in str(details):
                raise RuntimeError(
                    "В таблице всё ещё стоит уникальность по (address,protocol_id) → история не сохраняется.\n"
                    "Решение:\n"
                    "  A) DROP CONSTRAINT debank_protocols_address_protocol_id_key;  (см. SQL ниже)\n"
                    "  B) Либо переключитесь на --mode upsert_snapshot И создайте UNIQUE(address,protocol_id,fetched_at)."
                ) from e
            raise

def main():
    load_dotenv()

    api_key = get_env("DEBANK_API_KEY")
    supabase_url = get_env("SUPABASE_URL")
    supabase_key = get_env("SUPABASE_KEY")
    table = get_env("SUPABASE_TABLE", required=False, default="debank_protocols")

    parser = argparse.ArgumentParser(description="Fetch DeBank data and write into Supabase (history-friendly)")
    parser.add_argument("--wallets", type=str, help="Comma-separated wallet addresses (overrides .env WALLETS)")
    parser.add_argument("--mode", type=str, choices=["append", "upsert_snapshot"],
                        default=os.getenv("WRITE_MODE", "append"),
                        help="append (default) or upsert_snapshot (needs UNIQUE address,protocol_id,fetched_at)")
    args = parser.parse_args()

    wallets = []
    if args.wallets:
        wallets = [w.strip() for w in args.wallets.split(",") if w.strip()]
    else:
        wallets = [w.strip() for w in os.getenv("WALLETS", "").split(",") if w.strip()]
    if not wallets:
        raise RuntimeError("No wallets provided. Use --wallets or set WALLETS in .env")

    sb: Client = create_client(supabase_url, supabase_key)

    # округляем до секунд — один запуск = один timestamp
    fetched_at_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    total_rows = 0
    for addr in wallets:
        print(f"[DeBank] fetching: {addr}")
        items = fetch_debank(addr, api_key)
        rows = normalize_rows(addr, items, fetched_at_iso)
        write_supabase(sb, table, rows, mode=args.mode)
        print(f"[Supabase] {args.mode}: wrote {len(rows)} rows for {addr}")
        total_rows += len(rows)

    print(f"Done. Total rows written: {total_rows}")

if __name__ == "__main__":
    main()
