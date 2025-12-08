import os
import sys
import time
import json
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

BCS_PORTFOLIO_URL = os.getenv("BCS_PORTFOLIO_URL") or "https://be.broker.ru/trade-api-bff-portfolio/api/v1/portfolio"
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE_MOEX") or "moex_bcs_moneylimit_logs"

TOKENS_LIST = [t.strip() for t in (os.getenv("BCS_TOKENS") or "").split(",") if t.strip()]

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: SUPABASE_URL / SUPABASE_KEY not set", file=sys.stderr)
    sys.exit(2)

if not TOKENS_LIST:
    print("ERROR: BCS_TOKENS is empty. Put something like: BCS_TOKENS=BCS_1,BCS_2", file=sys.stderr)
    sys.exit(2)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- HTTP helpers -------------------------------------------------------------

DEFAULT_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "bcs-moneylimit-bot/1.0",
}

def fetch_portfolio(bearer_token: str) -> Any:
    headers = DEFAULT_HEADERS.copy()
    headers["Authorization"] = f"Bearer {bearer_token}"
    # При необходимости можно добавить: Accept-Language, X-App-Platform и т.п.
    resp = requests.get(BCS_PORTFOLIO_URL, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()

# --- Parsing -----------------------------------------------------------------

def is_moneylimit_record(rec: Dict[str, Any]) -> bool:
    """
    Фильтруем только те записи, где:
      - type == MONEYLIMIT
      - term == "T0"
    """
    t = (rec.get("type") or rec.get("upperType") or "").upper()
    term = (rec.get("term") or "").upper()

    return t == "MONEYLIMIT" and term == "T0"


def extract_fields(rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Из записи MONEYLIMIT вытаскиваем требуемые поля.
    Поля по образцу: account, balanceValueRub, balanceValueUsd
    """
    try:
        account = str(rec.get("account") or "")
        bvr = rec.get("balanceValueRub")
        bvu = rec.get("balanceValueUsd")
        if not account:
            return None
        # Приводим к float (Postgres numeric всё равно примет как decimal)
        bvr = float(bvr) if bvr is not None else 0.0
        bvu = float(bvu) if bvu is not None else 0.0
        return {
            "account": account,
            "balance_value_rub": bvr,
            "balance_value_usd": bvu,
            "raw": rec,  # полезно хранить для отладки
        }
    except Exception:
        return None

def flatten_response(payload: Any) -> List[Dict[str, Any]]:
    """
    Ответ у BCS может быть списком на корне или в полях (например, 'items').
    Делаем максимально устойчиво.
    """
    candidates: List[Dict[str, Any]] = []
    if isinstance(payload, list):
        candidates = payload
    elif isinstance(payload, dict):
        # частые варианты
        for key in ["items", "data", "positions", "portfolio", "result"]:
            val = payload.get(key)
            if isinstance(val, list):
                candidates = val
                break
        if not candidates:
            # возможно, сам словарь — это одна запись
            candidates = [payload]
    # фильтруем только MONEYLIMIT
    out: List[Dict[str, Any]] = []
    for rec in candidates:
        if isinstance(rec, dict) and is_moneylimit_record(rec):
            parsed = extract_fields(rec)
            if parsed:
                out.append(parsed)
    return out

# --- Supabase write -----------------------------------------------------------

def insert_rows(rows: List[Dict[str, Any]], source_label: str) -> None:
    if not rows:
        return
    payload = []
    for r in rows:
        payload.append({
            "source": source_label,
            "account": r["account"],
            "balance_value_rub": r["balance_value_rub"],
            "balance_value_usd": r["balance_value_usd"],
            "raw": r["raw"],
        })
    res = supabase.table(SUPABASE_TABLE).insert(payload).execute()
    if getattr(res, "data", None) is None:
        print(f"WARNING: insert returned no data for {source_label}", file=sys.stderr)

# --- Main --------------------------------------------------------------------

def main() -> None:
    for label in TOKENS_LIST:
        bearer = os.getenv(f"{label}_BEARER")
        if not bearer:
            print(f"WARNING: token {label}_BEARER is not set; skipping", file=sys.stderr)
            continue

        try:
            payload = fetch_portfolio(bearer)
            rows = flatten_response(payload)
            if not rows:
                print(f"INFO: no MONEYLIMIT rows for {label}", file=sys.stderr)
            insert_rows(rows, label)
            print(f"OK: {label} -> inserted {len(rows)} row(s)")
        except requests.HTTPError as e:
            print(f"HTTP ERROR for {label}: {e} | body={getattr(e.response, 'text', '')}", file=sys.stderr)
        except Exception as e:
            print(f"ERROR for {label}: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
