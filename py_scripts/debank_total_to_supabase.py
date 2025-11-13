import os
import time
import logging
from decimal import Decimal, getcontext
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv
from supabase import create_client, Client

# -------- numeric precision & logging ----------
getcontext().prec = 50
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("debank_total_to_supabase")

# -------- env ----------
load_dotenv()

DEBANK_API_KEY = os.getenv("DEBANK_API_KEY")
SUPABASE_URL   = os.getenv("SUPABASE_URL")
SUPABASE_KEY   = os.getenv("SUPABASE_KEY")
ADDRESSES      = [a.strip() for a in (os.getenv("ADDRESSES") or "").split(",") if a.strip()]
QPS            = float(os.getenv("QPS") or "1.0")
SLEEP_BETWEEN  = 1.0 / max(QPS, 0.0001)

if not DEBANK_API_KEY:
    raise RuntimeError("DEBANK_API_KEY is not set")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL or SUPABASE_KEY is not set")
if not ADDRESSES:
    raise RuntimeError("ADDRESSES is empty")

# -------- clients ----------
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

DEBANK_BASE = "https://pro-openapi.debank.com/v1"
HEADERS = {
    "accept": "application/json",
    #"X-API-Key": DEBANK_API_KEY,          # основной заголовок для Pro OpenAPI
    "AccessKey": DEBANK_API_KEY,        # можно оставить закомментированным на случай совместимости
}

# -------- http with retries ----------
def http_get(url: str, params: Dict[str, Any] = None, max_retries: int = 5) -> Any:
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=30)
            if r.status_code == 429:
                wait = min(30.0, backoff)
                log.warning("429 Too Many Requests. Sleeping %.1fs (attempt %d/%d)", wait, attempt, max_retries)
                time.sleep(wait)
                backoff *= 2
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            if attempt == max_retries:
                log.error("GET %s failed after %d attempts: %s", url, attempt, e)
                raise
            wait = min(30.0, backoff)
            log.warning("GET error: %s. Retry %d/%d after %.1fs", str(e), attempt, max_retries, wait)
            time.sleep(wait)
            backoff *= 2

# -------- parsing helpers ----------
def _to_decimal(x: Any) -> Decimal:
    try:
        return Decimal(str(x))
    except Exception:
        return Decimal(0)

def parse_total_usd(payload: Any) -> Decimal:
    """
    DeBank /v1/user/total_balance может возвращать число или объект.
    Поддержим несколько распространённых ключей ради надёжности.
    """
    if payload is None:
        return Decimal(0)
    if isinstance(payload, (int, float, str)):
        return _to_decimal(payload)
    if isinstance(payload, dict):
        for key in ("total_usd_value", "total_usd", "usd_value", "total_value", "value", "total_balance"):
            if key in payload:
                return _to_decimal(payload[key])
    # если формат неожиданный
    log.warning("Unexpected total_balance payload format: %s", str(payload)[:200])
    return Decimal(0)

# -------- debank call ----------
def fetch_total_balance_usd(address: str) -> Decimal:
    url = f"{DEBANK_BASE}/user/total_balance"
    data = http_get(url, params={"id": address})
    return parse_total_usd(data)

# -------- db write ----------
def insert_snapshot(address: str, as_of_iso: str, total_usd: Decimal):
    payload = {
        "address": address.lower(),
        "as_of": as_of_iso,
        "total_usd": str(total_usd),
        "source": "DeBank OpenAPI",
    }
    res = supabase.table("wallet_total_balance").insert(payload).execute()
    # supabase-py v2 doesn't throw exceptions on errors by default; проверим поле error, если есть
    if getattr(res, "error", None):
        raise RuntimeError(f"Supabase insert error: {res.error}")

# -------- main ----------
def process_address(address: str):
    log.info("Processing %s", address)
    as_of = datetime.now(timezone.utc).isoformat()
    total_usd = fetch_total_balance_usd(address)
    log.info("Total balance (USD) for %s: %s", address, total_usd)
    insert_snapshot(address, as_of, total_usd)

def main():
    log.info("Start. %d addresses, QPS=%.2f", len(ADDRESSES), QPS)
    for i, addr in enumerate(ADDRESSES, 1):
        try:
            process_address(addr)
        except Exception as e:
            log.exception("Failed %s: %s", addr, e)
        if i < len(ADDRESSES):
            time.sleep(SLEEP_BETWEEN)
    log.info("Done.")

if __name__ == "__main__":
    main()
