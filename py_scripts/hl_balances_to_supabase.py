import os
import json
from decimal import Decimal
from datetime import datetime, timezone
from typing import Tuple, List

import requests
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE_HL2", "hl_balance_logs")
HL_ADDRESSES = [a.strip() for a in (os.getenv("WALLETS_HL") or "").split(",") if a.strip()]

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL / SUPABASE_KEY не заданы в .env")

if not HL_ADDRESSES:
    raise RuntimeError("Список адресов пуст. Заполните WALLETS_HL в .env")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

HL_INFO_URL = "https://api.hyperliquid.xyz/info"


def fetch_latest_hl_balance(address: str) -> Tuple[int, Decimal]:
    """
    Возвращает (hl_timestamp_ms, balance_usd) по САМОМУ ПОЗДНЕМУ значению accountValueHistory
    из ответа type="portfolio". Обрабатывает top-level list и пустые ответы.
    """
    payload = {"type": "portfolio", "user": address}
    r = requests.post(HL_INFO_URL, json=payload, timeout=25)
    r.raise_for_status()
    data = r.json()

    # Если сервер вернул пустой объект — это часто адрес API-агента, а не торговый адрес
    if data == {}:
        raise ValueError(
            "Empty portfolio for this address. "
            "Проверьте, что это именно trading address (master/sub-account), а не API-agent."
        )

    # Ответ — массив пар: [period, { accountValueHistory: [...] }, ...]
    histories: List[Tuple[int, Decimal]] = []

    def collect_from_period_obj(obj):
        avh = obj.get("accountValueHistory")
        if isinstance(avh, list):
            for item in avh:
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    ts = int(item[0])
                    val = Decimal(str(item[1]))
                    histories.append((ts, val))

    if isinstance(data, list):
        # Каждая запись — [ "day" | "week" | ... , { ... } ]
        for pair in data:
            if isinstance(pair, (list, tuple)) and len(pair) == 2 and isinstance(pair[1], dict):
                collect_from_period_obj(pair[1])
    elif isinstance(data, dict):
        # На всякий случай: если вдруг вернули dict по периодам
        for v in data.values():
            if isinstance(v, dict):
                collect_from_period_obj(v)

    if not histories:
        # Ничего не нашли — дадим диагностическое сообщение
        raise ValueError(
            "No accountValueHistory found in portfolio response. "
            "Убедитесь, что адрес корректный и имеет историю. "
            f"Sample type={type(data).__name__}"
        )

    # Берём глобально самый поздний таймстемп
    latest_ts, latest_val = max(histories, key=lambda p: p[0])
    return latest_ts, latest_val



def upsert_row(address: str, ts_ms: int, balance_usd: Decimal) -> None:
    row = {
        "address": address.lower(),
        "hl_timestamp": ts_ms,
        "hl_datetime": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat(),
        "balance_usd": float(balance_usd),  # supabase python клиент корректно проглотит float
        "written_at": datetime.now(timezone.utc).isoformat(),
    }
    # on_conflict по уникальному индексу (address, hl_timestamp)
    res = supabase.table(SUPABASE_TABLE).upsert(row, on_conflict="address,hl_timestamp").execute()
    if getattr(res, "error", None):
        raise RuntimeError(res.error)
    # Можно распечатать краткий лог:
    print(f"[OK] {address.lower()} ts={ts_ms} bal={balance_usd}")


def main():
    errors: List[str] = []
    for addr in HL_ADDRESSES:
        try:
            ts_ms, bal = fetch_latest_hl_balance(addr)
            upsert_row(addr, ts_ms, bal)
        except Exception as e:
            msg = f"[ERR] {addr}: {e}"
            print(msg)
            errors.append(msg)

    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
