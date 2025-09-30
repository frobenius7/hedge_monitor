import argparse
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from collections import deque

import requests
from dotenv import load_dotenv
from supabase import create_client, Client
from postgrest.exceptions import APIError

HL_DEFAULT_URL = "https://api.hyperliquid.xyz/info"

def get_env(name: str, required: bool = True, default: Optional[str] = None) -> str:
    val = os.getenv(name, default)
    if required and (val is None or val == ""):
        raise RuntimeError(f"ENV {name} is required")
    return val

def coerce_number(x) -> Optional[float]:
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        try:
            return float(x.strip())
        except ValueError:
            return None
    return None

def deep_find_first_number(obj: Any, candidate_names: List[str]) -> Tuple[Optional[float], Optional[List[Any]]]:
    """
    BFS по всему JSON. Возвращает (число, путь) — первое найденное поле,
    имя которого совпадает (case-insensitive) с одним из candidate_names,
    и значение которого можно привести к числу.
    """
    wanted = {n.lower() for n in candidate_names}
    q = deque([(obj, [])])
    while q:
        node, path = q.popleft()
        if isinstance(node, dict):
            # сначала проверяем текущий уровень
            for k, v in node.items():
                if k.lower() in wanted:
                    num = coerce_number(v)
                    if num is not None:
                        return num, path + [k]
            # затем углубляемся
            for k, v in node.items():
                if isinstance(v, (dict, list)):
                    q.append((v, path + [k]))
        elif isinstance(node, list):
            for i, v in enumerate(node):
                if isinstance(v, (dict, list)):
                    q.append((v, path + [i]))
    return None, None

def deep_find_positions_count(obj: Any) -> Optional[int]:
    """
    Пытаемся найти список или словарь позиций и вернуть их количество.
    """
    name_candidates = {"assetpositions", "perppositions", "positions", "openpositions"}
    q = deque([obj])
    while q:
        node = q.popleft()
        if isinstance(node, dict):
            for k, v in node.items():
                kl = k.lower()
                if kl in name_candidates:
                    if isinstance(v, list):
                        return len(v)
                    if isinstance(v, dict):
                        return len(v.keys())
                if isinstance(v, (dict, list)):
                    q.append(v)
        elif isinstance(node, list):
            for v in node:
                if isinstance(v, (dict, list)):
                    q.append(v)
    return None

def fetch_hl_state(address: str, api_url: str, timeout: int = 30, max_retries: int = 5) -> Dict[str, Any]:
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    payload = {"type": "clearinghouseState", "user": address}
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        resp = requests.post(api_url, headers=headers, data=json.dumps(payload), timeout=timeout)
        if resp.status_code == 200:
            try:
                return resp.json()
            except Exception as e:
                raise RuntimeError(f"Hyperliquid JSON parse error: {e}; body={resp.text[:500]}")
        if resp.status_code in (429, 500, 502, 503, 504):
            if attempt == max_retries:
                raise RuntimeError(f"Hyperliquid API failed after {max_retries} retries: {resp.status_code} {resp.text}")
            time.sleep(backoff)
            backoff = min(backoff * 2, 16)
            continue
        raise RuntimeError(f"Hyperliquid API error {resp.status_code}: {resp.text}")

def try_extract_metrics(raw_in: Any, key_hint: Optional[str] = None) -> Tuple[Optional[float], Optional[int], Optional[List[Any]]]:
    """
    Возвращает (equity_usd, positions_count, equity_path).
    - key_hint: опциональный путь (через точки), например: "data.userAccountSummary.marginSummary.equity"
    """
    raw = raw_in
    # Если ответ завернут в "data"/"result", разворачиваем во время поиска — deep_find и так обойдет всё,
    # но hint может быть короче:
    wrappers = ["data", "result", "response"]
    for w in wrappers:
        if isinstance(raw, dict) and w in raw and isinstance(raw[w], (dict, list)):
            # не заменяем raw, deep_find сам обойдет; просто оставляем как есть
            pass

    # 1) hint-путь, если задан
    path_used = None
    if key_hint:
        node = raw
        path = []
        for seg in key_hint.split("."):
            if isinstance(node, dict) and seg in node:
                node = node[seg]; path.append(seg)
            else:
                node = None; path = None; break
        if path and node is not None:
            num = coerce_number(node)
            if num is not None:
                equity = num
                pos_cnt = deep_find_positions_count(raw)
                return equity, pos_cnt, path

    # 2) без хинта — глубокий поиск по типичным именам
    equity_keys = ["accountValue", "equity", "equityUsd", "equity_usd", "account_value", "netLiq", "net_liq"]
    equity, path_used = deep_find_first_number(raw, equity_keys)
    positions_count = deep_find_positions_count(raw)
    return equity, positions_count, path_used

def write_supabase(sb: Client, table: str, rows: List[Dict[str, Any]], mode: str) -> None:
    if not rows: return
    for i in range(0, len(rows), 500):
        chunk = rows[i:i+500]
        try:
            if mode == "append":
                sb.table(table).insert(chunk).execute()
            elif mode == "upsert_snapshot":
                sb.table(table).upsert(
                    chunk, on_conflict="address,snapshot_type,fetched_at", ignore_duplicates=False
                ).execute()
            else:
                raise ValueError("Unknown mode")
        except APIError as e:
            details = (isinstance(e.args[0], dict) and e.args[0].get("details")) or str(e)
            code = (isinstance(e.args[0], dict) and e.args[0].get("code")) or ""
            if str(code) == "23505" and "address, snapshot_type" in str(details):
                raise RuntimeError(
                    "В таблице ещё стоит уникальность по (address, snapshot_type) → история не сохраняется.\n"
                    "Снимите ограничение или включите upsert по (address,snapshot_type,fetched_at)."
                ) from e
            raise

def main():
    load_dotenv()
    supabase_url = get_env("SUPABASE_URL")
    supabase_key = get_env("SUPABASE_KEY")
    table = get_env("SUPABASE_TABLE_HL", required=False, default="hyperliquid_state")
    api_url = get_env("HL_API_URL", required=False, default=HL_DEFAULT_URL)

    parser = argparse.ArgumentParser(description="Fetch Hyperliquid clearinghouseState and write history to Supabase")
    parser.add_argument("--wallets", type=str, help="Comma-separated wallet addresses (overrides .env WALLETS_HL or WALLETS)")
    parser.add_argument("--mode", type=str, choices=["append","upsert_snapshot"], default=os.getenv("WRITE_MODE","append"))
    parser.add_argument("--equity-path", type=str, default=os.getenv("HL_EQUITY_PATH", ""),
                        help="Optional dot-path for equity inside JSON, e.g. data.userAccountSummary.marginSummary.equity")
    parser.add_argument("--verbose", action="store_true", help="Print where equity was found")
    args = parser.parse_args()

    wallets = []
    if args.wallets:
        wallets = [w.strip() for w in args.wallets.split(",") if w.strip()]
    else:
        wallets = [w.strip() for w in os.getenv("WALLETS_HL", os.getenv("WALLETS","")).split(",") if w.strip()]
    if not wallets:
        raise RuntimeError("No wallets provided. Use --wallets or set WALLETS_HL / WALLETS in .env")

    sb: Client = create_client(supabase_url, supabase_key)
    fetched_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()  # один запуск = один timestamp

    total = 0
    for addr in wallets:
        print(f"[HL] fetching clearinghouseState for {addr}")
        raw = fetch_hl_state(addr, api_url=api_url)
        equity_usd, positions_count, path_used = try_extract_metrics(raw, key_hint=args.equity_path or None)
        if args.verbose:
            print(f"[HL] equity={equity_usd} path={path_used} positions={positions_count}")

        row = {
            "address": addr.lower(),
            "snapshot_type": "clearinghouseState",
            "equity_usd": equity_usd,
            "positions_count": positions_count,
            "raw": raw,
            "fetched_at": fetched_at,
        }
        write_supabase(sb, table, [row], mode=args.mode)
        print(f"[Supabase] {args.mode}: wrote snapshot for {addr} (equity={equity_usd}, positions={positions_count})")
        total += 1

    print(f"Done. Inserted/Upserted: {total} rows")

if __name__ == "__main__":
    main()
