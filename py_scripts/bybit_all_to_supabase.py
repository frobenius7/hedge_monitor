# bybit_to_supabase_full_usd.py  (Python 3.9)
# -------------------------------------------------------------------
# .env пример:
# BYBIT_API_KEY=xxx
# BYBIT_API_SECRET=yyy
# BYBIT_ACCOUNT_TYPE=UNIFIED
# BYBIT_TESTNET=false
# SAVE_EMPTY_POSITIONS=false
# BYBIT_LINEAR_SETTLE=USDT            # напр.: USDT или USDT,USDC
# BYBIT_INVERSE_SETTLE=               # напр.: BTC,ETH  (пусто = пропускаем inverse)
# SUPABASE_URL=https://YOUR-PROJECT.ref.supabase.co
# SUPABASE_KEY=YOUR_SERVICE_OR_ANON_KEY
# SUPABASE_TABLE_bb=bybit_balance_logs
# SUPABASE_TABLE_bbPOS=bybit_positions_logs
#
# SQL (один раз):
# ALTER TABLE public.bybit_balance_logs
#   ADD COLUMN IF NOT EXISTS total_equity_usd_all numeric,
#   ADD COLUMN IF NOT EXISTS per_coin_usd jsonb;

import os
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, getcontext
from dotenv import load_dotenv
from supabase import create_client, Client
from pybit.unified_trading import HTTP  # pybit v5

getcontext().prec = 28
load_dotenv()

# ---- Bybit env ----
BYBIT_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_SECRET = os.getenv("BYBIT_API_SECRET")
BYBIT_KEY2 = os.getenv("BYBIT_API_KEY2")
BYBIT_SECRET2 = os.getenv("BYBIT_API_SECRET2")
BYBIT_KEY3 = os.getenv("BYBIT_API_KEY3")
BYBIT_SECRET3 = os.getenv("BYBIT_API_SECRET3")
ACCOUNT_TYPE = (os.getenv("BYBIT_ACCOUNT_TYPE") or "UNIFIED").upper()
TESTNET = (os.getenv("BYBIT_TESTNET") or "false").lower() == "true"
SAVE_EMPTY_POSITIONS = (os.getenv("SAVE_EMPTY_POSITIONS") or "false").lower() == "true"

# settleCoin списки для позиций
LINEAR_SETTLES = [s.strip().upper() for s in (os.getenv("BYBIT_LINEAR_SETTLE") or "USDT").split(",") if s.strip()]
INVERSE_SETTLES = [s.strip().upper() for s in (os.getenv("BYBIT_INVERSE_SETTLE") or "").split(",") if s.strip()]

# ---- Supabase env ----
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TAB_BAL = os.getenv("SUPABASE_TABLE_bb") or "bybit_balance_logs"
TAB_POS = os.getenv("SUPABASE_TABLE_bbPOS") or "bybit_positions_logs"


# ===================== UTILS =====================
def sb_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("SUPABASE_URL / SUPABASE_KEY не заданы")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def to_dec(x: Any) -> Decimal:
    try:
        return Decimal(str(x)) if x not in (None, "", "null") else Decimal("0")
    except (InvalidOperation, ValueError):
        return Decimal("0")

def pybit_session(acc_id: int = 1) -> HTTP:
    if acc_id == 1:
        if not BYBIT_KEY or not BYBIT_SECRET:
            raise RuntimeError("BYBIT_API_KEY / BYBIT_API_SECRET не заданы")
        return HTTP(api_key=BYBIT_KEY, api_secret=BYBIT_SECRET, testnet=TESTNET)
    elif acc_id == 2:
        if not BYBIT_KEY2 or not BYBIT_SECRET2:
            raise RuntimeError("BYBIT_API_KEY2 / BYBIT_API_SECRET2 не заданы")
        return HTTP(api_key=BYBIT_KEY2, api_secret=BYBIT_SECRET2, testnet=TESTNET)
    elif acc_id == 3:
        if not BYBIT_KEY3 or not BYBIT_SECRET3:
            raise RuntimeError("BYBIT_API_KEY3 / BYBIT_API_SECRET3 не заданы")
        return HTTP(api_key=BYBIT_KEY3, api_secret=BYBIT_SECRET3, testnet=TESTNET)

# ===================== UID =====================
def get_uid(session: HTTP) -> Optional[str]:
    """UID владельца текущего API-ключа через /v5/user/query-api."""
    try:
        info = session.get_api_key_information()
        res = info.get("result") or {}
        uid = res.get("userID") or res.get("userId") or res.get("uid")
        uid = str(uid).strip() if uid is not None else None
        if not uid:
            print("[warn] get_api_key_information: UID не найден в ответе:", info)
        return uid
    except Exception as e:
        print("[warn] query-api failed:", e)
        return None


# ===================== БАЛАНС =====================
def get_wallet_balance(session: HTTP, account_type: str = "UNIFIED") -> Dict[str, Any]:
    resp = session.get_wallet_balance(accountType=account_type)
    if not isinstance(resp, dict) or "result" not in resp:
        raise RuntimeError("Неожиданный ответ Bybit (balance): %r" % resp)
    return resp

def parse_balance(bybit_resp: Dict[str, Any]):
    """
    Возвращает:
      total_equity_usdt (Decimal) — только монета USDT,
      total_available_usdt (Decimal),
      coins_json = {"COIN":{"equity":"...","available":"..."}},
      per_coin_wallet_usd = {"COIN":"..."} (usdValue из кошелька, если есть)
    """
    result = bybit_resp.get("result") or {}
    items = result.get("list") or []
    coins_breakdown: Dict[str, Dict[str, Decimal]] = {}
    per_coin_wallet_usd: Dict[str, str] = {}

    for acc in items:
        for c in (acc.get("coin") or []):
            sym = c.get("coin")
            equity = to_dec(c.get("equity"))
            avail = to_dec(
                c.get("availableToWithdraw")
                or c.get("availableBalance")
                or c.get("walletBalance")
            )
            if not sym:
                continue
            if sym not in coins_breakdown:
                coins_breakdown[sym] = {"equity": Decimal("0"), "available": Decimal("0")}
            coins_breakdown[sym]["equity"] += equity
            coins_breakdown[sym]["available"] += avail

            if c.get("usdValue") is not None:
                try:
                    per_coin_wallet_usd[sym] = str(to_dec(c.get("usdValue")))
                except Exception:
                    pass

    usdt_equity = coins_breakdown.get("USDT", {}).get("equity", Decimal("0"))
    usdt_available = coins_breakdown.get("USDT", {}).get("available", Decimal("0"))

    coins_json = {k: {"equity": str(v["equity"]), "available": str(v["available"])} for k, v in coins_breakdown.items()}
    return usdt_equity, usdt_available, coins_json, per_coin_wallet_usd


# ===================== ОЦЕНКА В USD =====================
def get_spot_tickers_for_base(session: HTTP, base_coin: str) -> List[Dict[str, Any]]:
    data = session.get_tickers(category="spot", baseCoin=base_coin)
    result = data.get("result") or {}
    return result.get("list") or []

def get_linear_tickers_for_symbol(session: HTTP, symbol_like: str) -> List[Dict[str, Any]]:
    data = session.get_tickers(category="linear", symbol=symbol_like)
    result = data.get("result") or {}
    return result.get("list") or []

def pick_usd_price_from_tickers(tickers: List[Dict[str, Any]]) -> Optional[Decimal]:
    best = None
    for t in tickers:
        symbol = (t.get("symbol") or "").upper()
        last = to_dec(t.get("lastPrice"))
        if symbol.endswith("USDT"):
            return last
        if symbol.endswith("USDC"):
            best = best or last
    return best

def get_prices_usd(session: HTTP, coins: List[str]) -> Dict[str, Decimal]:
    prices: Dict[str, Decimal] = {}
    for c in coins:
        u = c.upper()
        if u in ("USDT", "USDC"):
            prices[u] = Decimal("1")
            continue
        try:
            px = pick_usd_price_from_tickers(get_spot_tickers_for_base(session, u))
            if px is None:
                px = pick_usd_price_from_tickers(get_linear_tickers_for_symbol(session, u + "USDT"))
            prices[u] = px if px is not None else Decimal("0")
        except Exception as e:
            print("[warn] price fetch failed for %s: %s" % (u, e))
            prices[u] = Decimal("0")
    return prices

def compute_total_usd(coins_json: Dict[str, Dict[str, str]],
                      per_coin_wallet_usd: Dict[str, str],
                      prices_usd: Dict[str, Decimal]):
    """
    Приоритет источников:
      1) usdValue из кошелька (если есть)
      2) equity * price из тикера (spot/linear)
    """
    total = Decimal("0")
    per_coin_usd: Dict[str, Dict[str, str]] = {}

    for coin, vals in coins_json.items():
        eq = to_dec(vals.get("equity"))
        if coin in per_coin_wallet_usd:
            usd_val = to_dec(per_coin_wallet_usd[coin])
            px = (usd_val / eq) if eq > 0 else Decimal("0")
        else:
            px = prices_usd.get(coin.upper(), Decimal("0"))
            usd_val = eq * px

        total += usd_val
        per_coin_usd[coin] = {"price": str(px), "equity_usd": str(usd_val)}
    return total, per_coin_usd


# ===================== INSERTS =====================
def insert_balance(
    sb: Client,
    uid: Optional[str],
    account_type: str,
    total_eq_usdt: Decimal,
    total_av_usdt: Decimal,
    coins_json: Dict[str, Dict[str, str]],
    raw: Dict[str, Any],
    total_equity_usd_all: Optional[Decimal] = None,
    per_coin_usd: Optional[Dict[str, Dict[str, str]]] = None,
):
    row = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "uid": uid,
        "account_type": account_type,
        "total_equity_usdt": str(total_eq_usdt),
        "total_available_usdt": str(total_av_usdt),
        "coins": coins_json,
        "raw": raw,
    }
    if total_equity_usd_all is not None:
        row["total_equity_usd_all"] = str(total_equity_usd_all)
    if per_coin_usd is not None:
        row["per_coin_usd"] = per_coin_usd
    return sb.table(TAB_BAL).insert(row).execute()


# ===================== ПОЗИЦИИ =====================
def get_positions(session: HTTP, category: str, **params) -> Dict[str, Any]:
    """category: 'linear' | 'inverse' | 'option' — нужен symbol или settleCoin/baseCoin."""
    resp = session.get_positions(category=category, **params)
    if not isinstance(resp, dict) or "result" not in resp:
        raise RuntimeError("Неожиданный ответ Bybit (positions, %s): %r" % (category, resp))
    return resp

def flatten_position_item(item: Dict[str, Any], uid: Optional[str], category: str) -> Dict[str, Any]:
    pos_idx = str(item.get("positionIdx", "")) if item.get("positionIdx") is not None else ""
    margin_mode = "Isolated" if pos_idx in ("1", "3") else (item.get("tradeMode") or item.get("marginMode") or "Cross")
    return {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "uid": uid,
        "category": category,
        "symbol": item.get("symbol"),
        "side": item.get("side"),
        "size": str(to_dec(item.get("size"))),
        "leverage": str(to_dec(item.get("leverage"))),
        "avg_entry_price": str(to_dec(item.get("avgPrice") or item.get("entryPrice"))),
        "mark_price": str(to_dec(item.get("markPrice"))),
        "liq_price": str(to_dec(item.get("liqPrice"))),
        "position_value": str(to_dec(item.get("positionValue"))),
        "unrealized_pnl": str(to_dec(item.get("unrealisedPnl") or item.get("unrealizedPnl"))),
        "cum_realised_pnl": str(to_dec(item.get("cumRealisedPnl") or item.get("cumRealizedPnl"))),
        "risk_limit_value": str(to_dec(item.get("riskLimitValue"))),
        "margin_mode": margin_mode,
        "position_status": item.get("positionStatus") or item.get("positionStatusVo"),
        "take_profit": str(to_dec(item.get("takeProfit"))),
        "stop_loss": str(to_dec(item.get("stopLoss"))),
        "raw": item,
    }

def insert_positions(sb: Client, rows: List[Dict[str, Any]]):
    if not rows:
        return None
    return sb.table(TAB_POS).insert(rows).execute()


def run_for_profile(acc_id: int = 1):
    print (acc_id)

    sb = sb_client()
    session = pybit_session(acc_id)

    # 0) UID
    uid = get_uid(session)
    print("[Bybit] UID:", uid)

    # 1) Баланс -> цены -> USD
    bal_raw = get_wallet_balance(session, ACCOUNT_TYPE)
    total_equity_usdt, total_available_usdt, coins_json, per_coin_wallet_usd = parse_balance(bal_raw)

    prices = get_prices_usd(session, list(coins_json.keys()))
    total_usd_all, per_coin_usd = compute_total_usd(coins_json, per_coin_wallet_usd, prices)

    insert_balance(
        sb,
        uid,
        ACCOUNT_TYPE,
        total_equity_usdt,
        total_available_usdt,
        coins_json,
        bal_raw,
        total_equity_usd_all=total_usd_all,
        per_coin_usd=per_coin_usd,
    )
    print("[Supabase] balance inserted: uid=%s, total_usd=%s" % (uid, total_usd_all))

    # 2) Позиции (linear + inverse через settleCoin)
    all_rows: List[Dict[str, Any]] = []

    for settle in LINEAR_SETTLES:
        try:
            pos_raw = get_positions(session, category="linear", settleCoin=settle)
        except Exception as e:
            print("[warn] positions fetch failed (linear, settle=%s): %s" % (settle, e))
            continue
        result = pos_raw.get("result") or {}
        for item in (result.get("list") or []):
            size_dec = to_dec(item.get("size"))
            if not SAVE_EMPTY_POSITIONS and size_dec == 0:
                continue
            all_rows.append(flatten_position_item(item, uid=uid, category="linear"))

    for settle in INVERSE_SETTLES:
        try:
            pos_raw = get_positions(session, category="inverse", settleCoin=settle)
        except Exception as e:
            print("[warn] positions fetch failed (inverse, settle=%s): %s" % (settle, e))
            continue
        result = pos_raw.get("result") or {}
        for item in (result.get("list") or []):
            size_dec = to_dec(item.get("size"))
            if not SAVE_EMPTY_POSITIONS and size_dec == 0:
                continue
            all_rows.append(flatten_position_item(item, uid=uid, category="inverse"))

    if all_rows:
        insert_positions(sb, all_rows)
        print("[Supabase] positions inserted: %d rows" % len(all_rows))
    else:
        print("[Supabase] positions: nothing to insert")    

# ===================== MAIN =====================
def main():
    print("[Bybit] accountType=%s | testnet=%s" % (ACCOUNT_TYPE, TESTNET))

    run_for_profile(1);

    run_for_profile(2);

    run_for_profile(3);

    print("[done]")

if __name__ == "__main__":
    main()
