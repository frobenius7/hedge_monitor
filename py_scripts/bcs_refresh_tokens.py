import os
import sys
import time
from typing import Dict, Optional

import requests
from dotenv import load_dotenv, set_key

ENV_PATH = os.path.join(os.getcwd(), ".env")

def env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if v in ("1", "true", "yes", "y", "on"): return True
    if v in ("0", "false", "no", "n", "off"): return False
    return default

def load_env() -> None:
    if not os.path.exists(ENV_PATH):
        print(f"INFO: .env not found at {ENV_PATH}, will create on first update")
    load_dotenv(ENV_PATH, override=True)

def post_token(url: str, client_id: str, refresh_token: str, extra_key: Optional[str] = None) -> Dict:
    """
    Запрос нового access_token по refresh_token (Keycloak).
    """
    data = {
        "client_id": client_id,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    # Если нужен параметр Key (иногда обязателен) — добавим
    if extra_key:
        data["Key"] = extra_key

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "bcs-refresh-bot/1.0",
    }

    # Небольшой ретрай
    last_err = None
    for attempt in range(1, 4):
        try:
            resp = requests.post(url, data=data, headers=headers, timeout=30)
            if resp.status_code >= 400:
                raise requests.HTTPError(f"{resp.status_code} {resp.text}")
            return resp.json()
        except Exception as e:
            last_err = e
            print(f"WARNING: attempt {attempt} failed: {e}", file=sys.stderr)
            time.sleep(1.5 * attempt)
    raise RuntimeError(f"Failed to refresh token after retries: {last_err}")

def main() -> None:
    load_env()

    tokens_list = [t.strip() for t in (os.getenv("BCS_TOKENS") or "").split(",") if t.strip()]
    if not tokens_list:
        print("ERROR: BCS_TOKENS is empty. Put e.g. BCS_TOKENS=BCS_1,BCS_2", file=sys.stderr)
        sys.exit(2)

    url = os.getenv("BCS_TOKEN_URL") or "https://be.broker.ru/trade-api-keycloak/realms/tradeapi/protocol/openid-connect/token"
    client_id = os.getenv("BCS_CLIENT_ID") or "trade-api-read"
    extra_key = (os.getenv("BCS_EXTRA_KEY") or "").strip() or None
    update_refresh = env_bool("UPDATE_REFRESH", True)

    updated = 0
    for label in tokens_list:
        refresh_env_name = f"{label}_REFRESH"
        bearer_env_name  = f"{label}_BEARER"

        refresh_token = os.getenv(refresh_env_name)
        if not refresh_token:
            print(f"WARNING: no {refresh_env_name} in .env; skipping {label}", file=sys.stderr)
            continue

        try:
            payload = post_token(url=url, client_id=client_id, refresh_token=refresh_token, extra_key=extra_key)
        except Exception as e:
            print(f"ERROR: {label} refresh failed: {e}", file=sys.stderr)
            continue

        access = payload.get("access_token")
        new_refresh = payload.get("refresh_token")  # Keycloak часто отдаёт новый refresh

        if not access:
            print(f"ERROR: {label} no access_token in response: {payload}", file=sys.stderr)
            continue

        # Обновляем/создаём в .env
        set_key(ENV_PATH, bearer_env_name, access, quote_mode="never")
        if update_refresh and new_refresh:
            set_key(ENV_PATH, refresh_env_name, new_refresh, quote_mode="never")

        updated += 1
        print(f"OK: {label} updated {bearer_env_name}" + (" and refresh" if (update_refresh and new_refresh) else ""))

    if updated == 0:
        sys.exit(1)

if __name__ == "__main__":
    main()
