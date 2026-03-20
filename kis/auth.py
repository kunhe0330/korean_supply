"""
KIS API 인증 — OAuth 토큰 발급 및 자동 갱신
토큰은 발급 후 약 24시간 유효. 만료 1시간 전 자동 갱신.
"""

import time
import logging
from datetime import datetime, timedelta
from threading import Lock

import requests

from config import KIS_BASE_URL, KIS_APP_KEY, KIS_APP_SECRET

logger = logging.getLogger(__name__)

_token_lock = Lock()
_access_token: str = ""
_token_expires_at: datetime = datetime.min


def _issue_token() -> tuple[str, datetime]:
    """토큰 발급 API 호출."""
    url = f"{KIS_BASE_URL}/oauth2/tokenP"
    body = {
        "grant_type": "client_credentials",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
    }
    resp = requests.post(url, json=body, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    token = data["access_token"]
    # expires_in은 초 단위 (보통 86400 = 24h)
    expires_in = int(data.get("expires_in", 86400))
    expires_at = datetime.now() + timedelta(seconds=expires_in - 3600)  # 1시간 여유
    logger.info("KIS 토큰 발급 완료, 만료: %s", expires_at.isoformat())
    return token, expires_at


def get_access_token() -> str:
    """유효한 액세스 토큰 반환. 만료 임박 시 자동 갱신."""
    global _access_token, _token_expires_at
    with _token_lock:
        if not _access_token or datetime.now() >= _token_expires_at:
            for attempt in range(3):
                try:
                    _access_token, _token_expires_at = _issue_token()
                    break
                except Exception as e:
                    logger.warning("토큰 발급 실패 (%d/3): %s", attempt + 1, e)
                    if attempt < 2:
                        time.sleep(2)
                    else:
                        raise
        return _access_token


def get_auth_headers(tr_id: str) -> dict:
    """KIS API 공통 인증 헤더."""
    return {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {get_access_token()}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": tr_id,
    }
