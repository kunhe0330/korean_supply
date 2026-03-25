"""
KIS API 인증 — OAuth 토큰 발급 및 파일 캐시
토큰은 발급 후 약 24시간 유효.
파일 캐시로 프로세스 재시작/배포 시에도 유효한 토큰 재사용.
1일 1회 발급 원칙 준수.
"""

import json
import os
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

# 토큰 캐시 파일 경로 (Railway 볼륨 마운트 경로)
_db_path = os.environ.get("DB_PATH", os.path.join("storage", "supply.db"))
_TOKEN_CACHE_DIR = os.path.dirname(_db_path) or "storage"
_TOKEN_CACHE_PATH = os.path.join(_TOKEN_CACHE_DIR, ".kis_token_cache.json")


def _save_token_cache(token: str, expires_at: datetime) -> None:
    """토큰을 파일에 캐시."""
    try:
        os.makedirs(os.path.dirname(_TOKEN_CACHE_PATH), exist_ok=True)
        cache = {
            "access_token": token,
            "expires_at": expires_at.isoformat(),
        }
        with open(_TOKEN_CACHE_PATH, "w") as f:
            json.dump(cache, f)
        logger.info("토큰 캐시 저장: %s", _TOKEN_CACHE_PATH)
    except Exception as e:
        logger.warning("토큰 캐시 저장 실패: %s", e)


def _load_token_cache() -> tuple[str, datetime] | None:
    """캐시된 토큰 로드. 유효하면 (token, expires_at) 반환, 없거나 만료면 None."""
    try:
        if not os.path.exists(_TOKEN_CACHE_PATH):
            return None
        with open(_TOKEN_CACHE_PATH, "r") as f:
            cache = json.load(f)
        token = cache["access_token"]
        expires_at = datetime.fromisoformat(cache["expires_at"])
        if datetime.now() >= expires_at:
            logger.info("캐시 토큰 만료됨, 재발급 필요")
            return None
        logger.info("캐시 토큰 로드 성공, 만료: %s", expires_at.isoformat())
        return token, expires_at
    except Exception as e:
        logger.warning("토큰 캐시 로드 실패: %s", e)
        return None


def _issue_token() -> tuple[str, datetime]:
    """토큰 발급 API 호출 (1일 1회만 호출되어야 함)."""
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
    # 만료 1시간 전에 갱신하도록 여유 설정
    expires_at = datetime.now() + timedelta(seconds=expires_in - 3600)
    logger.info("KIS 토큰 신규 발급, 만료: %s", expires_at.isoformat())
    # 파일 캐시에 저장
    _save_token_cache(token, expires_at)
    return token, expires_at


def get_access_token() -> str:
    """유효한 액세스 토큰 반환. 캐시 우선, 만료 시에만 신규 발급."""
    global _access_token, _token_expires_at
    with _token_lock:
        # 1) 메모리에 유효한 토큰이 있으면 바로 반환
        if _access_token and datetime.now() < _token_expires_at:
            return _access_token

        # 2) 파일 캐시에서 로드 시도
        cached = _load_token_cache()
        if cached:
            _access_token, _token_expires_at = cached
            return _access_token

        # 3) 캐시도 없거나 만료 → 신규 발급 (1일 1회)
        logger.info("유효한 토큰 없음, 신규 발급 시작")
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
