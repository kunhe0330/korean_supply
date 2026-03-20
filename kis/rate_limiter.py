"""
KIS API Rate Limiter — 초당 호출 제한 관리
"""

import time as time_mod
from datetime import datetime, time
from threading import Lock

from config import API_RATE_LIMIT

_lock = Lock()
_last_call_time = 0.0


def get_sleep_time() -> float:
    """15:30~16:30 피크 타임에는 sleep 늘려서 안정성 확보."""
    now = datetime.now()
    if API_RATE_LIMIT["peak_start"] <= now.time() <= API_RATE_LIMIT["peak_end"]:
        return API_RATE_LIMIT["sleep_peak"]
    return API_RATE_LIMIT["sleep_default"]


def rate_limit():
    """API 호출 전 rate limit 대기. 스레드 세이프."""
    global _last_call_time
    with _lock:
        sleep_time = get_sleep_time()
        elapsed = time_mod.time() - _last_call_time
        if elapsed < sleep_time:
            time_mod.sleep(sleep_time - elapsed)
        _last_call_time = time_mod.time()
