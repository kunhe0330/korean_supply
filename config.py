"""
KIS Supply-Demand Sector Analyzer — 설정 파일
환경변수로 관리되는 API 키, 스케줄, 분석 파라미터
"""

import os
from datetime import time

# ── KIS API ──────────────────────────────────────────────
KIS_APP_KEY = os.environ.get("KIS_APP_KEY", "")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET", "")
KIS_ACCOUNT_NO = os.environ.get("KIS_ACCOUNT_NO", "")
KIS_ACCOUNT_PRODUCT = os.environ.get("KIS_ACCOUNT_PRODUCT", "01")
KIS_BASE_URL = os.environ.get(
    "KIS_BASE_URL", "https://openapi.koreainvestment.com:9443"
)

# ── Telegram ─────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ── DB ───────────────────────────────────────────────────
DB_PATH = os.environ.get("DB_PATH", os.path.join("storage", "supply.db"))

# ── Rate Limit ───────────────────────────────────────────
API_RATE_LIMIT = {
    "calls_per_second": 10,
    "sleep_default": 0.08,
    "sleep_peak": 0.15,
    "peak_start": time(15, 30),
    "peak_end": time(16, 30),
    "max_retries": 3,
    "retry_delay": 2.0,
}

# ── 수급 분석 파라미터 ───────────────────────────────────
SUPPLY_PARAMS = {
    # 수급 스코어 임계값
    "score_threshold_high": 75,
    "score_threshold_mid": 50,
    "score_threshold_low": 30,
    # VDU 감지 파라미터
    "vdu_big_move_pct": 20,        # BIG MOVE 최소 상승률 (%)
    "vdu_big_move_days": 20,       # BIG MOVE 최소 연속 상승 거래일
    "vdu_pullback_min": -15,       # Pullback 최대 하락 (%)
    "vdu_pullback_max": -5,        # Pullback 최소 하락 (%)
    "vdu_vol_ratio": 0.4,          # VDU 거래량 비율 (BIG MOVE 대비)
    "vdu_vol_50d_ratio": 0.5,      # VDU 거래량 비율 (50일 평균 대비)
    # Breakout 감지 파라미터
    "breakout_vol_expansion": 2.0,  # 거래량 팽창 배수
    "breakout_vol_power_min": 120,  # 최소 체결강도 (%)
    "breakout_lookback": 20,        # 고가 돌파 확인 기간 (일)
    # 주도 섹터 판별
    "leading_sector_min_stocks": 3,  # 최소 수급 종목 수
    "leading_sector_accel_ratio": 0.5,  # 가속 종목 비율
}

# ── 상대강도 보너스 ──────────────────────────────────────
REL_STRENGTH_BONUS = {
    10: 10,  # +10%↑ → +10점
    5: 7,    # +5%↑  → +7점
    2: 4,    # +2%↑  → +4점
    0: 1,    # 0%↑   → +1점
}

# ── 데이터 파일 경로 ─────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
MANUAL_THEME_MAP_PATH = os.path.join(DATA_DIR, "manual_theme_map.json")
ETF_THEME_MAP_PATH = os.path.join(DATA_DIR, "etf_theme_map.json")
SECTOR_CODES_PATH = os.path.join(DATA_DIR, "sector_codes.json")
CACHE_DIR = os.path.join(DATA_DIR, "cache")

# ── Backfill ─────────────────────────────────────────────
BACKFILL_TOP_N = 500             # Backfill 대상 종목 수
BACKFILL_DAYS = 180              # 과거 데이터 수집 일수
