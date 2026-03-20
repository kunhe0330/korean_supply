"""
Breakout 감지 + Stage 분류
VDU 상태에서 거래량 동반 상승 돌파 감지.
"""

import logging
from datetime import datetime, timedelta

from db.migrations import get_connection
from config import SUPPLY_PARAMS

logger = logging.getLogger(__name__)


def detect_breakout(stock_code: str) -> dict:
    """
    Breakout 감지.
    조건 (모두 충족):
    1. 직전 상태가 VDU
    2. 당일 종가가 최근 20일 고가 상향 돌파
    3. 당일 거래량이 최근 5일 평균 대비 2배 이상
    4. 양봉 마감 (종가 > 시가)
    5. 당일 체결강도 120% 이상
    """
    conn = get_connection()
    try:
        cutoff = (datetime.now() - timedelta(days=40)).strftime("%Y%m%d")

        # 가격 데이터
        prices = conn.execute(
            """SELECT trade_date, open_price, high_price, low_price,
                      close_price, volume
               FROM price_daily
               WHERE stock_code = ? AND trade_date >= ?
               ORDER BY trade_date ASC""",
            (stock_code, cutoff),
        ).fetchall()

        if len(prices) < 5:
            return _no_breakout()

        today = prices[-1]
        today_close = today["close_price"] or 0
        today_open = today["open_price"] or 0
        today_volume = today["volume"] or 0

        # 체결강도
        supply_row = conn.execute(
            """SELECT vol_power FROM daily_supply
               WHERE stock_code = ? AND trade_date = ?""",
            (stock_code, today["trade_date"]),
        ).fetchone()
        vol_power = supply_row["vol_power"] if supply_row and supply_row["vol_power"] else 0

        # 20일 고가
        lookback = SUPPLY_PARAMS["breakout_lookback"]
        recent_prices = prices[-lookback - 1:-1] if len(prices) > lookback else prices[:-1]
        high_20d = max((p["high_price"] or 0) for p in recent_prices) if recent_prices else 0

        # 5일 평균 거래량
        recent_5d = prices[-6:-1] if len(prices) > 5 else prices[:-1]
        avg_vol_5d = sum((p["volume"] or 0) for p in recent_5d) / len(recent_5d) if recent_5d else 1

        # 거래량 팽창
        vol_expansion = today_volume / avg_vol_5d if avg_vol_5d > 0 else 0

        # 양봉 여부
        close_above_open = today_close > today_open

        # 20일 고가 돌파
        price_above_20d = today_close > high_20d if high_20d > 0 else False

        # Breakout 타입 판정
        breakout_type = "NONE"
        is_breakout = False

        min_vol_expansion = SUPPLY_PARAMS["breakout_vol_expansion"]
        min_vol_power = SUPPLY_PARAMS["breakout_vol_power_min"]

        if (price_above_20d
                and vol_expansion >= min_vol_expansion
                and close_above_open
                and vol_power >= min_vol_power):
            breakout_type = "HIGH_BREAK"
            is_breakout = True
        elif (today_close >= high_20d * 0.95
              and vol_expansion >= min_vol_expansion
              and close_above_open
              and vol_power >= min_vol_power):
            breakout_type = "RECOVERY"
            is_breakout = True

        return {
            "is_breakout": is_breakout,
            "breakout_type": breakout_type,
            "price_above_20d_high": price_above_20d,
            "volume_expansion": round(vol_expansion, 2),
            "vol_power": round(vol_power, 2),
            "close_above_open": close_above_open,
        }
    finally:
        conn.close()


def classify_stage(stock_code: str, vdu_result: dict, breakout_result: dict) -> str:
    """
    BIG MOVE → PULLBACK → VDU → BREAKOUT → EXTENDED 사이클 단계 판별.
    """
    if breakout_result.get("is_breakout"):
        # Breakout 이후 추가 상승 확인
        conn = get_connection()
        try:
            recent = conn.execute(
                """SELECT close_price FROM price_daily
                   WHERE stock_code = ?
                   ORDER BY trade_date DESC LIMIT 5""",
                (stock_code,),
            ).fetchall()
            if len(recent) >= 3:
                # 3일 연속 상승이면 EXTENDED
                if all(recent[i]["close_price"] <= recent[i + 1]["close_price"]
                       for i in range(min(2, len(recent) - 1))):
                    pass  # 아직 당일이므로 BREAKOUT 유지
        finally:
            conn.close()
        return "BREAKOUT"

    stage = vdu_result.get("stage", "NONE")
    return stage


def calc_breakout_score(breakout_result: dict) -> float:
    """Breakout 관련 스코어 (15점 만점)."""
    if not breakout_result.get("is_breakout"):
        # 20일 고가 근접 체크
        if breakout_result.get("price_above_20d_high"):
            return 5
        return 0

    vol_exp = breakout_result.get("volume_expansion", 0)
    vp = breakout_result.get("vol_power", 0)

    if vol_exp >= 3.0 and vp >= 150:
        return 15  # 완벽
    if vol_exp >= 2.0:
        return 12
    return 8


def _no_breakout() -> dict:
    return {
        "is_breakout": False,
        "breakout_type": "NONE",
        "price_above_20d_high": False,
        "volume_expansion": 0,
        "vol_power": 0,
        "close_above_open": False,
    }
