"""
Volume Dry Up (VDU) 감지
BIG MOVE → Shallow Pullback → Volume Dry Up 패턴 탐지
"""

import logging
from datetime import datetime, timedelta

from db.migrations import get_connection
from config import SUPPLY_PARAMS

logger = logging.getLogger(__name__)


def detect_volume_dry_up(stock_code: str, lookback_days: int = 60) -> dict:
    """
    VDU 패턴 감지.
    1. BIG MOVE: 최근 60거래일 내 20거래일 이상 연속 상승, 상승률 20%+
    2. Shallow Pullback: 고점 대비 -5% ~ -15% 조정
    3. Volume Dry Up: 최근 5일 평균 거래량 < BIG MOVE 평균의 40%
    """
    conn = get_connection()
    try:
        cutoff = (datetime.now() - timedelta(days=lookback_days * 1.5)).strftime("%Y%m%d")
        rows = conn.execute(
            """SELECT trade_date, close_price, high_price, low_price, volume
               FROM price_daily
               WHERE stock_code = ? AND trade_date >= ?
               ORDER BY trade_date ASC""",
            (stock_code, cutoff),
        ).fetchall()

        if len(rows) < 20:
            return _no_vdu()

        prices = [{"date": r["trade_date"], "close": r["close_price"],
                    "high": r["high_price"], "low": r["low_price"],
                    "volume": r["volume"]} for r in rows]

        # ── Step 1: BIG MOVE 구간 찾기 ──
        big_move = _find_big_move(prices)
        if not big_move:
            return _no_vdu()

        # ── Step 2: Shallow Pullback 확인 ──
        high_idx = big_move["high_idx"]
        high_price = big_move["high_price"]
        current_price = prices[-1]["close"]

        pullback_pct = (current_price - high_price) / high_price * 100
        pb_min = SUPPLY_PARAMS["vdu_pullback_min"]
        pb_max = SUPPLY_PARAMS["vdu_pullback_max"]

        # 고점 이후 일수
        days_since_high = len(prices) - 1 - high_idx

        if pullback_pct < pb_min:
            # 너무 깊은 조정 → Pullback 실패
            return {
                "is_vdu": False,
                "big_move_pct": big_move["move_pct"],
                "pullback_pct": round(pullback_pct, 2),
                "vol_ratio": 0,
                "vol_50d_ratio": 0,
                "days_since_high": days_since_high,
                "stage": "NONE",
            }

        if pullback_pct > 0:
            # 아직 고점 돌파 중 → BIG_MOVE 단계
            stage = "BIG_MOVE"
        else:
            stage = "PULLBACK"

        # ── Step 3: Volume Dry Up 확인 ──
        recent_5d_vol = [p["volume"] for p in prices[-5:] if p["volume"]]
        big_move_vols = [p["volume"] for p in prices[big_move["start_idx"]:high_idx + 1] if p["volume"]]
        all_vols = [p["volume"] for p in prices if p["volume"]]

        avg_recent_5d = sum(recent_5d_vol) / len(recent_5d_vol) if recent_5d_vol else 0
        avg_big_move = sum(big_move_vols) / len(big_move_vols) if big_move_vols else 1
        avg_50d = sum(all_vols[-50:]) / min(len(all_vols), 50) if all_vols else 1

        vol_ratio = avg_recent_5d / avg_big_move if avg_big_move > 0 else 1
        vol_50d_ratio = avg_recent_5d / avg_50d if avg_50d > 0 else 1

        is_vdu = (
            stage == "PULLBACK"
            and (vol_ratio < SUPPLY_PARAMS["vdu_vol_ratio"]
                 or vol_50d_ratio < SUPPLY_PARAMS["vdu_vol_50d_ratio"])
            and days_since_high >= 3
        )

        if is_vdu:
            stage = "VDU"

        # 수급 이탈 여부 확인
        supply_rows = conn.execute(
            """SELECT COALESCE(SUM(frgn_net_qty + orgn_net_qty), 0) as smart_net
               FROM daily_supply
               WHERE stock_code = ? AND trade_date >= ?""",
            (stock_code, prices[high_idx]["date"]),
        ).fetchone()
        supply_maintained = supply_rows and supply_rows["smart_net"] >= 0

        return {
            "is_vdu": is_vdu,
            "big_move_pct": round(big_move["move_pct"], 2),
            "pullback_pct": round(pullback_pct, 2),
            "vol_ratio": round(vol_ratio, 3),
            "vol_50d_ratio": round(vol_50d_ratio, 3),
            "days_since_high": days_since_high,
            "stage": stage,
            "supply_maintained": supply_maintained,
        }
    finally:
        conn.close()


def _find_big_move(prices: list[dict]) -> dict | None:
    """
    BIG MOVE 구간 탐색.
    최근 데이터에서 역순으로 20거래일 이상 상승률 20%+ 구간 탐색.
    """
    if len(prices) < 20:
        return None

    min_pct = SUPPLY_PARAMS["vdu_big_move_pct"]

    # 고점 찾기
    high_price = 0
    high_idx = 0
    for i, p in enumerate(prices):
        if p["high"] and p["high"] > high_price:
            high_price = p["high"]
            high_idx = i

    if high_price == 0:
        return None

    # 고점 이전에서 저점 찾기 (BIG MOVE 시작점)
    low_price = high_price
    low_idx = high_idx
    for i in range(high_idx, -1, -1):
        if prices[i]["low"] and prices[i]["low"] < low_price:
            low_price = prices[i]["low"]
            low_idx = i

    if low_price == 0:
        return None

    move_pct = (high_price - low_price) / low_price * 100
    move_days = high_idx - low_idx

    if move_pct >= min_pct and move_days >= 5:
        return {
            "start_idx": low_idx,
            "high_idx": high_idx,
            "low_price": low_price,
            "high_price": high_price,
            "move_pct": move_pct,
            "move_days": move_days,
        }

    return None


def _no_vdu() -> dict:
    return {
        "is_vdu": False,
        "big_move_pct": 0,
        "pullback_pct": 0,
        "vol_ratio": 0,
        "vol_50d_ratio": 0,
        "days_since_high": 0,
        "stage": "NONE",
    }


def calc_vdu_score(vdu_result: dict) -> float:
    """VDU 관련 스코어 (15점 만점)."""
    if vdu_result.get("is_vdu") and vdu_result.get("supply_maintained"):
        return 15
    if vdu_result.get("is_vdu"):
        return 8
    if vdu_result.get("stage") == "PULLBACK":
        return 4
    return 0
