"""
수급 스코어 알고리즘 — 기간별 순매수 방향 + 가속도 + 손바뀜 + 집중도 + 체결강도
"""

import logging
from datetime import datetime, timedelta

from db.migrations import get_connection

logger = logging.getLogger(__name__)


def _calc_period_net(conn, stock_code: str, months: int) -> dict:
    """N개월 기관+외인 순매수 누적 계산."""
    cutoff = (datetime.now() - timedelta(days=months * 30)).strftime("%Y%m%d")
    row = conn.execute(
        """SELECT
             COALESCE(SUM(frgn_net_qty), 0) as frgn_net,
             COALESCE(SUM(orgn_net_qty), 0) as orgn_net,
             COALESCE(SUM(prsn_net_qty), 0) as prsn_net,
             COALESCE(SUM(frgn_net_qty + orgn_net_qty), 0) as smart_net,
             COALESCE(SUM(COALESCE(frgn_net_amount, 0) + COALESCE(orgn_net_amount, 0)), 0) as smart_amount
           FROM daily_supply
           WHERE stock_code = ? AND trade_date >= ?""",
        (stock_code, cutoff),
    ).fetchone()
    return dict(row) if row else {"frgn_net": 0, "orgn_net": 0, "prsn_net": 0, "smart_net": 0, "smart_amount": 0}


def _calc_period_net_weeks(conn, stock_code: str, weeks: int) -> dict:
    """N주 기관+외인 순매수 누적."""
    cutoff = (datetime.now() - timedelta(weeks=weeks)).strftime("%Y%m%d")
    row = conn.execute(
        """SELECT
             COALESCE(SUM(frgn_net_qty + orgn_net_qty), 0) as smart_net
           FROM daily_supply
           WHERE stock_code = ? AND trade_date >= ?""",
        (stock_code, cutoff),
    ).fetchone()
    return {"smart_net": row["smart_net"] if row else 0}


def check_acceleration(net_6m: int, net_3m: int, net_1m: int) -> str:
    """
    수급 가속도 판별.
    월평균 순매수로 환산하여 비교.
    """
    avg_6m = net_6m / 6 if net_6m else 0
    avg_3m = net_3m / 3 if net_3m else 0
    avg_1m = net_1m

    if avg_1m > avg_3m > avg_6m and avg_6m > 0:
        return "FULL_ACCEL"
    if avg_1m > avg_3m and avg_3m > 0:
        return "SHORT_ACCEL"
    if net_6m < 0 and net_1m > 0:
        return "REVERSAL"
    if avg_1m < avg_3m < avg_6m and avg_1m > 0:
        return "DECEL"
    return "FLAT"


def check_handover(conn, stock_code: str) -> str:
    """
    손바뀜 판별.
    최근 1M 개인 순매도 + 기관/외인 순매수 + 주가 상승 = 손바뀜
    """
    cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
    row = conn.execute(
        """SELECT
             COALESCE(SUM(prsn_net_qty), 0) as prsn_1m,
             COALESCE(SUM(frgn_net_qty + orgn_net_qty), 0) as smart_1m
           FROM daily_supply
           WHERE stock_code = ? AND trade_date >= ?""",
        (stock_code, cutoff),
    ).fetchone()

    if not row:
        return "NONE"

    prsn_1m = row["prsn_1m"]
    smart_1m = row["smart_1m"]

    # 주가 변화 확인
    prices = conn.execute(
        """SELECT close_price FROM daily_supply
           WHERE stock_code = ? AND trade_date >= ?
           ORDER BY trade_date""",
        (stock_code, cutoff),
    ).fetchall()

    if len(prices) < 2:
        return "NONE"

    first_price = prices[0]["close_price"] or 1
    last_price = prices[-1]["close_price"] or 1
    price_change = (last_price - first_price) / first_price

    if prsn_1m < 0 and smart_1m > 0 and price_change > 0:
        return "HANDOVER_STRONG"
    if prsn_1m < 0 and smart_1m > 0:
        return "HANDOVER_MILD"
    if price_change > 0 and prsn_1m > 0 and smart_1m < 0:
        return "DISTRIBUTION"
    return "NONE"


def calc_vol_power_score(conn, stock_code: str) -> dict:
    """
    체결강도 기반 점수 산출 (20점 만점).
    """
    today = datetime.now().strftime("%Y%m%d")
    cutoff_5d = (datetime.now() - timedelta(days=10)).strftime("%Y%m%d")

    rows = conn.execute(
        """SELECT vol_power FROM daily_supply
           WHERE stock_code = ? AND trade_date >= ? AND vol_power IS NOT NULL
           ORDER BY trade_date DESC LIMIT 5""",
        (stock_code, cutoff_5d),
    ).fetchall()

    if not rows:
        return {"today_vol_power": 0, "avg_5d_vol_power": 0, "vol_power_trend": "STABLE", "score": 0}

    today_vp = rows[0]["vol_power"] if rows else 0
    avg_5d = sum(r["vol_power"] for r in rows) / len(rows) if rows else 0

    # 추세 판단
    if today_vp >= 150 and today_vp > avg_5d * 1.5:
        trend = "SURGE"
    elif today_vp > avg_5d * 1.1:
        trend = "RISING"
    elif today_vp < avg_5d * 0.9:
        trend = "FALLING"
    else:
        trend = "STABLE"

    # 점수 (20점 만점)
    # A. 당일 체결강도 수준 (12점)
    if today_vp >= 200:
        level_score = 12
    elif today_vp >= 150:
        level_score = 9
    elif today_vp >= 120:
        level_score = 6
    elif today_vp >= 100:
        level_score = 3
    else:
        level_score = 0

    # B. 체결강도 추세 (8점)
    if trend == "SURGE":
        trend_score = 8
    elif trend == "RISING" and avg_5d >= 120:
        trend_score = 5
    elif avg_5d >= 120:
        trend_score = 3
    else:
        trend_score = 0

    return {
        "today_vol_power": round(today_vp, 2),
        "avg_5d_vol_power": round(avg_5d, 2),
        "vol_power_trend": trend,
        "score": level_score + trend_score,
    }


def calc_supply_score(stock_code: str) -> dict:
    """
    종합 수급 스코어 계산 (0~100).

    A. 수급 기본 점수 (50점)
    B. 체결강도 점수 (20점)
    C. VDU/Breakout 점수 (30점) — vdu_detector/breakout_detector에서 별도 계산
    """
    conn = get_connection()
    try:
        # 기간별 순매수
        net_6m = _calc_period_net(conn, stock_code, 6)
        net_3m = _calc_period_net(conn, stock_code, 3)
        net_1m = _calc_period_net(conn, stock_code, 1)
        net_1w = _calc_period_net_weeks(conn, stock_code, 1)

        # ── A. 수급 기본 점수 (50점) ──

        # 1. 기간별 순매수 방향 (15점)
        direction_score = 0
        if net_6m["smart_net"] > 0:
            direction_score += 5
        if net_3m["smart_net"] > 0:
            direction_score += 5
        if net_1m["smart_net"] > 0:
            direction_score += 5

        # 2. 수급 가속도 (15점)
        accel = check_acceleration(
            net_6m["smart_net"], net_3m["smart_net"], net_1m["smart_net"]
        )
        accel_score = {
            "FULL_ACCEL": 15,
            "SHORT_ACCEL": 12,
            "REVERSAL": 8,
            "DECEL": 0,
            "FLAT": 0,
        }.get(accel, 0)

        # 3. 손바뀜 (12점)
        handover = check_handover(conn, stock_code)
        handover_score = {
            "HANDOVER_STRONG": 12,
            "HANDOVER_MILD": 8,
            "DISTRIBUTION": 0,
            "NONE": 0,
        }.get(handover, 0)

        # 4. 수급 집중도 (8점)
        concentration_score = 0
        avg_1m_daily = net_1m["smart_net"] / 20 if net_1m["smart_net"] else 0
        avg_1w_daily = net_1w["smart_net"] / 5 if net_1w["smart_net"] else 0
        if avg_1m_daily != 0:
            ratio = avg_1w_daily / avg_1m_daily if avg_1m_daily > 0 else 0
            if ratio >= 2.0:
                concentration_score = 8
            elif ratio >= 1.5:
                concentration_score = 5
            elif ratio >= 1.0:
                concentration_score = 3

        supply_base_score = direction_score + accel_score + handover_score + concentration_score

        # ── B. 체결강도 점수 (20점) ──
        vp = calc_vol_power_score(conn, stock_code)

        # 종합 (VDU/Breakout은 analyzer.py에서 합산)
        score_total = supply_base_score + vp["score"]

        return {
            "stock_code": stock_code,
            "score_total": score_total,
            "net_6m": net_6m["smart_net"],
            "net_3m": net_3m["smart_net"],
            "net_1m": net_1m["smart_net"],
            "net_1w": net_1w["smart_net"],
            "acceleration": accel,
            "acceleration_flag": 1 if accel in ("FULL_ACCEL", "SHORT_ACCEL") else 0,
            "handover": handover,
            "handover_flag": 1 if "HANDOVER" in handover else 0,
            "vol_power_today": vp["today_vol_power"],
            "vol_power_5d_avg": vp["avg_5d_vol_power"],
            "vol_power_trend": vp["vol_power_trend"],
        }
    finally:
        conn.close()
