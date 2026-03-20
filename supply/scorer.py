"""
수급 스코어 알고리즘 v3
- 수급 유입 판별: 조건 필터 기반 (is_inflow + tags)
- 참고용 스코어: 수급 60점 + 체결강도 25점 + 거래량 15점 + RS 보너스 10점
- VDU/Breakout 제거 — 수급 3박자에 집중
"""

import json
import logging
from datetime import datetime, timedelta

from db.migrations import get_connection

logger = logging.getLogger(__name__)


# ── 기간별 순매수 계산 ─────────────────────────────────────

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


def _get_today_supply(conn, stock_code: str) -> dict:
    """당일 수급 데이터 조회."""
    row = conn.execute(
        """SELECT frgn_net_qty, orgn_net_qty, prsn_net_qty,
                  COALESCE(frgn_net_amount, 0) + COALESCE(orgn_net_amount, 0) as smart_amount,
                  vol_power, volume
           FROM daily_supply
           WHERE stock_code = ?
           ORDER BY trade_date DESC LIMIT 1""",
        (stock_code,),
    ).fetchone()
    if not row:
        return {"frgn_net": 0, "orgn_net": 0, "prsn_net": 0, "smart_amount": 0, "vol_power": 0, "volume": 0}
    return {
        "frgn_net": row["frgn_net_qty"] or 0,
        "orgn_net": row["orgn_net_qty"] or 0,
        "prsn_net": row["prsn_net_qty"] or 0,
        "smart_amount": row["smart_amount"] or 0,
        "vol_power": row["vol_power"] or 0,
        "volume": row["volume"] or 0,
    }


# ── 가속도 / 손바뀜 판별 ──────────────────────────────────

def check_acceleration(net_6m: int, net_3m: int, net_1m: int) -> str:
    """수급 가속도 판별."""
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
    """손바뀜 판별."""
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


# ── 체결강도 분석 ─────────────────────────────────────────

def calc_vol_power_analysis(conn, stock_code: str) -> dict:
    """체결강도 분석 (25점 만점)."""
    rows = conn.execute(
        """SELECT vol_power FROM daily_supply
           WHERE stock_code = ? AND vol_power IS NOT NULL
           ORDER BY trade_date DESC LIMIT 5""",
        (stock_code,),
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

    # A. 당일 체결강도 수준 (15점)
    if today_vp >= 200:
        level_score = 15
    elif today_vp >= 150:
        level_score = 12
    elif today_vp >= 120:
        level_score = 8
    elif today_vp >= 100:
        level_score = 4
    else:
        level_score = 0

    # B. 체결강도 추세 (10점)
    if trend == "SURGE" and today_vp >= 150:
        trend_score = 10
    elif trend in ("SURGE", "RISING"):
        trend_score = 7
    elif avg_5d >= 100:
        trend_score = 4
    else:
        trend_score = 0

    return {
        "today_vol_power": round(today_vp, 2),
        "avg_5d_vol_power": round(avg_5d, 2),
        "vol_power_trend": trend,
        "score": level_score + trend_score,
    }


# ── 거래량 추세 분석 ──────────────────────────────────────

def analyze_volume_trend(conn, stock_code: str) -> dict:
    """거래량 추세 분석 (15점 만점)."""
    rows = conn.execute(
        """SELECT volume FROM price_daily
           WHERE stock_code = ? AND volume > 0
           ORDER BY trade_date DESC LIMIT 20""",
        (stock_code,),
    ).fetchall()

    if len(rows) < 5:
        return {"vol_ratio_today": 0, "vol_trend": "STABLE", "score": 0}

    today_vol = rows[0]["volume"]
    avg_20d = sum(r["volume"] for r in rows) / len(rows)
    avg_3d = sum(r["volume"] for r in rows[:3]) / 3
    avg_prev3d = sum(r["volume"] for r in rows[3:6]) / min(3, len(rows[3:6])) if len(rows) > 3 else avg_20d

    vol_ratio = today_vol / avg_20d if avg_20d > 0 else 0

    # 추세 판정
    if avg_3d > avg_prev3d * 1.3:
        vol_trend = "EXPANDING"
    elif avg_3d < avg_prev3d * 0.7:
        vol_trend = "CONTRACTING"
    else:
        vol_trend = "STABLE"

    # A. 거래량 수준 (10점)
    if vol_ratio >= 2.0:
        level_score = 10
    elif vol_ratio >= 1.5:
        level_score = 7
    elif vol_ratio >= 0.7:
        level_score = 3
    else:
        level_score = 0

    # B. 거래량 방향 (5점)
    if vol_trend == "EXPANDING":
        trend_score = 5
    elif vol_trend == "STABLE":
        trend_score = 2
    else:
        trend_score = 0

    return {
        "vol_ratio_today": round(vol_ratio, 2),
        "vol_trend": vol_trend,
        "score": level_score + trend_score,
    }


# ── 수급 유입 판별 (조건 필터) ─────────────────────────────

def is_supply_inflow(today_data: dict, net_1m: int, accel: str, handover: str,
                     vol_power: float, vol_ratio: float, rs_1m: float) -> dict:
    """
    수급 유입 조건 필터.

    필수 조건 (모두 충족해야 is_inflow=True):
    1. 당일 기관+외인 합산 순매수가 양(+)
    2. 체결강도 100% 이상
    3. 최근 1개월 기관+외인 누적 순매수가 양(+)

    강화 조건 (충족 시 태그 부여):
    - "가속": 수급 가속 중
    - "손바뀜": 개인→기관외인 물량 이전
    - "체결강도↑": 체결강도 150%+
    - "거래량↑": 당일 거래량 > 20일 평균의 1.5배
    - "RS강함": 1개월 상대강도 +5%+
    """
    smart_today = today_data["frgn_net"] + today_data["orgn_net"]

    # 필수 조건 체크
    cond1 = smart_today > 0
    cond2 = vol_power >= 100
    cond3 = net_1m > 0
    is_inflow = cond1 and cond2 and cond3

    # 강화 조건 태그
    tags = []
    if accel in ("FULL_ACCEL", "SHORT_ACCEL", "REVERSAL"):
        tags.append("가속")
    if handover in ("HANDOVER_STRONG", "HANDOVER_MILD"):
        tags.append("손바뀜")
    if vol_power >= 150:
        tags.append("체결강도↑")
    if vol_ratio >= 1.5:
        tags.append("거래량↑")
    if rs_1m >= 5:
        tags.append("RS강함")

    return {
        "is_inflow": is_inflow,
        "tags": tags,
        "tag_count": len(tags),
    }


# ── 참고용 스코어 (100점 만점) ─────────────────────────────

def calc_ref_score(net_6m: int, net_3m: int, net_1m: int, net_1w: int,
                   accel: str, handover: str,
                   vp_score: int, vol_score: int) -> float:
    """
    참고용 스코어 계산 (정렬 옵션 전용).

    A. 외인기관 수급 (60점)
    B. 체결강도 (25점) — vp_score
    C. 거래량 (15점) — vol_score
    """
    # A1. 기간별 순매수 방향 (15점)
    direction = 0
    if net_6m > 0:
        direction += 5
    if net_3m > 0:
        direction += 5
    if net_1m > 0:
        direction += 5

    # A2. 수급 가속도 (20점)
    accel_score = {"FULL_ACCEL": 20, "SHORT_ACCEL": 15, "REVERSAL": 10}.get(accel, 0)

    # A3. 손바뀜 (15점)
    handover_score = {"HANDOVER_STRONG": 15, "HANDOVER_MILD": 10}.get(handover, 0)

    # A4. 수급 집중도 (10점)
    avg_1m_daily = net_1m / 20 if net_1m else 0
    avg_1w_daily = net_1w / 5 if net_1w else 0
    concentration = 0
    if avg_1m_daily > 0:
        ratio = avg_1w_daily / avg_1m_daily
        if ratio >= 2.0:
            concentration = 10
        elif ratio >= 1.5:
            concentration = 7
        elif ratio >= 1.0:
            concentration = 3

    supply_score = direction + accel_score + handover_score + concentration  # max 60

    return min(100, supply_score + vp_score + vol_score)


# ── 종합 스코어 계산 (메인 엔트리) ─────────────────────────

def calc_supply_score(stock_code: str) -> dict:
    """
    종합 수급 분석 (v3).
    조건 필터(is_inflow + tags) + 참고용 스코어(ref_score).
    """
    conn = get_connection()
    try:
        # 기간별 순매수
        net_6m = _calc_period_net(conn, stock_code, 6)
        net_3m = _calc_period_net(conn, stock_code, 3)
        net_1m = _calc_period_net(conn, stock_code, 1)
        net_1w = _calc_period_net_weeks(conn, stock_code, 1)
        today_data = _get_today_supply(conn, stock_code)

        # 가속도
        accel = check_acceleration(
            net_6m["smart_net"], net_3m["smart_net"], net_1m["smart_net"]
        )

        # 손바뀜
        handover = check_handover(conn, stock_code)

        # 체결강도
        vp = calc_vol_power_analysis(conn, stock_code)

        # 거래량 추세
        vol = analyze_volume_trend(conn, stock_code)

        # 참고용 스코어 (RS 보너스 제외 — analyzer에서 합산)
        ref_score = calc_ref_score(
            net_6m["smart_net"], net_3m["smart_net"], net_1m["smart_net"],
            net_1w["smart_net"], accel, handover, vp["score"], vol["score"],
        )

        return {
            "stock_code": stock_code,
            "net_6m": net_6m["smart_net"],
            "net_3m": net_3m["smart_net"],
            "net_1m": net_1m["smart_net"],
            "net_1w": net_1w["smart_net"],
            "net_today_amount": today_data["smart_amount"],
            "today_data": today_data,
            "acceleration_type": accel,
            "handover_type": handover,
            "vol_power_today": vp["today_vol_power"],
            "vol_power_5d_avg": vp["avg_5d_vol_power"],
            "vol_power_trend": vp["vol_power_trend"],
            "vol_trend": vol["vol_trend"],
            "vol_ratio_today": vol["vol_ratio_today"],
            "ref_score": round(ref_score, 1),
            "vp_score": vp["score"],
            "vol_score": vol["score"],
        }
    finally:
        conn.close()
