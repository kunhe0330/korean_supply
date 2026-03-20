"""
분석 통합 오케스트레이터 v3
수급 3박자 (외인기관 + 체결강도 + 거래량) + 상대강도 → 조건 필터 + 참고용 스코어 → 섹터 판별
VDU/Breakout 제거 — 수급 판별에만 집중
"""

import json
import logging
from datetime import datetime, timedelta

from db.migrations import get_connection
from config import REL_STRENGTH_BONUS
from supply.scorer import calc_supply_score, is_supply_inflow
from supply.sector import aggregate_by_theme, identify_leading_sectors, save_sector_analysis
from supply.theme_mapper import get_stock_themes

logger = logging.getLogger(__name__)


def calc_relative_strength(stock_code: str, market: str) -> dict:
    """종목 수익률 - 소속 지수 수익률 = 상대강도 (초과수익률)."""
    conn = get_connection()
    try:
        cutoff_1m = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")

        stock_prices = conn.execute(
            """SELECT close_price FROM price_daily
               WHERE stock_code = ? AND trade_date >= ?
               ORDER BY trade_date ASC""",
            (stock_code, cutoff_1m),
        ).fetchall()

        if len(stock_prices) < 2:
            return {"rel_strength_1m": 0, "rel_strength_1w": 0, "rs_rating": "NEUTRAL", "score": 0}

        stock_ret_1m = (stock_prices[-1]["close_price"] - stock_prices[0]["close_price"]) / stock_prices[0]["close_price"] * 100 if stock_prices[0]["close_price"] else 0

        week_prices = stock_prices[-6:] if len(stock_prices) >= 6 else stock_prices
        stock_ret_1w = (week_prices[-1]["close_price"] - week_prices[0]["close_price"]) / week_prices[0]["close_price"] * 100 if week_prices and week_prices[0]["close_price"] else 0

        market_name = "KOSPI" if market == "KOSPI" else "KOSDAQ"
        idx_prices = conn.execute(
            """SELECT index_close FROM index_daily
               WHERE market = ? AND trade_date >= ?
               ORDER BY trade_date ASC""",
            (market_name, cutoff_1m),
        ).fetchall()

        idx_ret_1m = 0
        idx_ret_1w = 0
        if len(idx_prices) >= 2:
            idx_ret_1m = (idx_prices[-1]["index_close"] - idx_prices[0]["index_close"]) / idx_prices[0]["index_close"] * 100 if idx_prices[0]["index_close"] else 0
            week_idx = idx_prices[-6:] if len(idx_prices) >= 6 else idx_prices
            idx_ret_1w = (week_idx[-1]["index_close"] - week_idx[0]["index_close"]) / week_idx[0]["index_close"] * 100 if week_idx and week_idx[0]["index_close"] else 0

        rs_1m = stock_ret_1m - idx_ret_1m
        rs_1w = stock_ret_1w - idx_ret_1w

        if rs_1m >= 5:
            rating = "STRONG"
        elif rs_1m >= -5:
            rating = "NEUTRAL"
        else:
            rating = "WEAK"

        bonus = 0
        for threshold in sorted(REL_STRENGTH_BONUS.keys(), reverse=True):
            if rs_1m >= threshold:
                bonus = REL_STRENGTH_BONUS[threshold]
                break

        return {
            "rel_strength_1m": round(rs_1m, 2),
            "rel_strength_1w": round(rs_1w, 2),
            "rs_rating": rating,
            "score": bonus,
        }
    finally:
        conn.close()


def analyze_stock(stock_code: str) -> dict:
    """
    단일 종목 종합 분석 (v3).
    수급 3박자 + 상대강도 → 조건 필터(is_inflow + tags) + 참고용 스코어
    """
    # 수급 기본 분석
    score_data = calc_supply_score(stock_code)

    # 상대강도
    conn = get_connection()
    try:
        market_row = conn.execute(
            "SELECT market FROM stock_master WHERE stock_code = ?", (stock_code,)
        ).fetchone()
        market = market_row["market"] if market_row else "KOSPI"
    finally:
        conn.close()

    rs = calc_relative_strength(stock_code, market)

    # 수급 유입 판별 (조건 필터)
    inflow_result = is_supply_inflow(
        today_data=score_data["today_data"],
        net_1m=score_data["net_1m"],
        accel=score_data["acceleration_type"],
        handover=score_data["handover_type"],
        vol_power=score_data["vol_power_today"],
        vol_ratio=score_data["vol_ratio_today"],
        rs_1m=rs["rel_strength_1m"],
    )

    # 테마 조회
    themes = get_stock_themes(stock_code)
    theme_list = [t["theme_id"] for t in themes]

    # 참고용 스코어 + RS 보너스
    ref_score = score_data["ref_score"]
    rs_bonus = rs["score"]

    return {
        "stock_code": stock_code,
        "is_inflow": 1 if inflow_result["is_inflow"] else 0,
        "tags": json.dumps(inflow_result["tags"], ensure_ascii=False),
        "tag_count": inflow_result["tag_count"],
        "tags_list": inflow_result["tags"],  # 내부 처리용 (DB 저장 안함)
        "net_6m": score_data["net_6m"],
        "net_3m": score_data["net_3m"],
        "net_1m": score_data["net_1m"],
        "net_1w": score_data["net_1w"],
        "net_today_amount": score_data["net_today_amount"],
        "acceleration_type": score_data["acceleration_type"],
        "handover_type": score_data["handover_type"],
        "vol_power_today": score_data["vol_power_today"],
        "vol_power_5d_avg": score_data["vol_power_5d_avg"],
        "vol_power_trend": score_data["vol_power_trend"],
        "vol_trend": score_data["vol_trend"],
        "vol_ratio_today": score_data["vol_ratio_today"],
        "theme_list": json.dumps(theme_list, ensure_ascii=False),
        "rel_strength_1m": rs["rel_strength_1m"],
        "ref_score": round(ref_score, 1),
        "ref_score_rs_bonus": rs_bonus,
        "rs_detail": rs,
    }


def save_supply_score(result: dict, calc_date: str):
    """분석 결과를 supply_score 테이블에 저장."""
    conn = get_connection()
    try:
        market_row = conn.execute(
            "SELECT sector_large, sector_name FROM stock_master WHERE stock_code = ?",
            (result["stock_code"],),
        ).fetchone()

        conn.execute(
            """INSERT OR REPLACE INTO supply_score
               (stock_code, calc_date, is_inflow, tags, tag_count,
                net_6m, net_3m, net_1m, net_1w, net_today_amount,
                acceleration_type, handover_type,
                vol_power_today, vol_power_5d_avg, vol_power_trend,
                vol_trend, vol_ratio_today, theme_list,
                rel_strength_1m, ref_score, ref_score_rs_bonus,
                sector_code, sector_name)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                result["stock_code"],
                calc_date,
                result["is_inflow"],
                result["tags"],
                result["tag_count"],
                result["net_6m"],
                result["net_3m"],
                result["net_1m"],
                result["net_1w"],
                result["net_today_amount"],
                result["acceleration_type"],
                result["handover_type"],
                result["vol_power_today"],
                result["vol_power_5d_avg"],
                result["vol_power_trend"],
                result["vol_trend"],
                result["vol_ratio_today"],
                result["theme_list"],
                result["rel_strength_1m"],
                result["ref_score"],
                result["ref_score_rs_bonus"],
                market_row["sector_large"] if market_row else "",
                market_row["sector_name"] if market_row else "",
            ),
        )
        conn.commit()
    finally:
        conn.close()


def run_analysis(stock_codes: list[str]):
    """
    전체 분석 배치 실행 (v3).
    수급 3박자 분석 → 조건 필터 → 섹터 집계 → 주도 섹터 판별
    """
    calc_date = datetime.now().strftime("%Y%m%d")
    logger.info("=== 분석 배치 시작 (%d 종목) ===", len(stock_codes))

    results = []
    for i, code in enumerate(stock_codes):
        try:
            result = analyze_stock(code)
            save_supply_score(result, calc_date)
            results.append(result)
        except Exception as e:
            logger.warning("종목 분석 실패 (%s): %s", code, e)

        if (i + 1) % 50 == 0:
            logger.info("분석 진행: %d/%d", i + 1, len(stock_codes))

    # 섹터 집계 + 주도 섹터 판별
    sector_list = aggregate_by_theme(calc_date)
    leading = identify_leading_sectors(sector_list)
    save_sector_analysis(sector_list, calc_date)

    inflow_count = sum(1 for r in results if r.get("is_inflow"))
    logger.info(
        "=== 분석 배치 완료: 종목 %d개, 수급유입 %d개, 주도 섹터 %d개 ===",
        len(results), inflow_count, len(leading),
    )

    return {
        "calc_date": calc_date,
        "stock_results": results,
        "all_sectors": sector_list,
        "leading_sectors": leading,
    }
