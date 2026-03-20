"""
분석 통합 오케스트레이터
수급 스코어 + VDU + Breakout + 상대강도 → 종합 스코어 → 섹터 판별
"""

import json
import logging
from datetime import datetime, timedelta

from db.migrations import get_connection
from config import REL_STRENGTH_BONUS
from supply.scorer import calc_supply_score
from supply.vdu_detector import detect_volume_dry_up, calc_vdu_score
from supply.breakout_detector import detect_breakout, classify_stage, calc_breakout_score
from supply.sector import aggregate_by_theme, identify_leading_sectors, save_sector_analysis
from supply.theme_mapper import get_stock_themes

logger = logging.getLogger(__name__)


def calc_relative_strength(stock_code: str, market: str) -> dict:
    """
    종목 수익률 - 소속 지수 수익률 = 상대강도 (초과수익률).
    """
    conn = get_connection()
    try:
        cutoff_1m = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
        cutoff_1w = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")

        # 종목 수익률
        stock_prices = conn.execute(
            """SELECT close_price FROM price_daily
               WHERE stock_code = ? AND trade_date >= ?
               ORDER BY trade_date ASC""",
            (stock_code, cutoff_1m),
        ).fetchall()

        if len(stock_prices) < 2:
            return {"rel_strength_1m": 0, "rel_strength_1w": 0, "rs_rating": "NEUTRAL", "score": 0}

        stock_ret_1m = (stock_prices[-1]["close_price"] - stock_prices[0]["close_price"]) / stock_prices[0]["close_price"] * 100 if stock_prices[0]["close_price"] else 0

        # 1주 수익률
        week_prices = [p for i, p in enumerate(stock_prices) if i >= len(stock_prices) - 6]
        stock_ret_1w = (week_prices[-1]["close_price"] - week_prices[0]["close_price"]) / week_prices[0]["close_price"] * 100 if week_prices and week_prices[0]["close_price"] else 0

        # 지수 수익률
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

        # RS 등급
        if rs_1m >= 5:
            rating = "STRONG"
        elif rs_1m >= -5:
            rating = "NEUTRAL"
        else:
            rating = "WEAK"

        # 보너스 점수
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
    단일 종목 종합 분석.
    수급 스코어 + VDU + Breakout + 상대강도 → 종합 스코어
    """
    # 수급 기본 스코어 (50점 + 체결강도 20점 = 70점)
    score_data = calc_supply_score(stock_code)

    # VDU 감지 (15점)
    vdu_result = detect_volume_dry_up(stock_code)
    vdu_score = calc_vdu_score(vdu_result)

    # Breakout 감지 (15점)
    breakout_result = detect_breakout(stock_code)
    breakout_score = calc_breakout_score(breakout_result)

    # Stage 분류
    stage = classify_stage(stock_code, vdu_result, breakout_result)

    # 종합 스코어 (최대 100점)
    total = min(100, score_data["score_total"] + vdu_score + breakout_score)

    # 상대강도 (보너스)
    conn = get_connection()
    try:
        market_row = conn.execute(
            "SELECT market FROM stock_master WHERE stock_code = ?", (stock_code,)
        ).fetchone()
        market = market_row["market"] if market_row else "KOSPI"
    finally:
        conn.close()

    rs = calc_relative_strength(stock_code, market)

    # 테마 조회
    themes = get_stock_themes(stock_code)
    theme_list = [t["theme_id"] for t in themes]

    return {
        **score_data,
        "score_total": round(total, 1),
        "vdu_flag": 1 if vdu_result.get("is_vdu") else 0,
        "breakout_flag": 1 if breakout_result.get("is_breakout") else 0,
        "stage": stage,
        "theme_list": json.dumps(theme_list, ensure_ascii=False),
        "rel_strength_1m": rs["rel_strength_1m"],
        "rel_strength_bonus": rs["score"],
        "vdu_detail": vdu_result,
        "breakout_detail": breakout_result,
        "rs_detail": rs,
    }


def save_supply_score(result: dict, calc_date: str):
    """분석 결과를 supply_score 테이블에 저장."""
    conn = get_connection()
    try:
        # sector 정보 조회
        market_row = conn.execute(
            "SELECT sector_large, sector_name FROM stock_master WHERE stock_code = ?",
            (result["stock_code"],),
        ).fetchone()

        conn.execute(
            """INSERT OR REPLACE INTO supply_score
               (stock_code, calc_date, score_total, net_6m, net_3m, net_1m, net_1w,
                acceleration_flag, handover_flag, vdu_flag, breakout_flag,
                vol_power_today, vol_power_5d_avg, vol_power_trend,
                stage, theme_list, rel_strength_1m, rel_strength_bonus,
                sector_code, sector_name)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                result["stock_code"],
                calc_date,
                result["score_total"],
                result["net_6m"],
                result["net_3m"],
                result["net_1m"],
                result["net_1w"],
                result["acceleration_flag"],
                result["handover_flag"],
                result["vdu_flag"],
                result["breakout_flag"],
                result["vol_power_today"],
                result["vol_power_5d_avg"],
                result["vol_power_trend"],
                result["stage"],
                result["theme_list"],
                result["rel_strength_1m"],
                result["rel_strength_bonus"],
                market_row["sector_large"] if market_row else "",
                market_row["sector_name"] if market_row else "",
            ),
        )
        conn.commit()
    finally:
        conn.close()


def run_analysis(stock_codes: list[str]):
    """
    전체 분석 배치 실행.
    Step 4: 수급 스코어 계산
    Step 5: VDU + Breakout 감지
    Step 6: 섹터 집계 → 주도 섹터 판별
    """
    calc_date = datetime.now().strftime("%Y%m%d")
    logger.info("=== 분석 배치 시작 (%d 종목) ===", len(stock_codes))

    # Step 4~5: 종목별 분석
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

    # Step 6: 섹터 집계 + 주도 섹터 판별
    sector_list = aggregate_by_theme(calc_date)
    leading = identify_leading_sectors(sector_list)
    save_sector_analysis(sector_list, calc_date)

    logger.info(
        "=== 분석 배치 완료: 종목 %d개, 주도 섹터 %d개 ===",
        len(results), len(leading),
    )

    return {
        "calc_date": calc_date,
        "stock_results": results,
        "all_sectors": sector_list,
        "leading_sectors": leading,
    }
