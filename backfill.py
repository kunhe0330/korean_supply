"""
Backfill — 최초 1회 과거 데이터 일괄 수집 + 분석 실행
상위 500종목 과거 180일 데이터 수집 (수급 + OHLCV + 체결량 + 지수)
예상 소요: 약 10~15분 (rate limit 준수)
"""

import logging
import sys
from datetime import datetime, timedelta

from db.migrations import init_db, get_connection
from config import BACKFILL_TOP_N, BACKFILL_DAYS
from supply.collector import collect_top_supply_stocks, collect_investor_trade_daily, refresh_stock_master
from supply.price_collector import collect_ohlcv, collect_trade_volume, collect_index_daily
from supply.analyzer import run_analysis
from supply.theme_mapper import run_theme_update

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def run_backfill():
    """과거 데이터 일괄 수집 + 분석."""
    logger.info("=" * 60)
    logger.info("Backfill 시작: 상위 %d종목, 과거 %d일", BACKFILL_TOP_N, BACKFILL_DAYS)
    logger.info("=" * 60)

    # DB 초기화
    init_db()

    today = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=BACKFILL_DAYS)).strftime("%Y%m%d")

    # Step 1: 가집계 TOP 종목 수집
    logger.info("[1/7] 가집계 TOP 종목 수집...")
    top_codes = collect_top_supply_stocks()
    logger.info("가집계 TOP 종목: %d개", len(top_codes))

    # 대상 종목 = TOP 종목 (최대 BACKFILL_TOP_N개)
    target_codes = top_codes[:BACKFILL_TOP_N]
    logger.info("Backfill 대상: %d종목", len(target_codes))

    # Step 2: 종목 마스터 갱신
    logger.info("[2/7] 종목 마스터 갱신...")
    refresh_stock_master(target_codes)

    # Step 3: 투자자매매동향 수집
    logger.info("[3/7] 투자자매매동향 수집 (과거 %d일)...", BACKFILL_DAYS)
    supply_count = collect_investor_trade_daily(target_codes, today)
    if supply_count == 0:
        logger.warning("⚠️ 수급 데이터 수집 실패 — KIS API 시간 제한일 수 있음 (15:40 이전에만 가능)")

    # Step 4: OHLCV 수집
    logger.info("[4/7] OHLCV 수집...")
    collect_ohlcv(target_codes, start_date, today)

    # Step 5: 체결량 수집
    logger.info("[5/7] 체결량 수집...")
    collect_trade_volume(target_codes, start_date, today)

    # Step 6: 지수 데이터 수집
    logger.info("[6/7] 지수 데이터 수집...")
    collect_index_daily(start_date)

    # Step 7: 테마 매핑 + 분석 실행
    logger.info("[7/7] 테마 매핑 + 분석 실행...")
    try:
        run_theme_update()
    except Exception as e:
        logger.warning("테마 매핑 실패 (무시하고 계속): %s", e)

    try:
        analysis_result = run_analysis(target_codes)
        leading_count = len(analysis_result.get("leading_sectors", []))
        logger.info("분석 완료: 주도 섹터 %d개 판별", leading_count)
    except Exception as e:
        logger.warning("분석 실행 실패: %s", e)

    # 결과 확인
    conn = get_connection()
    try:
        master_count = conn.execute("SELECT COUNT(*) as c FROM stock_master").fetchone()["c"]
        supply_count = conn.execute("SELECT COUNT(*) as c FROM daily_supply").fetchone()["c"]
        price_count = conn.execute("SELECT COUNT(*) as c FROM price_daily").fetchone()["c"]
        index_count = conn.execute("SELECT COUNT(*) as c FROM index_daily").fetchone()["c"]
        score_count = conn.execute("SELECT COUNT(*) as c FROM supply_score").fetchone()["c"]
        sector_count = conn.execute("SELECT COUNT(*) as c FROM sector_analysis").fetchone()["c"]
    finally:
        conn.close()

    logger.info("=" * 60)
    logger.info("Backfill 완료!")
    logger.info("  종목 마스터: %d건", master_count)
    logger.info("  일별 수급: %d건", supply_count)
    logger.info("  일별 가격: %d건", price_count)
    logger.info("  지수 데이터: %d건", index_count)
    logger.info("  수급 스코어: %d건", score_count)
    logger.info("  섹터 분석: %d건", sector_count)
    logger.info("=" * 60)


if __name__ == "__main__":
    run_backfill()
