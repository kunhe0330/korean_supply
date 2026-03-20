"""
가격 데이터 수집기
- 종목별 OHLCV (일봉)
- 종목별 매수매도체결량 (체결강도 계산용)
- 지수 일별 데이터 (상대강도 계산용)
"""

import logging
from datetime import datetime, timedelta

from db.migrations import get_connection
from kis.api import (
    fetch_daily_chart_price,
    fetch_daily_trade_volume,
    fetch_index_daily_price,
)
from supply.collector import _safe_int, _safe_float

logger = logging.getLogger(__name__)


def collect_ohlcv(stock_codes: list[str], start_date: str, end_date: str):
    """
    종목별 OHLCV 수집 → price_daily 테이블 저장.
    한 번에 최대 100거래일 조회.
    """
    conn = get_connection()
    total_saved = 0

    try:
        for i, code in enumerate(stock_codes):
            rows = fetch_daily_chart_price(code, start_date, end_date)
            if not rows:
                continue

            for row in rows:
                trade_date = row.get("stck_bsop_date", "")
                if not trade_date:
                    continue
                conn.execute(
                    """INSERT OR REPLACE INTO price_daily
                       (stock_code, trade_date, open_price, high_price,
                        low_price, close_price, volume, trade_amount, change_rate)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        code,
                        trade_date,
                        _safe_int(row.get("stck_oprc")),
                        _safe_int(row.get("stck_hgpr")),
                        _safe_int(row.get("stck_lwpr")),
                        _safe_int(row.get("stck_clpr")),
                        _safe_int(row.get("acml_vol")),
                        _safe_int(row.get("acml_tr_pbmn")),
                        _safe_float(row.get("prdy_vrss_sign", "0")),
                    ),
                )
                total_saved += 1

            if (i + 1) % 50 == 0:
                conn.commit()
                logger.info("OHLCV 수집 진행: %d/%d 종목", i + 1, len(stock_codes))

        conn.commit()
        logger.info("OHLCV 수집 완료: %d건 저장", total_saved)
    finally:
        conn.close()

    return total_saved


def collect_trade_volume(stock_codes: list[str], start_date: str, end_date: str):
    """
    종목별일별매수매도체결량 수집 → daily_supply 테이블의
    buy_vol, sell_vol, vol_power 업데이트.
    """
    conn = get_connection()
    total_updated = 0

    try:
        for i, code in enumerate(stock_codes):
            rows = fetch_daily_trade_volume(code, start_date, end_date)
            if not rows:
                continue

            for row in rows:
                trade_date = row.get("stck_bsop_date", "")
                if not trade_date:
                    continue
                buy_vol = _safe_int(row.get("total_shnu_qty"))
                sell_vol = _safe_int(row.get("total_seln_qty"))
                vol_power = (buy_vol / sell_vol * 100) if sell_vol > 0 else 0.0

                conn.execute(
                    """UPDATE daily_supply
                       SET buy_vol = ?, sell_vol = ?, vol_power = ?
                       WHERE stock_code = ? AND trade_date = ?""",
                    (buy_vol, sell_vol, round(vol_power, 2), code, trade_date),
                )
                total_updated += 1

            if (i + 1) % 50 == 0:
                conn.commit()
                logger.info("체결량 수집 진행: %d/%d 종목", i + 1, len(stock_codes))

        conn.commit()
        logger.info("체결량 수집 완료: %d건 업데이트", total_updated)
    finally:
        conn.close()

    return total_updated


def collect_index_daily(start_date: str):
    """
    코스피/코스닥 지수 일별 데이터 수집 → index_daily 테이블.
    """
    conn = get_connection()
    total_saved = 0
    indices = [
        ("0001", "KOSPI"),
        ("1001", "KOSDAQ"),
    ]

    try:
        for code, market_name in indices:
            rows = fetch_index_daily_price(code, start_date)
            if not rows:
                logger.warning("%s 지수 데이터 없음", market_name)
                continue

            for row in rows:
                trade_date = row.get("stck_bsop_date", "")
                if not trade_date:
                    continue
                conn.execute(
                    """INSERT OR REPLACE INTO index_daily
                       (market, trade_date, index_close, change_rate)
                       VALUES (?, ?, ?, ?)""",
                    (
                        market_name,
                        trade_date,
                        _safe_int(row.get("bstp_nmix_prpr")),
                        _safe_float(row.get("bstp_nmix_prdy_ctrt")),
                    ),
                )
                total_saved += 1

        conn.commit()
        logger.info("지수 데이터 수집 완료: %d건", total_saved)
    finally:
        conn.close()

    return total_saved


def run_price_collection(stock_codes: list[str]):
    """
    장 마감 후 가격 데이터 수집 배치.
    collector.py의 run_daily_collection() 이후 호출.
    """
    logger.info("=== 가격 데이터 수집 배치 시작 ===")

    today = datetime.now().strftime("%Y%m%d")
    # 100거래일(약 5개월) 조회
    start = (datetime.now() - timedelta(days=150)).strftime("%Y%m%d")

    # Step 3: OHLCV
    collect_ohlcv(stock_codes, start, today)

    # Step 3.1: 매수매도체결량 (체결강도)
    collect_trade_volume(stock_codes, start, today)

    # Step 3.2: 지수 데이터
    collect_index_daily(start)

    logger.info("=== 가격 데이터 수집 배치 완료 ===")
