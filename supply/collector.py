"""
수급 데이터 수집기
- 종목 마스터 수집 (KIS API 주식기본조회)
- 기관/외인 가집계 TOP 종목 수집
- 종목별 일별 투자자매매동향 수집 (6M 수급 구조 파악)
"""

import logging
from datetime import datetime, timedelta

from db.migrations import get_connection
from kis.api import (
    fetch_foreign_institution_total,
    fetch_investor_trade_daily,
    fetch_stock_info,
)

logger = logging.getLogger(__name__)

# KIS 업종 대분류 코드 → 한글 업종명 매핑
KRX_SECTOR_MAP = {
    "001": "음식료품",
    "002": "전기전자",
    "003": "의약품",
    "004": "비금속광물",
    "005": "철강금속",
    "006": "기계",
    "007": "화학",
    "008": "섬유의복",
    "009": "유통업",
    "010": "건설업",
    "011": "운수장비",
    "012": "운수창고",
    "013": "통신업",
    "014": "전기가스",
    "015": "은행",
    "016": "증권",
    "017": "보험",
    "018": "서비스업",
    "019": "종이목재",
    "020": "기타금융",
    "021": "기타제조",
    "022": "의료정밀",
    "024": "IT",
    "025": "반도체",
    "026": "소프트웨어",
    "027": "디지털컨텐츠",
    "028": "인터넷",
    "029": "바이오",
    "030": "게임",
    "031": "통신장비",
    "032": "정보기기",
    "033": "방송서비스",
    "034": "기타서비스",
}


# ── 종목 마스터 ──────────────────────────────────────────

def refresh_stock_master(stock_codes: list[str]):
    """
    KIS API 주식기본조회(CTPF1002R)로 종목 마스터 갱신.
    stock_codes: 갱신할 종목코드 리스트
    """
    conn = get_connection()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    updated = 0
    try:
        for code in stock_codes:
            info = fetch_stock_info(code)
            if not info:
                continue
            sector_large = info.get("idx_bztp_lcls_cd", "")
            sector_name = info.get("bstp_kor_isnm", "")
            # bstp_kor_isnm이 비어있으면 업종 대분류 코드로 한글명 매핑
            if not sector_name and sector_large:
                sector_name = KRX_SECTOR_MAP.get(sector_large, "")
            conn.execute(
                """INSERT OR REPLACE INTO stock_master
                   (stock_code, stock_name, market, sector_large, sector_medium,
                    sector_small, sector_name, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    code,
                    info.get("prdt_abrv_name", ""),
                    "KOSPI" if info.get("mket_id_cd") == "STK" else "KOSDAQ",
                    sector_large,
                    info.get("idx_bztp_mcls_cd", ""),
                    info.get("idx_bztp_scls_cd", ""),
                    sector_name,
                    now,
                ),
            )
            updated += 1
            if updated % 100 == 0:
                conn.commit()
                logger.info("종목 마스터 갱신 진행: %d/%d", updated, len(stock_codes))
        conn.commit()
        logger.info("종목 마스터 갱신 완료: %d건", updated)
    finally:
        conn.close()
    return updated


# ── 기관/외인 가집계 TOP 수집 ────────────────────────────

def collect_top_supply_stocks() -> list[str]:
    """
    국내기관_외국인 매매종목가집계(FHPTJ04400000) 4회 호출.
    코스피/코스닥 × 외국인/기관 순매수 TOP 종목 수집.
    Returns: 고유 종목코드 리스트 (중복 제거)
    """
    all_codes = set()
    combos = [
        ("0001", "1", "코스피·외국인"),
        ("0001", "2", "코스피·기관"),
        ("1001", "1", "코스닥·외국인"),
        ("1001", "2", "코스닥·기관"),
    ]

    conn = get_connection()
    today = datetime.now().strftime("%Y%m%d")
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        for market, investor, label in combos:
            items = fetch_foreign_institution_total(market, investor)
            logger.info("[%s] TOP 수집: %d건", label, len(items))
            for item in items:
                code = item.get("mksc_shrn_iscd", "").strip()
                if not code or len(code) != 6:
                    continue
                all_codes.add(code)

                # stock_master에 없으면 임시 등록
                name = item.get("hts_kor_isnm", "")
                mkt = "KOSPI" if market == "0001" else "KOSDAQ"
                conn.execute(
                    """INSERT OR IGNORE INTO stock_master
                       (stock_code, stock_name, market, updated_at)
                       VALUES (?, ?, ?, ?)""",
                    (code, name, mkt, now_str),
                )

        conn.commit()
        logger.info("가집계 TOP 고유 종목: %d개", len(all_codes))
    finally:
        conn.close()

    return sorted(all_codes)


# ── 종목별 일별 투자자 매매동향 수집 ──────────────────────

def _safe_int(val) -> int:
    """문자열을 int로 변환. 빈 값이면 0."""
    if val is None or val == "":
        return 0
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


def _safe_float(val) -> float:
    if val is None or val == "":
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def collect_investor_trade_daily(stock_codes: list[str], base_date: str = None):
    """
    종목별 투자자매매동향(일별) 수집 → daily_supply 테이블 저장.
    base_date: YYYYMMDD (기본: 오늘)
    """
    if base_date is None:
        base_date = datetime.now().strftime("%Y%m%d")

    conn = get_connection()
    total_saved = 0

    try:
        for i, code in enumerate(stock_codes):
            rows = fetch_investor_trade_daily(code, base_date)
            if not rows:
                continue

            for row in rows:
                trade_date = row.get("stck_bsop_date", "")
                if not trade_date:
                    continue
                conn.execute(
                    """INSERT OR REPLACE INTO daily_supply
                       (stock_code, trade_date, close_price, change_rate,
                        volume, trade_amount, frgn_net_qty, orgn_net_qty,
                        prsn_net_qty, frgn_net_amount, orgn_net_amount,
                        prsn_net_amount, scrt_net_qty, ivtr_net_qty,
                        bank_net_qty, insu_net_qty, fund_net_qty)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        code,
                        trade_date,
                        _safe_int(row.get("stck_clpr")),
                        _safe_float(row.get("prdy_ctrt")),
                        _safe_int(row.get("acml_vol")),
                        _safe_int(row.get("acml_tr_pbmn")),
                        _safe_int(row.get("frgn_ntby_qty")),
                        _safe_int(row.get("orgn_ntby_qty")),
                        _safe_int(row.get("prsn_ntby_qty")),
                        _safe_int(row.get("frgn_ntby_tr_pbmn")),
                        _safe_int(row.get("orgn_ntby_tr_pbmn")),
                        _safe_int(row.get("prsn_ntby_tr_pbmn")),
                        _safe_int(row.get("scrt_ntby_qty")),
                        _safe_int(row.get("ivtr_ntby_qty")),
                        _safe_int(row.get("bank_ntby_qty")),
                        _safe_int(row.get("insu_ntby_qty")),
                        _safe_int(row.get("fund_ntby_qty")),
                    ),
                )
                total_saved += 1

            if (i + 1) % 50 == 0:
                conn.commit()
                logger.info("투자자매매동향 수집 진행: %d/%d 종목", i + 1, len(stock_codes))

        conn.commit()
        logger.info("투자자매매동향 수집 완료: %d건 저장", total_saved)
    finally:
        conn.close()

    return total_saved


# ── 장 마감 후 메인 수집 배치 ─────────────────────────────

def run_daily_collection():
    """
    매일 15:35 실행되는 메인 수집 배치.
    Step 1: 가집계 TOP 종목 수집
    Step 2: 종목별 일별 투자자매매동향 수집
    (Step 3~4는 price_collector.py에서 처리)
    """
    logger.info("=== 일별 수급 수집 배치 시작 ===")

    # Step 1: 가집계 TOP 종목
    top_codes = collect_top_supply_stocks()
    if not top_codes:
        logger.error("가집계 TOP 종목 수집 실패 — 배치 중단")
        return []

    # Step 2: 종목별 투자자매매동향
    today = datetime.now().strftime("%Y%m%d")
    collect_investor_trade_daily(top_codes, today)

    # 신규 종목이면 마스터 정보 갱신
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT stock_code FROM stock_master WHERE sector_large IS NULL OR sector_large = ''"
        ).fetchall()
        new_codes = [r["stock_code"] for r in rows]
    finally:
        conn.close()

    if new_codes:
        logger.info("신규 종목 마스터 갱신: %d건", len(new_codes))
        refresh_stock_master(new_codes)

    logger.info("=== 일별 수급 수집 배치 완료 (종목 %d개) ===", len(top_codes))
    return top_codes
