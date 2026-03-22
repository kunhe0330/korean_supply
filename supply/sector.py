"""
섹터/테마별 집계 + 주도 섹터 판별 v3
VDU/Breakout 제거 — is_inflow 기반 판별
"""

import json
import logging
from datetime import datetime

from db.migrations import get_connection
from config import SUPPLY_PARAMS

logger = logging.getLogger(__name__)


def aggregate_by_theme(calc_date: str) -> list[dict]:
    """
    stock_theme_map 기준 테마별 수급 집계.
    v3: is_inflow 기반 수급 유입 종목 카운트.
    """
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT
                 stm.theme_id,
                 tm.theme_name,
                 ss.stock_code,
                 ss.is_inflow,
                 ss.tags,
                 ss.tag_count,
                 ss.ref_score,
                 ss.net_1m,
                 ss.net_today_amount,
                 ss.acceleration_type,
                 ss.vol_power_today,
                 ss.rel_strength_1m,
                 sm.stock_name
               FROM supply_score ss
               JOIN stock_theme_map stm ON ss.stock_code = stm.stock_code
               LEFT JOIN theme_master tm ON stm.theme_id = tm.theme_id
               LEFT JOIN stock_master sm ON ss.stock_code = sm.stock_code
               WHERE ss.calc_date = ?
               ORDER BY stm.theme_id, ss.net_today_amount DESC""",
            (calc_date,),
        ).fetchall()

        if not rows:
            return _aggregate_by_krx_sector(conn, calc_date)

        themes: dict[str, dict] = {}
        for r in rows:
            tid = r["theme_id"]
            if tid not in themes:
                themes[tid] = {
                    "sector_code": tid,
                    "sector_name": r["theme_name"] or tid,
                    "sector_type": "THEME",
                    "stocks": [],
                    "seen_codes": set(),
                    "total_net_amount": 0,
                    "accel_count": 0,
                }
            # 같은 테마 내 종목 중복 방지
            if r["stock_code"] in themes[tid]["seen_codes"]:
                continue
            themes[tid]["seen_codes"].add(r["stock_code"])
            themes[tid]["stocks"].append(dict(r))
            themes[tid]["total_net_amount"] += r["net_today_amount"] or 0
            if r["acceleration_type"] in ("FULL_ACCEL", "SHORT_ACCEL", "REVERSAL"):
                themes[tid]["accel_count"] += 1

        result = []
        for tid, data in themes.items():
            stocks = data["stocks"]
            inflow_stocks = [s for s in stocks if s["is_inflow"]]
            scores = [s["ref_score"] for s in stocks if s["ref_score"]]

            # 중복 종목 제거 (같은 종목이 여러 테마 매핑으로 중복될 수 있음)
            seen_codes = set()
            top_5 = []
            for s in stocks:
                if s["stock_code"] in seen_codes:
                    continue
                seen_codes.add(s["stock_code"])
                top_5.append({
                    "code": s["stock_code"],
                    "name": s["stock_name"],
                    "ref_score": s["ref_score"],
                    "is_inflow": s["is_inflow"],
                    "tags": s["tags"],
                    "tag_count": s["tag_count"],
                    "net_amount": s["net_today_amount"],
                    "vol_power": s["vol_power_today"],
                    "rs_1m": s["rel_strength_1m"],
                })
                if len(top_5) >= 5:
                    break

            result.append({
                "sector_code": tid,
                "sector_name": data["sector_name"],
                "sector_type": "THEME",
                "total_net_amount": data["total_net_amount"],
                "supply_stock_count": len(inflow_stocks),
                "avg_score": round(sum(scores) / len(scores), 1) if scores else 0,
                "top_stocks": top_5,
                "total_stock_count": len(stocks),
                "accel_ratio": data["accel_count"] / len(stocks) if stocks else 0,
            })

        return result

    finally:
        conn.close()


def _aggregate_by_krx_sector(conn, calc_date: str) -> list[dict]:
    """KRX 표준 업종 기준 fallback 집계."""
    rows = conn.execute(
        """SELECT
             sm.sector_name,
             ss.stock_code,
             ss.is_inflow,
             ss.tags,
             ss.tag_count,
             ss.ref_score,
             ss.net_1m,
             ss.net_today_amount,
             ss.acceleration_type,
             ss.vol_power_today,
             ss.rel_strength_1m,
             sm.stock_name
           FROM supply_score ss
           JOIN stock_master sm ON ss.stock_code = sm.stock_code
           WHERE ss.calc_date = ?
           ORDER BY sm.sector_name, ss.net_today_amount DESC""",
        (calc_date,),
    ).fetchall()

    sectors: dict[str, list] = {}
    for r in rows:
        sname = r["sector_name"] or "기타"
        if sname not in sectors:
            sectors[sname] = []
        sectors[sname].append(dict(r))

    result = []
    for sname, stocks in sectors.items():
        inflow_stocks = [s for s in stocks if s["is_inflow"]]
        scores = [s["ref_score"] for s in stocks if s["ref_score"]]
        top_5 = [
            {
                "code": s["stock_code"],
                "name": s["stock_name"],
                "ref_score": s["ref_score"],
                "is_inflow": s["is_inflow"],
                "tags": s["tags"],
                "tag_count": s["tag_count"],
                "net_amount": s["net_today_amount"],
                "vol_power": s["vol_power_today"],
                "rs_1m": s["rel_strength_1m"],
            }
            for s in stocks[:5]
        ]
        result.append({
            "sector_code": sname,
            "sector_name": sname,
            "sector_type": "KRX",
            "total_net_amount": sum(s["net_today_amount"] or 0 for s in stocks),
            "supply_stock_count": len(inflow_stocks),
            "avg_score": round(sum(scores) / len(scores), 1) if scores else 0,
            "top_stocks": top_5,
            "total_stock_count": len(stocks),
            "accel_ratio": sum(1 for s in stocks if s["acceleration_type"] in ("FULL_ACCEL", "SHORT_ACCEL", "REVERSAL")) / len(stocks) if stocks else 0,
        })

    return result


def identify_leading_sectors(sector_list: list[dict]) -> list[dict]:
    """
    주도 섹터 판별 (3개 조건 중 2개 이상 충족).
    1. 수급 유입(is_inflow=1) 종목이 3개 이상
    2. 섹터 합산 순매수 양(+)
    3. 가속 태그 종목 비율 50% 이상
    """
    min_stocks = SUPPLY_PARAMS["leading_sector_min_stocks"]
    min_accel = SUPPLY_PARAMS["leading_sector_accel_ratio"]

    for sector in sector_list:
        conditions_met = 0

        if sector["supply_stock_count"] >= min_stocks:
            conditions_met += 1
        if sector["total_net_amount"] > 0:
            conditions_met += 1
        if sector["accel_ratio"] >= min_accel:
            conditions_met += 1

        sector["is_leading"] = conditions_met >= 2

    leading = [s for s in sector_list if s["is_leading"]]
    leading.sort(key=lambda x: -(x.get("total_net_amount") or 0))

    for i, s in enumerate(leading):
        s["rank"] = i + 1

    return leading


def save_sector_analysis(sectors: list[dict], calc_date: str):
    """섹터 분석 결과 DB 저장."""
    conn = get_connection()
    try:
        for s in sectors:
            conn.execute(
                """INSERT OR REPLACE INTO sector_analysis
                   (sector_code, sector_name, sector_type, calc_date,
                    total_net_amount, supply_stock_count, avg_score,
                    top_stocks, is_leading, rank)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    s["sector_code"],
                    s["sector_name"],
                    s.get("sector_type", "THEME"),
                    calc_date,
                    s["total_net_amount"],
                    s["supply_stock_count"],
                    s["avg_score"],
                    json.dumps(s["top_stocks"], ensure_ascii=False),
                    1 if s.get("is_leading") else 0,
                    s.get("rank", 0),
                ),
            )
        conn.commit()
        logger.info("섹터 분석 저장: %d건", len(sectors))
    finally:
        conn.close()
