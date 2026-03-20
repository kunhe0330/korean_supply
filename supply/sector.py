"""
섹터/테마별 집계 + 주도 섹터 판별
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
    한 종목이 여러 테마에 속할 수 있으므로 중복 카운트 허용.
    """
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT
                 stm.theme_id,
                 tm.theme_name,
                 ss.stock_code,
                 ss.score_total,
                 ss.net_1m,
                 ss.acceleration_flag,
                 ss.vdu_flag,
                 ss.breakout_flag,
                 ss.vol_power_today,
                 ss.stage,
                 sm.stock_name
               FROM supply_score ss
               JOIN stock_theme_map stm ON ss.stock_code = stm.stock_code
               LEFT JOIN theme_master tm ON stm.theme_id = tm.theme_id
               LEFT JOIN stock_master sm ON ss.stock_code = sm.stock_code
               WHERE ss.calc_date = ?
               ORDER BY stm.theme_id, ss.score_total DESC""",
            (calc_date,),
        ).fetchall()

        if not rows:
            # Fallback: KRX 표준 업종으로 집계
            return _aggregate_by_krx_sector(conn, calc_date)

        # 테마별 그룹핑
        themes: dict[str, dict] = {}
        for r in rows:
            tid = r["theme_id"]
            if tid not in themes:
                themes[tid] = {
                    "sector_code": tid,
                    "sector_name": r["theme_name"] or tid,
                    "sector_type": "THEME",
                    "stocks": [],
                    "total_net_amount": 0,
                    "accel_count": 0,
                }
            themes[tid]["stocks"].append(dict(r))
            themes[tid]["total_net_amount"] += r["net_1m"] or 0
            if r["acceleration_flag"]:
                themes[tid]["accel_count"] += 1

        result = []
        for tid, data in themes.items():
            stocks = data["stocks"]
            supply_stocks = [s for s in stocks if (s["score_total"] or 0) >= 50]
            scores = [s["score_total"] for s in stocks if s["score_total"]]

            top_5 = [
                {"code": s["stock_code"], "name": s["stock_name"], "score": s["score_total"]}
                for s in stocks[:5]
            ]

            result.append({
                "sector_code": tid,
                "sector_name": data["sector_name"],
                "sector_type": "THEME",
                "total_net_amount": data["total_net_amount"],
                "supply_stock_count": len(supply_stocks),
                "avg_score": round(sum(scores) / len(scores), 1) if scores else 0,
                "vdu_count": sum(1 for s in stocks if s["vdu_flag"]),
                "breakout_count": sum(1 for s in stocks if s["breakout_flag"]),
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
             ss.score_total,
             ss.net_1m,
             ss.acceleration_flag,
             ss.vdu_flag,
             ss.breakout_flag,
             sm.stock_name
           FROM supply_score ss
           JOIN stock_master sm ON ss.stock_code = sm.stock_code
           WHERE ss.calc_date = ?
           ORDER BY sm.sector_name, ss.score_total DESC""",
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
        supply_stocks = [s for s in stocks if (s["score_total"] or 0) >= 50]
        scores = [s["score_total"] for s in stocks if s["score_total"]]
        top_5 = [
            {"code": s["stock_code"], "name": s["stock_name"], "score": s["score_total"]}
            for s in stocks[:5]
        ]
        result.append({
            "sector_code": sname,
            "sector_name": sname,
            "sector_type": "KRX",
            "total_net_amount": sum(s["net_1m"] or 0 for s in stocks),
            "supply_stock_count": len(supply_stocks),
            "avg_score": round(sum(scores) / len(scores), 1) if scores else 0,
            "vdu_count": sum(1 for s in stocks if s["vdu_flag"]),
            "breakout_count": sum(1 for s in stocks if s["breakout_flag"]),
            "top_stocks": top_5,
            "total_stock_count": len(stocks),
            "accel_ratio": sum(1 for s in stocks if s["acceleration_flag"]) / len(stocks) if stocks else 0,
        })

    return result


def identify_leading_sectors(sector_list: list[dict]) -> list[dict]:
    """
    주도 섹터 판별 (3개 조건 중 2개 이상 충족).
    1. 수급 스코어 60+ 종목이 3개 이상
    2. 섹터 합산 순매수 양(+)
    3. 가속 종목 비율 50% 이상
    """
    min_stocks = SUPPLY_PARAMS["leading_sector_min_stocks"]
    min_accel = SUPPLY_PARAMS["leading_sector_accel_ratio"]

    for sector in sector_list:
        conditions_met = 0
        supply_60 = sum(
            1 for s in sector.get("top_stocks", [])
            if (s.get("score") or 0) >= 60
        )
        # 조건 확인은 supply_stock_count 기준
        if sector["supply_stock_count"] >= min_stocks:
            conditions_met += 1
        if sector["total_net_amount"] > 0:
            conditions_met += 1
        if sector["accel_ratio"] >= min_accel:
            conditions_met += 1

        # 보너스
        bonus = 0
        if sector["vdu_count"] > 0 or sector["breakout_count"] > 0:
            bonus += 5
        sector["leading_score"] = sector["avg_score"] + bonus
        sector["is_leading"] = conditions_met >= 2

    # 정렬: 주도 섹터 우선, 그 안에서 점수순
    leading = [s for s in sector_list if s["is_leading"]]
    leading.sort(key=lambda x: -x["leading_score"])

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
                    vdu_count, breakout_count, top_stocks,
                    is_leading, rank)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    s["sector_code"],
                    s["sector_name"],
                    s.get("sector_type", "THEME"),
                    calc_date,
                    s["total_net_amount"],
                    s["supply_stock_count"],
                    s["avg_score"],
                    s["vdu_count"],
                    s["breakout_count"],
                    json.dumps(s["top_stocks"], ensure_ascii=False),
                    1 if s.get("is_leading") else 0,
                    s.get("rank", 0),
                ),
            )
        conn.commit()
        logger.info("섹터 분석 저장: %d건", len(sectors))
    finally:
        conn.close()
