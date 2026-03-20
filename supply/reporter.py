"""
주간/월간 리포트 생성 v3
"""

import json
import logging
from datetime import datetime, timedelta

from db.migrations import get_connection
from supply.notifier import _send_telegram, _format_amount

logger = logging.getLogger(__name__)


def generate_weekly_report():
    """매주 금요일 16:00 — 주간 수급 리포트."""
    conn = get_connection()
    try:
        today = datetime.now()
        week_ago = (today - timedelta(days=7)).strftime("%Y%m%d")
        week_start = (today - timedelta(days=4)).strftime("%m-%d")
        week_end = today.strftime("%m-%d")

        lines = [f"📊 주간 수급 리포트 [{today.strftime('%Y')}-{week_start} ~ {week_end}]\n"]

        # 주간 주도 섹터 변화
        lines.append("🏆 주간 주도 섹터 변화")

        current_sectors = conn.execute(
            """SELECT sector_name, avg_score, rank, is_leading, total_net_amount
               FROM sector_analysis
               WHERE calc_date = (SELECT MAX(calc_date) FROM sector_analysis)
               ORDER BY rank ASC""",
        ).fetchall()

        prev_sectors = conn.execute(
            """SELECT sector_name, avg_score, total_net_amount
               FROM sector_analysis
               WHERE calc_date = (SELECT MIN(calc_date) FROM sector_analysis
                                  WHERE calc_date >= ?)
               ORDER BY rank ASC""",
            (week_ago,),
        ).fetchall()

        prev_map = {r["sector_name"]: r["avg_score"] for r in prev_sectors}

        for s in current_sectors[:5]:
            name = s["sector_name"]
            curr_score = s["avg_score"] or 0
            prev_score = prev_map.get(name, 0) or 0
            diff = curr_score - prev_score
            trend = "▲" if diff > 0 else "▼" if diff < 0 else "─"
            net_str = _format_amount(s["total_net_amount"] or 0)
            leading = "🆕" if s["is_leading"] and name not in prev_map else ""
            lines.append(f"  {name}: {prev_score:.0f} → {curr_score:.0f} ({diff:+.0f}) {trend} {net_str} {leading}")

        lines.append("")

        # 주간 수급 요약
        lines.append("📊 주간 수급 요약")

        # 수급 유입 TOP
        top_stocks = conn.execute(
            """SELECT ss.stock_code, sm.stock_name, ss.ref_score, ss.tag_count,
                      ss.net_today_amount, ss.tags
               FROM supply_score ss
               JOIN stock_master sm ON ss.stock_code = sm.stock_code
               WHERE ss.calc_date = (SELECT MAX(calc_date) FROM supply_score)
                 AND ss.is_inflow = 1
               ORDER BY ss.net_today_amount DESC LIMIT 5""",
        ).fetchall()

        if top_stocks:
            lines.append("  수급 유입 TOP:")
            for s in top_stocks:
                stars = "★" * (s["tag_count"] or 0)
                net_str = _format_amount(s["net_today_amount"] or 0)
                lines.append(f"    {s['stock_name']}({s['stock_code']}): {net_str} {stars}")

        # 수급 유입 통계
        inflow_count = conn.execute(
            """SELECT COUNT(*) as cnt FROM supply_score
               WHERE calc_date >= ? AND is_inflow = 1""",
            (week_ago,),
        ).fetchone()

        vp_high = conn.execute(
            """SELECT COUNT(*) as cnt FROM supply_score
               WHERE calc_date >= ? AND vol_power_today >= 150""",
            (week_ago,),
        ).fetchone()

        lines.append(f"  주간 수급 유입: {inflow_count['cnt'] if inflow_count else 0}건 / "
                      f"체결강도 150%↑: {vp_high['cnt'] if vp_high else 0}건")

        _send_telegram("\n".join(lines))
        logger.info("주간 리포트 전송 완료")

    finally:
        conn.close()


def generate_monthly_report():
    """매월 마지막 영업일 16:00 — 월간 수급 리포트."""
    conn = get_connection()
    try:
        today = datetime.now()
        month_ago = (today - timedelta(days=30)).strftime("%Y%m%d")
        month_str = today.strftime("%Y년 %m월")

        lines = [f"📊 월간 수급 리포트 [{month_str}]\n"]

        # 월간 섹터별 수급 흐름
        lines.append("📈 월간 섹터 수급 흐름")

        sectors = conn.execute(
            """SELECT sector_name, avg_score, total_net_amount, is_leading, supply_stock_count
               FROM sector_analysis
               WHERE calc_date = (SELECT MAX(calc_date) FROM sector_analysis)
               ORDER BY total_net_amount DESC LIMIT 10""",
        ).fetchall()

        for s in sectors:
            emoji = "🔥" if s["is_leading"] else "  "
            net_str = _format_amount(s["total_net_amount"] or 0)
            lines.append(f"  {emoji} {s['sector_name']}: 유입 {s['supply_stock_count']}종목 / 순매수 {net_str}")

        lines.append("")

        # 월간 수급 집중 종목
        lines.append("⭐ 월간 수급 집중 종목 (태그 3개↑)")
        concentrations = conn.execute(
            """SELECT ss.stock_code, sm.stock_name, ss.ref_score,
                      ss.tag_count, ss.tags, ss.calc_date, ss.vol_power_today
               FROM supply_score ss
               JOIN stock_master sm ON ss.stock_code = sm.stock_code
               WHERE ss.calc_date >= ? AND ss.tag_count >= 3
               ORDER BY ss.tag_count DESC, ss.ref_score DESC LIMIT 10""",
            (month_ago,),
        ).fetchall()

        if concentrations:
            for b in concentrations:
                stars = "★" * (b["tag_count"] or 0)
                lines.append(
                    f"  {b['stock_name']}({b['stock_code']}): "
                    f"{stars} 체결강도 {b['vol_power_today']:.0f}% "
                    f"({b['calc_date']})"
                )
        else:
            lines.append("  해당 없음")

        _send_telegram("\n".join(lines))
        logger.info("월간 리포트 전송 완료")

    finally:
        conn.close()
