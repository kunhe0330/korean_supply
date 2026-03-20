"""
주간/월간 리포트 생성
"""

import json
import logging
from datetime import datetime, timedelta

from db.migrations import get_connection
from supply.notifier import _send_telegram

logger = logging.getLogger(__name__)


def generate_weekly_report():
    """매주 금요일 16:00 — 주간 수급 리포트."""
    conn = get_connection()
    try:
        today = datetime.now()
        week_ago = (today - timedelta(days=7)).strftime("%Y%m%d")
        today_str = today.strftime("%Y%m%d")
        week_start = (today - timedelta(days=4)).strftime("%m-%d")
        week_end = today.strftime("%m-%d")

        lines = [f"📊 주간 수급 리포트 [{today.strftime('%Y')}-{week_start} ~ {week_end}]\n"]

        # 주간 주도 섹터 변화
        lines.append("🏆 주간 주도 섹터 변화")

        # 이번 주 / 지난 주 섹터 분석 비교
        current_sectors = conn.execute(
            """SELECT sector_name, avg_score, rank, is_leading
               FROM sector_analysis
               WHERE calc_date = (SELECT MAX(calc_date) FROM sector_analysis)
               ORDER BY rank ASC""",
        ).fetchall()

        prev_sectors = conn.execute(
            """SELECT sector_name, avg_score
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
            leading = "🆕 신규 부상" if s["is_leading"] and name not in prev_map else ""
            lines.append(f"  {name}: {prev_score:.0f} → {curr_score:.0f} ({diff:+.0f}) {trend} {leading}")

        lines.append("")

        # 주간 수급 요약
        lines.append("📊 주간 수급 요약")

        # 수급 스코어 상위
        top_stocks = conn.execute(
            """SELECT ss.stock_code, sm.stock_name, ss.score_total, ss.net_1m
               FROM supply_score ss
               JOIN stock_master sm ON ss.stock_code = sm.stock_code
               WHERE ss.calc_date = (SELECT MAX(calc_date) FROM supply_score)
               ORDER BY ss.score_total DESC LIMIT 5""",
        ).fetchall()

        if top_stocks:
            lines.append("  수급 스코어 TOP:")
            for s in top_stocks:
                lines.append(f"    {s['stock_name']}({s['stock_code']}): 스코어 {s['score_total']:.0f}")

        # VDU / Breakout 통계
        vdu_count = conn.execute(
            """SELECT COUNT(*) as cnt FROM supply_score
               WHERE calc_date >= ? AND vdu_flag = 1""",
            (week_ago,),
        ).fetchone()

        bo_count = conn.execute(
            """SELECT COUNT(*) as cnt FROM supply_score
               WHERE calc_date >= ? AND breakout_flag = 1""",
            (week_ago,),
        ).fetchone()

        lines.append(f"  VDU 신규 진입: {vdu_count['cnt'] if vdu_count else 0}개 / "
                      f"Breakout 발생: {bo_count['cnt'] if bo_count else 0}개")

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
            """SELECT sector_name, avg_score, total_net_amount, is_leading
               FROM sector_analysis
               WHERE calc_date = (SELECT MAX(calc_date) FROM sector_analysis)
               ORDER BY avg_score DESC LIMIT 10""",
        ).fetchall()

        for s in sectors:
            emoji = "🔥" if s["is_leading"] else "  "
            net_str = f"{s['total_net_amount']:+,.0f}백만" if s["total_net_amount"] else "N/A"
            lines.append(f"  {emoji} {s['sector_name']}: 스코어 {s['avg_score']:.0f} / 순매수 {net_str}")

        lines.append("")

        # 완성형 패턴 발생 이력
        lines.append("⭐ 월간 완성형 패턴 (Breakout) 종목")
        breakouts = conn.execute(
            """SELECT ss.stock_code, sm.stock_name, ss.score_total,
                      ss.calc_date, ss.vol_power_today
               FROM supply_score ss
               JOIN stock_master sm ON ss.stock_code = sm.stock_code
               WHERE ss.calc_date >= ? AND ss.breakout_flag = 1
               ORDER BY ss.score_total DESC LIMIT 10""",
            (month_ago,),
        ).fetchall()

        if breakouts:
            for b in breakouts:
                lines.append(
                    f"  {b['stock_name']}({b['stock_code']}): "
                    f"스코어 {b['score_total']:.0f} / 체결강도 {b['vol_power_today']:.0f}% "
                    f"({b['calc_date']})"
                )
        else:
            lines.append("  해당 없음")

        _send_telegram("\n".join(lines))
        logger.info("월간 리포트 전송 완료")

    finally:
        conn.close()
