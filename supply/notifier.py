"""
텔레그램 알림 — 장중 즉시 알림 + 일별 리포트
"""

import json
import logging
from datetime import datetime

import requests

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from db.migrations import get_connection

logger = logging.getLogger(__name__)


def _send_telegram(text: str):
    """텔레그램 메시지 전송."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("텔레그램 설정 없음 — 메시지 콘솔 출력:\n%s", text)
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    # 4096자 제한 분할 전송
    chunks = [text[i:i + 4000] for i in range(0, len(text), 4000)]
    for chunk in chunks:
        try:
            resp = requests.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": chunk,
                "parse_mode": "HTML",
            }, timeout=10)
            if resp.status_code != 200:
                logger.warning("텔레그램 전송 실패: %s", resp.text)
        except Exception as e:
            logger.error("텔레그램 전송 에러: %s", e)


def send_intraday_alert(alerts: list[dict], rotation: list[dict]):
    """장중 수급 변동 즉시 알림."""
    if not alerts and not rotation:
        return

    now = datetime.now().strftime("%H:%M")
    lines = [f"⚡ 장중 수급 변동 [{now} 기준]\n"]

    if rotation:
        lines.append("🔄 섹터 로테이션 감지!")
        for r in rotation:
            direction = "유입↑" if r["direction"] == "IN" else "이탈↓"
            lines.append(f"  {r['sector']}: {direction} ({r['change_pct']:+.0f}%)")
        lines.append("")

    # 알림 타입별 그룹핑
    sign_changes = [a for a in alerts if a["type"] == "SIGN_CHANGE"]
    accels = [a for a in alerts if a["type"] == "ACCEL"]
    vp_surges = [a for a in alerts if a["type"] == "VOL_POWER_SURGE"]
    vdu_alerts = [a for a in alerts if a["type"] == "VDU_INFLOW"]

    if sign_changes or accels:
        lines.append("🔥 종목 수급 급변")
        for a in sign_changes + accels:
            lines.append(f"  {a['stock_name']}({a['stock_code']}): {a['detail']}")
        lines.append("")

    if vp_surges:
        lines.append("💪 체결강도 급등 (상위 30 스크리닝)")
        for a in vp_surges:
            lines.append(f"  {a['stock_name']}({a['stock_code']}): {a['detail']}")
        lines.append("")

    if vdu_alerts:
        lines.append("⭐ VDU 종목 수급 유입!")
        for a in vdu_alerts:
            lines.append(f"  {a['stock_name']}({a['stock_code']}): {a['detail']}")

    _send_telegram("\n".join(lines))


def send_daily_report(analysis_result: dict):
    """일별 수급 리포트 텔레그램 전송."""
    calc_date = analysis_result.get("calc_date", "")
    leading = analysis_result.get("leading_sectors", [])
    all_sectors = analysis_result.get("all_sectors", [])
    stock_results = analysis_result.get("stock_results", [])

    # 날짜 포맷
    try:
        dt = datetime.strptime(calc_date, "%Y%m%d")
        weekday = ["월", "화", "수", "목", "금", "토", "일"][dt.weekday()]
        date_str = f"{dt.strftime('%Y-%m-%d')} {weekday}"
    except ValueError:
        date_str = calc_date

    lines = [f"📊 수급 분석 리포트 [{date_str}]\n"]

    # 주도 섹터
    for i, sector in enumerate(leading[:3]):
        emoji = "🔥" if i == 0 else "📈"
        lines.append(f"{emoji} 주도 섹터 #{i + 1}: {sector['sector_name']} (스코어 {sector['avg_score']:.0f})")

        top_stocks = sector.get("top_stocks", [])
        for j, s in enumerate(top_stocks[:3]):
            connector = "└" if j == len(top_stocks[:3]) - 1 else "├"
            code = s.get("code", "")
            name = s.get("name", "")
            score = s.get("score", 0)

            # 해당 종목 상세 정보
            detail = _find_stock_detail(stock_results, code)
            stage_str = _format_stage(detail)
            vp_str = f"체결강도{detail.get('vol_power_today', 0):.0f}%" if detail else ""

            star = "⭐ " if detail and detail.get("breakout_flag") else ""
            lines.append(f"{connector} {star}{name}({code}): [{stage_str} {vp_str}]")

        lines.append("")

    # 신규 부상 섹터
    new_sectors = [s for s in all_sectors if not s.get("is_leading") and s.get("avg_score", 0) > 50]
    new_sectors.sort(key=lambda x: -x.get("avg_score", 0))
    if new_sectors:
        s = new_sectors[0]
        lines.append(f"🆕 신규 부상: {s['sector_name']} (스코어 {s['avg_score']:.0f})")
        if s.get("top_stocks"):
            ts = s["top_stocks"][0]
            lines.append(f"└ {ts.get('name', '')}({ts.get('code', '')})")
        lines.append("")

    # 완성형 패턴
    perfect = [r for r in stock_results
               if r.get("score_total", 0) >= 75
               and r.get("stage") == "BREAKOUT"
               and r.get("vol_power_today", 0) >= 150]
    if perfect:
        lines.append("⭐ 완성형 패턴 (수급+체결강도+VDU+Breakout):")
        for p in perfect[:3]:
            name = _get_stock_name(p["stock_code"])
            rs_str = f"지수대비{p.get('rel_strength_1m', 0):+.1f}%" if p.get("rel_strength_1m") else ""
            lines.append(
                f"└ {name}({p['stock_code']}) — "
                f"점수{p['score_total']:.0f}+RS{p.get('rel_strength_bonus', 0):.0f} / "
                f"체결강도{p.get('vol_power_today', 0):.0f}% / {rs_str}"
            )
        lines.append("")

    # 요약
    lines.append("━" * 20)
    total = len(stock_results)
    supply_in = len([r for r in stock_results if r.get("score_total", 0) >= 50])
    vp_high = len([r for r in stock_results if r.get("vol_power_today", 0) >= 150])
    vdu_count = len([r for r in stock_results if r.get("vdu_flag")])
    bo_count = len([r for r in stock_results if r.get("breakout_flag")])

    lines.append(f"전체 스캔: {total}종목")
    lines.append(f"수급 유입: {supply_in}개 / 체결강도 150%↑: {vp_high}개 / VDU: {vdu_count}개 / Breakout: {bo_count}개")

    _send_telegram("\n".join(lines))


def _find_stock_detail(results: list, code: str) -> dict:
    for r in results:
        if r.get("stock_code") == code:
            return r
    return {}


def _format_stage(detail: dict) -> str:
    if not detail:
        return ""
    stage = detail.get("stage", "NONE")
    parts = []
    if stage == "BREAKOUT":
        vol_exp = detail.get("breakout_detail", {}).get("volume_expansion", 0)
        parts.append(f"BREAKOUT! 거래량×{vol_exp:.1f}")
    elif stage == "VDU":
        parts.append("VDU")
    elif stage == "PULLBACK":
        parts.append("PULLBACK·수급유지")
    elif stage == "BIG_MOVE":
        parts.append("BIG MOVE")

    if detail.get("handover") and "HANDOVER" in detail.get("handover", ""):
        parts.append("손바뀜")
    if detail.get("acceleration") in ("FULL_ACCEL", "SHORT_ACCEL"):
        parts.append("가속↑")

    return "·".join(parts)


def _get_stock_name(stock_code: str) -> str:
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT stock_name FROM stock_master WHERE stock_code = ?",
            (stock_code,),
        ).fetchone()
        return row["stock_name"] if row else stock_code
    finally:
        conn.close()
