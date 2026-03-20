"""
텔레그램 알림 v3 — 장중 즉시 알림 + 일별 리포트
태그 기반 표시: ★ = 강화 태그 수
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


def _format_amount(amount_million: float) -> str:
    """백만원 단위를 억 단위로 변환."""
    if abs(amount_million) >= 100:
        return f"{amount_million / 100:+,.0f}억"
    return f"{amount_million:+,.0f}백만"


def _format_tags(tags_list: list, tag_count: int) -> str:
    """태그 + ★ 수 포맷."""
    if not tags_list:
        return ""
    tags_str = "·".join(tags_list)
    stars = "★" * tag_count
    return f"[{tags_str}] {stars}"


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

    sign_changes = [a for a in alerts if a["type"] == "SIGN_CHANGE"]
    accels = [a for a in alerts if a["type"] == "ACCEL"]
    vp_surges = [a for a in alerts if a["type"] == "VOL_POWER_SURGE"]

    if sign_changes or accels:
        lines.append("🔥 종목 수급 급변")
        for a in sign_changes + accels:
            lines.append(f"  {a['stock_name']}({a['stock_code']}): {a['detail']}")
        lines.append("")

    if vp_surges:
        lines.append("💪 체결강도 급등 (상위 30 스크리닝)")
        for a in vp_surges:
            lines.append(f"  {a['stock_name']}({a['stock_code']}): {a['detail']}")

    _send_telegram("\n".join(lines))


def send_daily_report(analysis_result: dict):
    """일별 수급 리포트 텔레그램 전송 (v3)."""
    calc_date = analysis_result.get("calc_date", "")
    leading = analysis_result.get("leading_sectors", [])
    all_sectors = analysis_result.get("all_sectors", [])
    stock_results = analysis_result.get("stock_results", [])

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
        net_str = _format_amount(sector.get("total_net_amount", 0))
        lines.append(f"{emoji} 주도 섹터 #{i + 1}: {sector['sector_name']} (순매수 {net_str})")

        top_stocks = sector.get("top_stocks", [])
        for j, s in enumerate(top_stocks[:3]):
            connector = "└" if j == len(top_stocks[:3]) - 1 else "├"
            code = s.get("code", "")
            name = s.get("name", "")

            # 태그 표시
            try:
                tags_list = json.loads(s.get("tags", "[]")) if isinstance(s.get("tags"), str) else (s.get("tags") or [])
            except (json.JSONDecodeError, TypeError):
                tags_list = []
            tag_count = s.get("tag_count", 0)
            tags_str = _format_tags(tags_list, tag_count)

            net_amt = _format_amount(s.get("net_amount", 0))
            vp = s.get("vol_power", 0)
            vp_str = f"체결{vp:.0f}%" if vp else ""
            rs = s.get("rs_1m")
            rs_str = f"RS{rs:+.1f}%" if rs else ""

            star = "⭐ " if tag_count >= 4 else ""
            detail_parts = [p for p in [vp_str, rs_str] if p]
            detail_str = " ".join(detail_parts)

            lines.append(f"{connector} {star}{name}({code}): {net_amt} {tags_str} {detail_str}")

        lines.append("")

    # 신규 부상 섹터
    new_sectors = [s for s in all_sectors if not s.get("is_leading") and s.get("supply_stock_count", 0) >= 2]
    new_sectors.sort(key=lambda x: -(x.get("total_net_amount") or 0))
    if new_sectors:
        s = new_sectors[0]
        net_str = _format_amount(s.get("total_net_amount", 0))
        lines.append(f"🆕 신규 부상: {s['sector_name']} (순매수 {net_str})")
        if s.get("top_stocks"):
            ts = s["top_stocks"][0]
            lines.append(f"└ {ts.get('name', '')}({ts.get('code', '')})")
        lines.append("")

    # 수급 이탈 주의
    exiting = [s for s in all_sectors if (s.get("total_net_amount") or 0) < -500]
    exiting.sort(key=lambda x: x.get("total_net_amount") or 0)
    if exiting:
        s = exiting[0]
        net_str = _format_amount(s.get("total_net_amount", 0))
        lines.append(f"📉 수급 이탈 주의: {s['sector_name']} (순매도 {net_str})")
        lines.append("")

    # 요약
    lines.append("━" * 20)
    total = len(stock_results)
    inflow_count = len([r for r in stock_results if r.get("is_inflow")])
    vp_high = len([r for r in stock_results if (r.get("vol_power_today") or 0) >= 150])
    rs_strong = len([r for r in stock_results if (r.get("rel_strength_1m") or 0) >= 5])

    lines.append(f"전체 스캔: {total}종목")
    lines.append(f"수급 유입: {inflow_count}개 / 체결강도 150%↑: {vp_high}개 / RS +5%↑: {rs_strong}개")
    lines.append("")
    lines.append("※ ★ = 강화 태그 수 (가속/손바뀜/체결강도↑/거래량↑/RS강함)")
    lines.append("  ★★★★~5: ⭐ 수급 집중 / ★★★: 강한 수급 / ★~2: 유입 확인")

    _send_telegram("\n".join(lines))


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
