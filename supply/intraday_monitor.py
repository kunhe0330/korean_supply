"""
장중 수급 모니터링 — 하루 4회 폴링
09:40, 11:30, 13:30, 14:40 실행
"""

import logging
from datetime import datetime

from db.migrations import get_connection
from kis.api import (
    fetch_investor_trend_estimate,
    fetch_foreign_institution_total,
    fetch_volume_power_ranking,
)
from supply.collector import _safe_int

logger = logging.getLogger(__name__)

# 시간대 슬롯 매핑: 폴링 시각 → 예상 API 데이터 슬롯
POLLING_SLOTS = {
    "09:40": "1",  # 09:30 입력분
    "11:30": "3",  # 11:20 입력분
    "13:30": "4",  # 13:20 입력분
    "14:40": "5",  # 14:30 입력분
}


def get_polling_targets() -> list[str]:
    """
    장중 폴링 대상 종목 선정.
    1. 전일 수급 스코어 상위 50종목
    2. VDU 감지 종목
    3. 주도 섹터 소속 종목
    """
    conn = get_connection()
    try:
        yesterday = conn.execute(
            "SELECT MAX(calc_date) as d FROM supply_score"
        ).fetchone()
        if not yesterday or not yesterday["d"]:
            return []

        last_date = yesterday["d"]

        rows = conn.execute(
            """SELECT stock_code FROM supply_score
               WHERE calc_date = ?
               ORDER BY score_total DESC
               LIMIT 50""",
            (last_date,),
        ).fetchall()

        codes = set(r["stock_code"] for r in rows)

        # VDU 종목 추가
        vdu_rows = conn.execute(
            """SELECT stock_code FROM supply_score
               WHERE calc_date = ? AND vdu_flag = 1""",
            (last_date,),
        ).fetchall()
        codes.update(r["stock_code"] for r in vdu_rows)

        return sorted(codes)
    finally:
        conn.close()


def poll_intraday_supply() -> dict:
    """
    장중 추정 수급 수집 + 변화 감지.
    Returns: {"alerts": [...], "sector_rotation": [...]}
    """
    now = datetime.now()
    today = now.strftime("%Y%m%d")
    time_str = now.strftime("%H:%M")
    collected_at = now.strftime("%H:%M:%S")

    # 현재 시간에 해당하는 슬롯 결정
    current_slot = None
    for poll_time, slot in POLLING_SLOTS.items():
        if time_str >= poll_time:
            current_slot = slot
    if not current_slot:
        current_slot = "1"

    targets = get_polling_targets()
    if not targets:
        logger.warning("장중 폴링 대상 종목 없음")
        return {"alerts": [], "sector_rotation": []}

    logger.info("장중 폴링 시작: %d종목 (슬롯 %s)", len(targets), current_slot)

    conn = get_connection()
    alerts = []

    try:
        for code in targets:
            rows = fetch_investor_trend_estimate(code)
            if not rows:
                continue

            for row in rows:
                slot = row.get("bsop_hour_gb", "")
                if not slot:
                    continue
                frgn = _safe_int(row.get("frgn_fake_ntby_qty"))
                orgn = _safe_int(row.get("orgn_fake_ntby_qty"))
                total = _safe_int(row.get("sum_fake_ntby_qty"))

                conn.execute(
                    """INSERT OR REPLACE INTO intraday_supply
                       (stock_code, trade_date, time_slot,
                        frgn_est_net_qty, orgn_est_net_qty,
                        sum_est_net_qty, collected_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (code, today, slot, frgn, orgn, total, collected_at),
                )

            # 이전 시간대 대비 변화 감지
            alert = _check_stock_alert(conn, code, today, current_slot)
            if alert:
                alerts.append(alert)

        conn.commit()

        # 체결강도 스크리닝
        vp_alerts = _check_volume_power_screening(conn, today)
        alerts.extend(vp_alerts)

        # 섹터 로테이션 감지
        rotation = _check_sector_rotation(conn, today, current_slot)

    finally:
        conn.close()

    logger.info("장중 폴링 완료: 알림 %d건", len(alerts))
    return {"alerts": alerts, "sector_rotation": rotation}


def _check_stock_alert(conn, stock_code: str, today: str, current_slot: str) -> dict | None:
    """종목 단위 수급 급변 감지."""
    prev_slot = str(int(current_slot) - 1) if int(current_slot) > 1 else None
    if not prev_slot:
        return None  # 1차 폴링은 baseline

    prev = conn.execute(
        """SELECT frgn_est_net_qty, orgn_est_net_qty FROM intraday_supply
           WHERE stock_code = ? AND trade_date = ? AND time_slot = ?""",
        (stock_code, today, prev_slot),
    ).fetchone()

    curr = conn.execute(
        """SELECT frgn_est_net_qty, orgn_est_net_qty FROM intraday_supply
           WHERE stock_code = ? AND trade_date = ? AND time_slot = ?""",
        (stock_code, today, current_slot),
    ).fetchone()

    if not prev or not curr:
        return None

    prev_frgn = prev["frgn_est_net_qty"] or 0
    curr_frgn = curr["frgn_est_net_qty"] or 0
    prev_orgn = prev["orgn_est_net_qty"] or 0
    curr_orgn = curr["orgn_est_net_qty"] or 0

    # 외인 부호 전환
    if prev_frgn < 0 and curr_frgn > 0:
        name = conn.execute(
            "SELECT stock_name FROM stock_master WHERE stock_code = ?", (stock_code,)
        ).fetchone()
        return {
            "type": "SIGN_CHANGE",
            "stock_code": stock_code,
            "stock_name": name["stock_name"] if name else stock_code,
            "detail": f"외인 {prev_frgn:+,}→{curr_frgn:+,} (부호전환!)",
        }

    # 기관 2배 이상 증가
    if prev_orgn > 0 and curr_orgn >= prev_orgn * 2:
        name = conn.execute(
            "SELECT stock_name FROM stock_master WHERE stock_code = ?", (stock_code,)
        ).fetchone()
        return {
            "type": "ACCEL",
            "stock_code": stock_code,
            "stock_name": name["stock_name"] if name else stock_code,
            "detail": f"기관 {prev_orgn:+,}→{curr_orgn:+,} (가속)",
        }

    return None


def _check_volume_power_screening(conn, today: str) -> list[dict]:
    """체결강도 상위 30 스크리닝 — 기존 수급 TOP에 없던 종목 발견."""
    alerts = []
    for market_code in ["0001", "1001"]:
        items = fetch_volume_power_ranking(market_code)
        for item in items:
            code = item.get("stck_shrn_iscd", "")
            vp = float(item.get("tday_rltv", "0") or "0")
            if vp < 200:
                continue
            # 기존 수급 TOP에 있는지 확인
            existing = conn.execute(
                "SELECT 1 FROM supply_score WHERE stock_code = ? AND calc_date >= ?",
                (code, today),
            ).fetchone()
            if not existing:
                alerts.append({
                    "type": "VOL_POWER_SURGE",
                    "stock_code": code,
                    "stock_name": item.get("hts_kor_isnm", code),
                    "detail": f"체결강도 {vp:.0f}% ← 수급 TOP에 없던 종목!",
                })
    return alerts


def _check_sector_rotation(conn, today: str, current_slot: str) -> list[dict]:
    """섹터 로테이션 감지."""
    # 간단한 구현: 섹터별 순매수 합산 변화
    prev_slot = str(int(current_slot) - 1) if int(current_slot) > 1 else None
    if not prev_slot:
        return []

    sectors_prev = conn.execute(
        """SELECT stm.theme_id, SUM(ids.sum_est_net_qty) as total
           FROM intraday_supply ids
           JOIN stock_theme_map stm ON ids.stock_code = stm.stock_code
           WHERE ids.trade_date = ? AND ids.time_slot = ?
           GROUP BY stm.theme_id""",
        (today, prev_slot),
    ).fetchall()

    sectors_curr = conn.execute(
        """SELECT stm.theme_id, SUM(ids.sum_est_net_qty) as total
           FROM intraday_supply ids
           JOIN stock_theme_map stm ON ids.stock_code = stm.stock_code
           WHERE ids.trade_date = ? AND ids.time_slot = ?
           GROUP BY stm.theme_id""",
        (today, current_slot),
    ).fetchall()

    prev_map = {r["theme_id"]: r["total"] for r in sectors_prev}
    curr_map = {r["theme_id"]: r["total"] for r in sectors_curr}

    rotations = []
    for tid, curr_val in curr_map.items():
        prev_val = prev_map.get(tid, 0)
        if prev_val and curr_val:
            change = (curr_val - prev_val) / abs(prev_val) * 100 if prev_val else 0
            if change > 100:
                rotations.append({
                    "sector": tid,
                    "direction": "IN",
                    "change_pct": round(change, 1),
                })
            elif change < -50:
                rotations.append({
                    "sector": tid,
                    "direction": "OUT",
                    "change_pct": round(change, 1),
                })

    return rotations
