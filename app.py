"""
KIS Supply-Demand Sector Analyzer — Flask 메인 앱 v3
"""

import json
import logging
import os
from datetime import datetime, timezone, timedelta

from flask import Flask, jsonify, request, render_template

KST = timezone(timedelta(hours=9))

from db.migrations import init_db, get_connection
from scheduler.cron import init_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

_scheduler = None
_last_analysis_time = None  # 마지막 분석 완료 시간


@app.before_request
def _init_once():
    """첫 요청 시 DB 초기화 + 스케줄러 시작 (1회만)."""
    global _scheduler
    if _scheduler is None:
        init_db()
        _scheduler = init_scheduler()
        logger.info("앱 초기화 완료")


@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/run-backfill", methods=["POST"])
def run_backfill_api():
    """Railway 서버에서 backfill 실행 (1회용)."""
    import threading
    from backfill import run_backfill

    def _run():
        try:
            run_backfill()
        except Exception as e:
            logger.error("Backfill 실패: %s", e)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return jsonify({"status": "backfill started", "message": "백그라운드에서 실행 중. /api/health로 진행 확인."})


@app.route("/api/rebuild-sectors", methods=["POST"])
def rebuild_sectors_api():
    """특정 날짜의 섹터 분석만 재집계."""
    from supply.sector import aggregate_by_theme, identify_leading_sectors, save_sector_analysis
    date = request.args.get("date")
    if not date:
        conn = get_connection()
        try:
            row = conn.execute("SELECT MAX(calc_date) as d FROM supply_score").fetchone()
            date = row["d"] if row else None
        finally:
            conn.close()
    if not date:
        return jsonify({"error": "데이터 없음"}), 404
    sector_list = aggregate_by_theme(date)
    leading = identify_leading_sectors(sector_list)
    save_sector_analysis(sector_list, date)
    return jsonify({"status": "ok", "date": date, "sectors": len(sector_list), "leading": len(leading)})


@app.route("/api/health")
def health():
    conn = get_connection()
    try:
        master = conn.execute("SELECT COUNT(*) as c FROM stock_master").fetchone()["c"]
        supply = conn.execute("SELECT COUNT(*) as c FROM daily_supply").fetchone()["c"]
        score = conn.execute("SELECT COUNT(*) as c FROM supply_score").fetchone()["c"]
        inflow = conn.execute("SELECT COUNT(*) as c FROM supply_score WHERE is_inflow = 1").fetchone()["c"]
        last_date = conn.execute("SELECT MAX(calc_date) as d FROM supply_score").fetchone()["d"]
    finally:
        conn.close()

    return jsonify({
        "status": "ok",
        "db": {
            "stock_master": master,
            "daily_supply": supply,
            "supply_score": score,
            "inflow_count": inflow,
            "last_calc_date": last_date,
        },
    })


@app.route("/api/supply-report")
def supply_report():
    """
    수급 리포트 JSON 조회 (v3).
    기본 정렬: 기관+외인 순매수 금액 내림차순.
    """
    date = request.args.get("date")
    sector_filter = request.args.get("sector")
    sort_by = request.args.get("sort", "amount")  # amount / ref_score / vol_power / rs

    conn = get_connection()
    try:
        if not date:
            row = conn.execute("SELECT MAX(calc_date) as d FROM supply_score").fetchone()
            date = row["d"] if row else None

        if not date:
            return jsonify({"error": "데이터 없음"}), 404

        query = """
            SELECT ss.*, sm.stock_name, sm.market, sm.sector_code, sm.sector_name
            FROM supply_score ss
            JOIN stock_master sm ON ss.stock_code = sm.stock_code
            WHERE ss.calc_date = ?
        """
        params = [date]

        if sector_filter:
            query += " AND ss.theme_list LIKE ?"
            params.append(f"%{sector_filter}%")

        # 정렬
        order_map = {
            "amount": "ss.net_today_amount DESC",
            "ref_score": "ss.ref_score DESC",
            "vol_power": "ss.vol_power_today DESC",
            "rs": "ss.rel_strength_1m DESC",
            "tags": "ss.tag_count DESC, ss.net_today_amount DESC",
        }
        query += f" ORDER BY {order_map.get(sort_by, order_map['amount'])}"

        rows = conn.execute(query, params).fetchall()
        stocks = [dict(r) for r in rows]

        # 주도 섹터
        sectors = conn.execute(
            """SELECT * FROM sector_analysis
               WHERE calc_date = ?
               ORDER BY is_leading DESC, rank ASC""",
            (date,),
        ).fetchall()

        leading = [dict(s) for s in sectors if s["is_leading"]]
        for s in leading:
            if s.get("top_stocks"):
                s["top_stocks"] = json.loads(s["top_stocks"])

        # 요약 통계
        inflow_count = len([s for s in stocks if s.get("is_inflow")])
        vp_high = len([s for s in stocks if (s.get("vol_power_today") or 0) >= 150])
        rs_strong = len([s for s in stocks if (s.get("rel_strength_1m") or 0) >= 5])

        # 다음 갱신 시간 계산 (KST 기준)
        POLL_TIMES = ["09:40", "11:30", "13:30", "14:40", "15:35"]
        now_kst = datetime.now(KST)
        now_hm = now_kst.strftime("%H:%M")
        next_refresh = None
        for pt in POLL_TIMES:
            if pt > now_hm:
                next_refresh = pt
                break
        if not next_refresh:
            # 오늘 모든 갱신 완료 → 다음 영업일 첫 갱신
            next_refresh = "내일 09:40"

        # last_updated: DB에서 가장 최근 calc_date 기반
        last_updated_str = None
        if _last_analysis_time:
            last_updated_str = _last_analysis_time
        elif date:
            # DB 날짜 기반 표시
            d = date
            if len(d) == 8:
                last_updated_str = f"{d[:4]}년 {d[4:6]}월 {d[6:8]}일"

        return jsonify({
            "date": date,
            "leading_sectors": leading,
            "supply_stocks": stocks[:50],
            "summary": {
                "total_stocks": len(stocks),
                "inflow_count": inflow_count,
                "vp_high_count": vp_high,
                "rs_strong_count": rs_strong,
            },
            "last_updated": last_updated_str,
            "next_refresh": next_refresh,
        })

    finally:
        conn.close()


@app.route("/api/supply-history/<stock_code>")
def supply_history(stock_code):
    """특정 종목의 수급 히스토리 + 스코어 변화."""
    conn = get_connection()
    try:
        stock = conn.execute(
            "SELECT * FROM stock_master WHERE stock_code = ?", (stock_code,)
        ).fetchone()

        if not stock:
            return jsonify({"error": "종목 없음"}), 404

        scores = conn.execute(
            """SELECT * FROM supply_score
               WHERE stock_code = ?
               ORDER BY calc_date DESC LIMIT 30""",
            (stock_code,),
        ).fetchall()

        supply = conn.execute(
            """SELECT * FROM daily_supply
               WHERE stock_code = ?
               ORDER BY trade_date DESC LIMIT 60""",
            (stock_code,),
        ).fetchall()

        return jsonify({
            "stock": dict(stock),
            "score_history": [dict(s) for s in scores],
            "supply_history": [dict(s) for s in supply],
        })

    finally:
        conn.close()


@app.route("/api/leading-sectors")
def leading_sectors():
    """주도 섹터 목록."""
    conn = get_connection()
    try:
        date = request.args.get("date")
        if not date:
            row = conn.execute("SELECT MAX(calc_date) as d FROM sector_analysis").fetchone()
            date = row["d"] if row else None

        if not date:
            return jsonify({"error": "데이터 없음"}), 404

        sectors = conn.execute(
            """SELECT * FROM sector_analysis
               WHERE calc_date = ? AND is_leading = 1
               ORDER BY rank ASC""",
            (date,),
        ).fetchall()

        result = []
        for s in sectors:
            d = dict(s)
            if d.get("top_stocks"):
                d["top_stocks"] = json.loads(d["top_stocks"])
            result.append(d)

        return jsonify({"date": date, "leading_sectors": result})

    finally:
        conn.close()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
