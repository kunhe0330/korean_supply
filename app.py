"""
KIS Supply-Demand Sector Analyzer — Flask 메인 앱
"""

import json
import logging
import os

from flask import Flask, jsonify, request, render_template

from db.migrations import init_db, get_connection
from scheduler.cron import init_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# 앱 시작 시 DB 초기화 + 스케줄러 시작
_scheduler = None


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


@app.route("/api/health")
def health():
    conn = get_connection()
    try:
        master = conn.execute("SELECT COUNT(*) as c FROM stock_master").fetchone()["c"]
        supply = conn.execute("SELECT COUNT(*) as c FROM daily_supply").fetchone()["c"]
        score = conn.execute("SELECT COUNT(*) as c FROM supply_score").fetchone()["c"]
        last_date = conn.execute("SELECT MAX(calc_date) as d FROM supply_score").fetchone()["d"]
    finally:
        conn.close()

    return jsonify({
        "status": "ok",
        "db": {
            "stock_master": master,
            "daily_supply": supply,
            "supply_score": score,
            "last_calc_date": last_date,
        },
    })


@app.route("/api/supply-report")
def supply_report():
    """
    수급 리포트 JSON 조회.
    Query params:
    - date: YYYYMMDD (default: 최신)
    - sector: 특정 섹터 필터
    """
    date = request.args.get("date")
    sector_filter = request.args.get("sector")

    conn = get_connection()
    try:
        if not date:
            row = conn.execute("SELECT MAX(calc_date) as d FROM supply_score").fetchone()
            date = row["d"] if row else None

        if not date:
            return jsonify({"error": "데이터 없음"}), 404

        # 수급 스코어 상위 종목
        query = """
            SELECT ss.*, sm.stock_name, sm.market
            FROM supply_score ss
            JOIN stock_master sm ON ss.stock_code = sm.stock_code
            WHERE ss.calc_date = ?
        """
        params = [date]

        if sector_filter:
            query += " AND ss.theme_list LIKE ?"
            params.append(f"%{sector_filter}%")

        query += " ORDER BY ss.score_total DESC"

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

        return jsonify({
            "date": date,
            "leading_sectors": leading,
            "supply_stocks": stocks[:50],
            "summary": {
                "total_stocks": len(stocks),
                "supply_in": len([s for s in stocks if (s.get("score_total") or 0) >= 50]),
                "vdu_count": len([s for s in stocks if s.get("vdu_flag")]),
                "breakout_count": len([s for s in stocks if s.get("breakout_flag")]),
            },
        })

    finally:
        conn.close()


@app.route("/api/supply-history/<stock_code>")
def supply_history(stock_code):
    """특정 종목의 수급 히스토리 + 스코어 변화."""
    conn = get_connection()
    try:
        # 종목 정보
        stock = conn.execute(
            "SELECT * FROM stock_master WHERE stock_code = ?", (stock_code,)
        ).fetchone()

        if not stock:
            return jsonify({"error": "종목 없음"}), 404

        # 스코어 히스토리 (최근 30일)
        scores = conn.execute(
            """SELECT * FROM supply_score
               WHERE stock_code = ?
               ORDER BY calc_date DESC LIMIT 30""",
            (stock_code,),
        ).fetchall()

        # 수급 히스토리 (최근 60일)
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
