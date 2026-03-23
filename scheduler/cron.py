"""
APScheduler 스케줄 관리
장중 4회 폴링 + 장 마감 배치 + 주간/월간 리포트
"""

import logging
from datetime import datetime, timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)


def _is_weekday():
    """평일 여부 확인."""
    return datetime.now().weekday() < 5


def job_intraday_poll():
    """장중 수급 폴링 (09:40, 11:30, 13:30, 14:40) + supply_score 갱신."""
    if not _is_weekday():
        return

    from supply.intraday_monitor import poll_intraday_supply
    from supply.notifier import send_intraday_alert

    logger.info("장중 폴링 실행: %s", datetime.now().strftime("%H:%M"))
    result = poll_intraday_supply()
    if result["alerts"] or result["sector_rotation"]:
        send_intraday_alert(result["alerts"], result["sector_rotation"])

    # 장중에도 대시보드 업데이트: 수급 데이터 수집 + 분석
    try:
        _run_intraday_analysis()
    except Exception as e:
        logger.warning("장중 분석 실패 (무시): %s", e)


def _run_intraday_analysis():
    """장중 수급 데이터 수집 + 분석 실행 (대시보드 갱신용)."""
    from supply.collector import collect_top_supply_stocks, collect_investor_trade_daily, refresh_stock_master
    from supply.price_collector import collect_ohlcv, collect_index_daily
    from supply.analyzer import run_analysis
    from supply.theme_mapper import run_theme_update

    today = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=180)).strftime("%Y%m%d")

    # 1. TOP 종목 수집
    top_codes = collect_top_supply_stocks()
    if not top_codes:
        logger.warning("장중 분석: TOP 종목 없음")
        return

    logger.info("장중 분석 시작: %d종목", len(top_codes))

    # 2. 종목 마스터 갱신 (신규 종목만)
    from db.migrations import get_connection
    conn = get_connection()
    try:
        existing = set(r["stock_code"] for r in
                       conn.execute("SELECT stock_code FROM stock_master WHERE sector_large IS NOT NULL AND sector_large != ''").fetchall())
    finally:
        conn.close()
    new_codes = [c for c in top_codes if c not in existing]
    if new_codes:
        refresh_stock_master(new_codes)

    # 3. 수급 데이터 수집
    collect_investor_trade_daily(top_codes, today)

    # 4. OHLCV + 지수 수집
    collect_ohlcv(top_codes, start_date, today)
    collect_index_daily(start_date)

    # 5. 테마 매핑 (캐시 있으면 빠름)
    try:
        run_theme_update()
    except Exception as e:
        logger.warning("테마 매핑 실패 (무시): %s", e)

    # 6. 분석 실행
    result = run_analysis(top_codes)
    inflow_count = sum(1 for r in result.get("stock_results", []) if r.get("is_inflow"))
    leading_count = len(result.get("leading_sectors", []))
    logger.info("장중 분석 완료: 수급유입 %d개, 주도섹터 %d개", inflow_count, leading_count)

    # 앱에 마지막 분석 시간 기록
    try:
        import app as app_module
        app_module._last_analysis_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    except Exception:
        pass


def job_daily_batch():
    """장 마감 후 메인 배치 (15:35)."""
    if not _is_weekday():
        return

    from supply.collector import run_daily_collection
    from supply.price_collector import run_price_collection
    from supply.analyzer import run_analysis

    logger.info("일별 배치 시작")
    top_codes = run_daily_collection()
    if top_codes:
        run_price_collection(top_codes)
        run_analysis(top_codes)
        try:
            import app as app_module
            app_module._last_analysis_time = datetime.now().strftime("%Y-%m-%d %H:%M")
        except Exception:
            pass


def job_daily_report():
    """일별 리포트 발송 (15:50)."""
    if not _is_weekday():
        return

    from supply.analyzer import run_analysis
    from supply.notifier import send_daily_report
    from db.migrations import get_connection

    conn = get_connection()
    try:
        today = datetime.now().strftime("%Y%m%d")
        # 오늘 분석 결과 조회
        rows = conn.execute(
            """SELECT stock_code FROM supply_score
               WHERE calc_date = ?""",
            (today,),
        ).fetchall()

        if rows:
            # 이미 분석 완료된 결과를 기반으로 리포트 생성
            from supply.sector import aggregate_by_theme, identify_leading_sectors

            all_sectors = aggregate_by_theme(today)
            leading = identify_leading_sectors(all_sectors)

            stock_results = []
            for r in conn.execute(
                "SELECT * FROM supply_score WHERE calc_date = ?", (today,)
            ).fetchall():
                stock_results.append(dict(r))

            send_daily_report({
                "calc_date": today,
                "stock_results": stock_results,
                "all_sectors": all_sectors,
                "leading_sectors": leading,
            })
        else:
            logger.warning("오늘 분석 결과 없음 — 리포트 미발송")
    finally:
        conn.close()


def job_weekly_report():
    """주간 리포트 (금요일 16:00)."""
    if datetime.now().weekday() != 4:
        return
    from supply.reporter import generate_weekly_report
    generate_weekly_report()


def job_monthly_report():
    """월간 리포트 (매월 마지막 영업일 16:00)."""
    from supply.reporter import generate_monthly_report
    today = datetime.now()
    # 간단한 월말 체크: 내일이 다른 달이면 오늘이 마지막 영업일
    import calendar
    last_day = calendar.monthrange(today.year, today.month)[1]
    if today.day >= last_day - 2 and today.weekday() == 4:
        generate_monthly_report()


def job_theme_update():
    """테마 매핑 갱신 (매주 토요일 06:00)."""
    from supply.theme_mapper import run_theme_update
    run_theme_update()


def job_stock_master_update():
    """종목 마스터 갱신 (매주 월요일 06:00)."""
    from supply.collector import refresh_stock_master
    from db.migrations import get_connection

    conn = get_connection()
    try:
        codes = [r["stock_code"] for r in
                 conn.execute("SELECT stock_code FROM stock_master").fetchall()]
    finally:
        conn.close()

    if codes:
        refresh_stock_master(codes)


def init_scheduler() -> BackgroundScheduler:
    """APScheduler 초기화 + 모든 잡 등록."""
    scheduler = BackgroundScheduler(timezone="Asia/Seoul")

    # 장중 폴링 (4회)
    for hm in ["09:40", "11:30", "13:30", "14:40"]:
        h, m = hm.split(":")
        scheduler.add_job(
            job_intraday_poll,
            CronTrigger(hour=int(h), minute=int(m), day_of_week="mon-fri"),
            id=f"intraday_{hm}",
            name=f"장중 폴링 {hm}",
            replace_existing=True,
        )

    # 장 마감 메인 배치
    scheduler.add_job(
        job_daily_batch,
        CronTrigger(hour=15, minute=35, day_of_week="mon-fri"),
        id="daily_batch",
        name="일별 메인 배치",
        replace_existing=True,
    )

    # 일별 리포트
    scheduler.add_job(
        job_daily_report,
        CronTrigger(hour=15, minute=50, day_of_week="mon-fri"),
        id="daily_report",
        name="일별 리포트",
        replace_existing=True,
    )

    # 주간 리포트 (금요일)
    scheduler.add_job(
        job_weekly_report,
        CronTrigger(hour=16, minute=0, day_of_week="fri"),
        id="weekly_report",
        name="주간 리포트",
        replace_existing=True,
    )

    # 월간 리포트 (금요일마다 체크)
    scheduler.add_job(
        job_monthly_report,
        CronTrigger(hour=16, minute=0, day_of_week="fri"),
        id="monthly_report",
        name="월간 리포트 체크",
        replace_existing=True,
    )

    # 테마 매핑 갱신 (토요일)
    scheduler.add_job(
        job_theme_update,
        CronTrigger(hour=6, minute=0, day_of_week="sat"),
        id="theme_update",
        name="테마 매핑 갱신",
        replace_existing=True,
    )

    # 종목 마스터 갱신 (월요일)
    scheduler.add_job(
        job_stock_master_update,
        CronTrigger(hour=6, minute=0, day_of_week="mon"),
        id="master_update",
        name="종목 마스터 갱신",
        replace_existing=True,
    )

    scheduler.start()
    logger.info("스케줄러 시작 — %d개 잡 등록", len(scheduler.get_jobs()))
    return scheduler
