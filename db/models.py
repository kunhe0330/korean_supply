"""
SQLite 테이블 정의 — DDL SQL문 모음
v3.0: VDU/Breakout 제거, 수급 유입 조건 필터 + 태그 시스템
"""

SCHEMA_SQL = """
-- 종목 마스터
CREATE TABLE IF NOT EXISTS stock_master (
    stock_code TEXT PRIMARY KEY,
    stock_name TEXT NOT NULL,
    market TEXT NOT NULL,              -- KOSPI / KOSDAQ
    sector_large TEXT,
    sector_medium TEXT,
    sector_small TEXT,
    sector_name TEXT,
    market_cap REAL,
    updated_at TEXT
);

-- 테마 정의 테이블
CREATE TABLE IF NOT EXISTS theme_master (
    theme_id TEXT PRIMARY KEY,
    theme_name TEXT NOT NULL,
    theme_category TEXT,
    updated_at TEXT
);

-- 종목 ↔ 테마 매핑 (N:M)
CREATE TABLE IF NOT EXISTS stock_theme_map (
    stock_code TEXT NOT NULL,
    theme_id TEXT NOT NULL,
    source TEXT NOT NULL,              -- WICS / NAVER / ETF / MANUAL
    confidence REAL DEFAULT 1.0,
    updated_at TEXT,
    PRIMARY KEY (stock_code, theme_id, source)
);

-- 일별 수급 데이터
CREATE TABLE IF NOT EXISTS daily_supply (
    stock_code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    close_price INTEGER,
    change_rate REAL,
    volume INTEGER,
    trade_amount INTEGER,
    frgn_net_qty INTEGER,
    orgn_net_qty INTEGER,
    prsn_net_qty INTEGER,
    frgn_net_amount INTEGER,
    orgn_net_amount INTEGER,
    prsn_net_amount INTEGER,
    scrt_net_qty INTEGER,
    ivtr_net_qty INTEGER,
    bank_net_qty INTEGER,
    insu_net_qty INTEGER,
    fund_net_qty INTEGER,
    buy_vol INTEGER,
    sell_vol INTEGER,
    vol_power REAL,
    PRIMARY KEY (stock_code, trade_date)
);

-- 장중 추정 수급 데이터
CREATE TABLE IF NOT EXISTS intraday_supply (
    stock_code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    time_slot TEXT NOT NULL,
    frgn_est_net_qty INTEGER,
    orgn_est_net_qty INTEGER,
    sum_est_net_qty INTEGER,
    collected_at TEXT,
    PRIMARY KEY (stock_code, trade_date, time_slot)
);

-- 일별 수급 스코어 (v3: 조건 필터 기반)
CREATE TABLE IF NOT EXISTS supply_score (
    stock_code TEXT NOT NULL,
    calc_date TEXT NOT NULL,
    -- 수급 유입 판별 (핵심)
    is_inflow INTEGER DEFAULT 0,       -- 1=수급 유입 조건 충족
    tags TEXT,                         -- JSON: 충족된 태그 ["가속","손바뀜","체결강도↑","거래량↑","RS강함"]
    tag_count INTEGER DEFAULT 0,       -- 태그 수 (많을수록 확신도 높음)
    -- 원시 데이터
    net_6m REAL,
    net_3m REAL,
    net_1m REAL,
    net_1w REAL,
    net_today_amount REAL,             -- 당일 기관+외인 순매수 금액 (백만원)
    acceleration_type TEXT,            -- FULL_ACCEL / SHORT_ACCEL / REVERSAL / DECEL / FLAT
    handover_type TEXT,                -- HANDOVER_STRONG / HANDOVER_MILD / DISTRIBUTION / NONE
    vol_power_today REAL,
    vol_power_5d_avg REAL,
    vol_power_trend TEXT,              -- SURGE / RISING / STABLE / FALLING
    vol_trend TEXT,                    -- EXPANDING / STABLE / CONTRACTING
    vol_ratio_today REAL,              -- 당일거래량 / 20일평균
    theme_list TEXT,                   -- JSON: 소속 테마 리스트
    rel_strength_1m REAL,
    -- 참고용 스코어 (정렬 옵션 전용)
    ref_score REAL DEFAULT 0,          -- 참고용 종합 스코어 (0~100)
    ref_score_rs_bonus REAL DEFAULT 0, -- 상대강도 보너스 (0~10)
    sector_code TEXT,
    sector_name TEXT,
    PRIMARY KEY (stock_code, calc_date)
);

-- 일별 가격 데이터 (OHLCV)
CREATE TABLE IF NOT EXISTS price_daily (
    stock_code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    open_price INTEGER,
    high_price INTEGER,
    low_price INTEGER,
    close_price INTEGER,
    volume INTEGER,
    trade_amount INTEGER,
    change_rate REAL,
    PRIMARY KEY (stock_code, trade_date)
);

-- 지수 일별 데이터
CREATE TABLE IF NOT EXISTS index_daily (
    market TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    index_close INTEGER,
    change_rate REAL,
    PRIMARY KEY (market, trade_date)
);

-- 섹터/테마 분석 결과
CREATE TABLE IF NOT EXISTS sector_analysis (
    sector_code TEXT NOT NULL,
    sector_name TEXT NOT NULL,
    sector_type TEXT NOT NULL,
    calc_date TEXT NOT NULL,
    total_net_amount REAL,
    supply_stock_count INTEGER,        -- 수급 유입(is_inflow=1) 종목 수
    avg_score REAL,                    -- 평균 참고용 스코어
    top_stocks TEXT,                   -- JSON: 순매수 금액 상위 5개 종목
    is_leading INTEGER,
    rank INTEGER,
    PRIMARY KEY (sector_code, calc_date)
);
"""

INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_stock_theme_stock ON stock_theme_map(stock_code);
CREATE INDEX IF NOT EXISTS idx_stock_theme_theme ON stock_theme_map(theme_id);
CREATE INDEX IF NOT EXISTS idx_daily_supply_date ON daily_supply(trade_date);
CREATE INDEX IF NOT EXISTS idx_daily_supply_stock ON daily_supply(stock_code);
CREATE INDEX IF NOT EXISTS idx_intraday_supply_date ON intraday_supply(trade_date, time_slot);
CREATE INDEX IF NOT EXISTS idx_price_daily_date ON price_daily(trade_date);
CREATE INDEX IF NOT EXISTS idx_price_daily_stock ON price_daily(stock_code);
CREATE INDEX IF NOT EXISTS idx_index_daily_market ON index_daily(market, trade_date);
CREATE INDEX IF NOT EXISTS idx_supply_score_date ON supply_score(calc_date);
CREATE INDEX IF NOT EXISTS idx_supply_score_inflow ON supply_score(is_inflow);
CREATE INDEX IF NOT EXISTS idx_sector_analysis_date ON sector_analysis(calc_date);
"""

# v3 마이그레이션: 기존 supply_score, sector_analysis 테이블 재생성
MIGRATION_V3_SQL = """
DROP TABLE IF EXISTS supply_score;
DROP TABLE IF EXISTS sector_analysis;
"""
