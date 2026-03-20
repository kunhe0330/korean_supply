"""
테마 매핑 시스템
WICS + 네이버 + ETF + 수동 매핑을 계층적으로 결합.
크롤링 실패 시 캐시 fallback.
"""

import json
import logging
import os
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from config import (
    MANUAL_THEME_MAP_PATH,
    ETF_THEME_MAP_PATH,
    CACHE_DIR,
    DATA_DIR,
)
from db.migrations import get_connection

logger = logging.getLogger(__name__)

# 소스별 기본 confidence
CONFIDENCE = {
    "MANUAL": 1.0,
    "ETF": 0.9,
    "WICS": 0.8,
    "NAVER": 0.7,
}


def _ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)


def _load_json(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(path: str, data: dict):
    _ensure_cache_dir()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ── 소스 ① 수동 매핑 ────────────────────────────────────

def load_manual_mapping() -> dict[str, list[str]]:
    """
    manual_theme_map.json → {theme_id: [stock_code, ...]}
    """
    data = _load_json(MANUAL_THEME_MAP_PATH)
    themes = data.get("themes", {})
    result = {}
    for theme_id, info in themes.items():
        stocks = info.get("stocks", [])
        if stocks:
            result[theme_id] = stocks
    return result


# ── 소스 ② ETF 구성종목 역매핑 ──────────────────────────

def load_etf_mapping() -> dict[str, list[str]]:
    """
    etf_theme_map.json의 ETF→테마 매핑을 로드.
    실제 ETF 구성종목은 KIS API 호출이 필요하므로 캐시 사용.
    """
    cache_path = os.path.join(CACHE_DIR, "etf_theme_cache.json")
    cache = _load_json(cache_path)
    if cache:
        return cache
    # 캐시 없으면 etf_theme_map.json에서 기본 매핑만 반환
    data = _load_json(ETF_THEME_MAP_PATH)
    return data.get("mappings", {})


def update_etf_theme_cache(etf_constituents: dict[str, list[str]]):
    """
    ETF 구성종목 데이터를 캐시에 저장.
    etf_constituents: {theme_id: [stock_code, ...]}
    """
    cache_path = os.path.join(CACHE_DIR, "etf_theme_cache.json")
    _save_json(cache_path, etf_constituents)
    logger.info("ETF 테마 캐시 저장 완료")


# ── 소스 ③ WICS 산업분류 크롤링 ─────────────────────────

WICS_CACHE_PATH = os.path.join(CACHE_DIR, "wics_cache.json") if CACHE_DIR else ""

# WICS 소분류 → 테마 매핑
WICS_TO_THEME = {
    "반도체와반도체장비": "SEMICONDUCTOR",
    "반도체": "SEMICONDUCTOR",
    "자동차부품": "AUTO",
    "자동차": "AUTO",
    "조선": "SHIPBUILDING",
    "에너지장비및서비스": "RENEWABLE",
    "우주항공과국방": "DEFENSE",
    "소프트웨어": "AI",
    "IT서비스": "AI",
    "제약": "BIO",
    "생물공학": "BIO",
    "화학": "CHEMICAL",
    "2차전지": "BATTERY",
    "전기장비": "BATTERY",
    "기계": "MACHINERY",
    "건설": "CONSTRUCTION",
    "은행": "FINANCE",
    "증권": "FINANCE",
    "보험": "FINANCE",
}


def crawl_wics() -> dict[str, list[str]]:
    """
    WiseFn WICS 산업분류 크롤링.
    Returns: {theme_id: [stock_code, ...]}
    """
    result = {}
    # WICS 대분류 코드 리스트
    sector_codes = [
        "G1010", "G1510", "G2010", "G2020", "G2030", "G2510", "G2520",
        "G2530", "G2550", "G3010", "G3020", "G3030", "G3510", "G4010",
        "G4020", "G4030", "G4040", "G4050", "G4510", "G4520", "G4530",
        "G4535", "G4540", "G5010", "G5510",
    ]

    for code in sector_codes:
        try:
            url = f"https://www.wiseindex.com/Index/GetIndexComponets"
            params = {
                "ceil_yn": "0",
                "dt": datetime.now().strftime("%Y%m%d"),
                "sec_cd": f"{code}",
            }
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            if not data or not isinstance(data, list):
                continue

            # 첫 항목에서 업종명 추출
            sector_name = data[0].get("SEC_NM_KOR", "") if data else ""
            theme_id = WICS_TO_THEME.get(sector_name, "")
            if not theme_id:
                continue

            stocks = []
            for item in data:
                stock_code = item.get("CMP_CD", "")
                if stock_code and len(stock_code) == 6:
                    stocks.append(stock_code)

            if stocks:
                if theme_id in result:
                    result[theme_id].extend(stocks)
                else:
                    result[theme_id] = stocks

        except Exception as e:
            logger.warning("WICS 크롤링 실패 (sector=%s): %s", code, e)
            continue

    # 중복 제거
    for theme_id in result:
        result[theme_id] = list(set(result[theme_id]))

    return result


# ── 소스 ④ 네이버 증권 테마 크롤링 ──────────────────────

NAVER_CACHE_PATH = os.path.join(CACHE_DIR, "naver_theme_cache.json") if CACHE_DIR else ""

# 네이버 테마명 → 내부 theme_id 매핑
NAVER_TO_THEME = {
    "인공지능(AI)": "AI",
    "인공지능": "AI",
    "AI": "AI",
    "2차전지": "BATTERY",
    "2차전지(소재/부품)": "BATTERY",
    "방위산업": "DEFENSE",
    "신재생에너지": "RENEWABLE",
    "조선": "SHIPBUILDING",
    "원자력발전": "NUCLEAR",
    "로봇": "ROBOT",
    "반도체": "SEMICONDUCTOR",
    "바이오": "BIO",
    "제약": "BIO",
    "자동차": "AUTO",
    "전기차": "EV",
    "HBM": "SEMICONDUCTOR",
    "양자컴퓨터": "QUANTUM",
}


def crawl_naver_themes() -> dict[str, list[str]]:
    """
    네이버 증권 테마 페이지 크롤링.
    Returns: {theme_id: [stock_code, ...]}
    """
    result = {}
    base_url = "https://finance.naver.com/sise/theme.naver"

    try:
        resp = requests.get(base_url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0"
        })
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # 테마 리스트 추출
        theme_links = soup.select("td.col_type1 a")
        for link in theme_links:
            theme_name = link.get_text(strip=True)
            theme_id = NAVER_TO_THEME.get(theme_name)
            if not theme_id:
                continue

            # 테마 상세 페이지에서 종목 추출
            href = link.get("href", "")
            if not href:
                continue
            detail_url = f"https://finance.naver.com{href}"
            try:
                detail_resp = requests.get(detail_url, timeout=10, headers={
                    "User-Agent": "Mozilla/5.0"
                })
                detail_resp.raise_for_status()
                detail_soup = BeautifulSoup(detail_resp.text, "html.parser")

                stock_links = detail_soup.select("div.name_area a[href*='main.naver?code=']")
                stocks = []
                for sl in stock_links:
                    code_href = sl.get("href", "")
                    if "code=" in code_href:
                        code = code_href.split("code=")[-1][:6]
                        if code.isdigit() and len(code) == 6:
                            stocks.append(code)

                if stocks:
                    if theme_id in result:
                        result[theme_id].extend(stocks)
                    else:
                        result[theme_id] = stocks

            except Exception as e:
                logger.warning("네이버 테마 상세 크롤링 실패 (%s): %s", theme_name, e)

    except Exception as e:
        logger.warning("네이버 테마 크롤링 실패: %s", e)

    # 중복 제거
    for theme_id in result:
        result[theme_id] = list(set(result[theme_id]))

    return result


# ── 통합 테마 매핑 로드 ──────────────────────────────────

def load_theme_mapping() -> dict[str, list[tuple[str, str, float]]]:
    """
    모든 소스의 테마 매핑을 통합 로드.
    Returns: {stock_code: [(theme_id, source, confidence), ...]}

    Fallback 체인:
    1. 수동 매핑 (항상 성공)
    2. ETF 구성종목 (캐시)
    3. WICS 크롤링 (실패 가능 → 캐시)
    4. 네이버 테마 크롤링 (실패 가능 → 캐시)
    """
    stock_themes: dict[str, list[tuple[str, str, float]]] = {}

    def _add(theme_id: str, stocks: list[str], source: str):
        conf = CONFIDENCE[source]
        for code in stocks:
            if code not in stock_themes:
                stock_themes[code] = []
            stock_themes[code].append((theme_id, source, conf))

    # 1. 수동 매핑
    manual = load_manual_mapping()
    for theme_id, stocks in manual.items():
        _add(theme_id, stocks, "MANUAL")

    # 2. ETF 매핑 (캐시)
    etf = load_etf_mapping()
    # etf는 {etf_code: theme_id} 형태 — 구성종목은 캐시에서
    etf_cache_path = os.path.join(CACHE_DIR, "etf_theme_cache.json")
    etf_cache = _load_json(etf_cache_path)
    if isinstance(etf_cache, dict):
        for theme_id, stocks in etf_cache.items():
            if isinstance(stocks, list):
                _add(theme_id, stocks, "ETF")

    # 3. WICS
    wics_cache_path = os.path.join(CACHE_DIR, "wics_cache.json")
    try:
        wics = crawl_wics()
        if wics:
            _save_json(wics_cache_path, wics)
            for theme_id, stocks in wics.items():
                _add(theme_id, stocks, "WICS")
        else:
            raise ValueError("WICS 결과 비어있음")
    except Exception as e:
        logger.warning("WICS 크롤링 실패, 캐시 사용: %s", e)
        cached = _load_json(wics_cache_path)
        for theme_id, stocks in cached.items():
            if isinstance(stocks, list):
                _add(theme_id, stocks, "WICS")

    # 4. 네이버 테마
    naver_cache_path = os.path.join(CACHE_DIR, "naver_theme_cache.json")
    try:
        naver = crawl_naver_themes()
        if naver:
            _save_json(naver_cache_path, naver)
            for theme_id, stocks in naver.items():
                _add(theme_id, stocks, "NAVER")
        else:
            raise ValueError("네이버 테마 결과 비어있음")
    except Exception as e:
        logger.warning("네이버 크롤링 실패, 캐시 사용: %s", e)
        cached = _load_json(naver_cache_path)
        for theme_id, stocks in cached.items():
            if isinstance(stocks, list):
                _add(theme_id, stocks, "NAVER")

    return stock_themes


def save_theme_mapping_to_db(stock_themes: dict[str, list[tuple[str, str, float]]]):
    """
    통합 테마 매핑을 DB에 저장.
    theme_master + stock_theme_map 테이블 업데이트.
    """
    conn = get_connection()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 테마 ID → 한글명 매핑
    THEME_NAMES = {
        "AI": "AI/인공지능",
        "SEMICONDUCTOR": "반도체",
        "BATTERY": "2차전지",
        "DEFENSE": "방산",
        "SHIPBUILDING": "조선",
        "BIO": "바이오",
        "ROBOT": "로봇",
        "NUCLEAR": "원자력",
        "RENEWABLE": "신재생에너지",
        "AUTO": "자동차",
        "EV": "전기차",
        "CHEMICAL": "화학",
        "MACHINERY": "기계",
        "CONSTRUCTION": "건설",
        "FINANCE": "금융",
        "QUANTUM": "양자컴퓨터",
    }

    THEME_CATEGORIES = {
        "AI": "TECH", "SEMICONDUCTOR": "TECH", "ROBOT": "TECH", "QUANTUM": "TECH",
        "BATTERY": "INDUSTRY", "DEFENSE": "INDUSTRY", "SHIPBUILDING": "INDUSTRY",
        "AUTO": "INDUSTRY", "EV": "INDUSTRY", "MACHINERY": "INDUSTRY",
        "BIO": "HEALTH",
        "NUCLEAR": "ENERGY", "RENEWABLE": "ENERGY",
        "CHEMICAL": "MATERIALS", "CONSTRUCTION": "MATERIALS",
        "FINANCE": "FINANCE",
    }

    try:
        # 테마 마스터 업데이트
        all_themes = set()
        for themes in stock_themes.values():
            for theme_id, _, _ in themes:
                all_themes.add(theme_id)

        for theme_id in all_themes:
            conn.execute(
                """INSERT OR REPLACE INTO theme_master
                   (theme_id, theme_name, theme_category, updated_at)
                   VALUES (?, ?, ?, ?)""",
                (
                    theme_id,
                    THEME_NAMES.get(theme_id, theme_id),
                    THEME_CATEGORIES.get(theme_id, "OTHER"),
                    now,
                ),
            )

        # stock_theme_map 업데이트
        for stock_code, themes in stock_themes.items():
            for theme_id, source, confidence in themes:
                conn.execute(
                    """INSERT OR REPLACE INTO stock_theme_map
                       (stock_code, theme_id, source, confidence, updated_at)
                       VALUES (?, ?, ?, ?, ?)""",
                    (stock_code, theme_id, source, confidence, now),
                )

        conn.commit()
        logger.info(
            "테마 매핑 DB 저장 완료: 테마 %d개, 매핑 %d건",
            len(all_themes),
            sum(len(v) for v in stock_themes.values()),
        )
    finally:
        conn.close()


def get_stock_themes(stock_code: str) -> list[dict]:
    """
    특정 종목의 테마 조회. 여러 소스 결합 + confidence 합산.
    Returns: [{"theme_id": str, "theme_name": str, "confidence": float}, ...]
    """
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT stm.theme_id, tm.theme_name, stm.source, stm.confidence
               FROM stock_theme_map stm
               LEFT JOIN theme_master tm ON stm.theme_id = tm.theme_id
               WHERE stm.stock_code = ?
               ORDER BY stm.confidence DESC""",
            (stock_code,),
        ).fetchall()

        # 같은 테마에 대해 여러 소스가 동의하면 confidence 합산 (최대 1.0)
        theme_conf = {}
        theme_names = {}
        for row in rows:
            tid = row["theme_id"]
            theme_names[tid] = row["theme_name"] or tid
            if tid not in theme_conf:
                theme_conf[tid] = 0.0
            theme_conf[tid] = min(1.0, theme_conf[tid] + row["confidence"] * 0.3)

        return [
            {"theme_id": tid, "theme_name": theme_names[tid], "confidence": conf}
            for tid, conf in sorted(theme_conf.items(), key=lambda x: -x[1])
        ]
    finally:
        conn.close()


def run_theme_update():
    """테마 매핑 전체 갱신 (주 1회 or 월 1회)."""
    logger.info("=== 테마 매핑 갱신 시작 ===")
    stock_themes = load_theme_mapping()
    save_theme_mapping_to_db(stock_themes)
    logger.info("=== 테마 매핑 갱신 완료 (%d 종목) ===", len(stock_themes))
