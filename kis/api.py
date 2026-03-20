"""
KIS API 공통 호출 함수 — GET 요청 + 재시도 + Rate Limit
"""

import time
import logging

import requests

from config import KIS_BASE_URL, API_RATE_LIMIT
from kis.auth import get_auth_headers
from kis.rate_limiter import rate_limit

logger = logging.getLogger(__name__)


def kis_get(path: str, tr_id: str, params: dict) -> dict:
    """
    KIS API GET 호출.
    - Rate limit 적용
    - 최대 3회 재시도
    - 응답 에러 코드 체크
    """
    url = f"{KIS_BASE_URL}{path}"
    max_retries = API_RATE_LIMIT["max_retries"]
    retry_delay = API_RATE_LIMIT["retry_delay"]

    for attempt in range(max_retries):
        rate_limit()
        try:
            headers = get_auth_headers(tr_id)
            resp = requests.get(url, headers=headers, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            # KIS API 에러 코드 체크
            rt_cd = data.get("rt_cd")
            if rt_cd and rt_cd != "0":
                msg = data.get("msg1", "Unknown error")
                msg_cd = data.get("msg_cd", "")
                logger.warning(
                    "KIS API 에러 [%s] %s (tr_id=%s, attempt=%d)",
                    msg_cd, msg, tr_id, attempt + 1,
                )
                # EGW00123 = 초당 거래 건수 초과 → 대기 후 재시도
                if msg_cd == "EGW00123":
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                # 기타 에러는 바로 반환 (호출자가 처리)
                return data

            return data

        except requests.exceptions.Timeout:
            logger.warning("API 타임아웃 (%d/%d): %s", attempt + 1, max_retries, path)
            time.sleep(retry_delay)
        except requests.exceptions.RequestException as e:
            logger.warning("API 요청 실패 (%d/%d): %s", attempt + 1, max_retries, e)
            time.sleep(retry_delay)

    logger.error("API 호출 최종 실패: %s (tr_id=%s)", path, tr_id)
    return {}


def kis_get_list(path: str, tr_id: str, params: dict, output_key: str = "output2") -> list:
    """
    KIS API 호출 후 리스트 데이터(output/output2) 추출.
    빈 응답이면 빈 리스트 반환.
    """
    data = kis_get(path, tr_id, params)
    if not data:
        return []
    result = data.get(output_key, [])
    if result is None:
        return []
    return result


# ── 주요 API 호출 함수들 ──────────────────────────────────

def fetch_foreign_institution_total(market: str, investor: str) -> list:
    """
    국내기관_외국인 매매종목가집계 (FHPTJ04400000)
    market: "0001"(코스피) / "1001"(코스닥)
    investor: "1"(외국인) / "2"(기관계)
    """
    params = {
        "FID_COND_MRKT_DIV_CODE": "V",
        "FID_COND_SCR_DIV_CODE": "16449",
        "FID_INPUT_ISCD": market,
        "FID_DIV_CLS_CODE": "1",
        "FID_RANK_SORT_CLS_CODE": "0",
        "FID_ETC_CLS_CODE": investor,
    }
    return kis_get_list(
        "/uapi/domestic-stock/v1/quotations/foreign-institution-total",
        "FHPTJ04400000",
        params,
        output_key="output",
    )


def fetch_investor_trade_daily(stock_code: str, base_date: str) -> list:
    """
    종목별 투자자매매동향(일별) (FHPTJ04160001)
    base_date: YYYYMMDD 형식
    """
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": stock_code,
        "FID_INPUT_DATE_1": base_date,
        "FID_ORG_ADJ_PRC": "",
        "FID_ETC_CLS_CODE": "1",
    }
    return kis_get_list(
        "/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily",
        "FHPTJ04160001",
        params,
    )


def fetch_daily_chart_price(stock_code: str, start_date: str, end_date: str) -> list:
    """
    국내주식기간별시세(일봉) (FHKST03010100)
    """
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": stock_code,
        "FID_INPUT_DATE_1": start_date,
        "FID_INPUT_DATE_2": end_date,
        "FID_PERIOD_DIV_CODE": "D",
        "FID_ORG_ADJ_PRC": "0",
    }
    return kis_get_list(
        "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
        "FHKST03010100",
        params,
    )


def fetch_daily_trade_volume(stock_code: str, start_date: str, end_date: str) -> list:
    """
    종목별일별매수매도체결량 (FHKST03010800)
    """
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": stock_code,
        "FID_INPUT_DATE_1": start_date,
        "FID_INPUT_DATE_2": end_date,
        "FID_PERIOD_DIV_CODE": "D",
    }
    return kis_get_list(
        "/uapi/domestic-stock/v1/quotations/inquire-daily-trade-volume",
        "FHKST03010800",
        params,
    )


def fetch_index_daily_price(market_code: str, start_date: str) -> list:
    """
    국내업종 일자별지수 (FHPUP02120000)
    market_code: "0001"(코스피) / "1001"(코스닥)
    """
    params = {
        "FID_COND_MRKT_DIV_CODE": "U",
        "FID_INPUT_ISCD": market_code,
        "FID_INPUT_DATE_1": start_date,
        "FID_PERIOD_DIV_CODE": "D",
    }
    return kis_get_list(
        "/uapi/domestic-stock/v1/quotations/inquire-index-daily-price",
        "FHPUP02120000",
        params,
    )


def fetch_investor_trend_estimate(stock_code: str) -> list:
    """
    종목별 외인기관 추정가집계 — 장중 추정 (HHPTJ04160200)
    """
    params = {
        "MKSC_SHRN_ISCD": stock_code,
    }
    return kis_get_list(
        "/uapi/domestic-stock/v1/quotations/investor-trend-estimate",
        "HHPTJ04160200",
        params,
    )


def fetch_stock_info(stock_code: str) -> dict:
    """
    주식기본조회 (CTPF1002R) — 종목 마스터 정보 (업종코드 등)
    """
    params = {
        "PRDT_TYPE_CD": "300",
        "PDNO": stock_code,
    }
    data = kis_get(
        "/uapi/domestic-stock/v1/quotations/search-stock-info",
        "CTPF1002R",
        params,
    )
    output = data.get("output", {})
    return output if output else {}


def fetch_volume_power_ranking(market_code: str) -> list:
    """
    국내주식 체결강도 상위 (FHPST01680000)
    market_code: "0001"(코스피) / "1001"(코스닥)
    """
    params = {
        "fid_cond_mrkt_div_code": "J",
        "fid_cond_scr_div_code": "20168",
        "fid_input_iscd": market_code,
        "fid_div_cls_code": "0",
        "fid_input_price_1": "",
        "fid_input_price_2": "",
        "fid_vol_cnt": "",
        "fid_trgt_exls_cls_code": "0",
        "fid_trgt_cls_code": "0",
    }
    return kis_get_list(
        "/uapi/domestic-stock/v1/ranking/volume-power",
        "FHPST01680000",
        params,
        output_key="output",
    )
