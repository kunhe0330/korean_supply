"""
SQLite DB 초기화 및 마이그레이션
"""

import os
import sqlite3
import logging

from db.models import SCHEMA_SQL, INDEX_SQL, MIGRATION_V3_SQL
from config import DB_PATH

logger = logging.getLogger(__name__)


def get_connection() -> sqlite3.Connection:
    """SQLite 커넥션 반환. WAL 모드 + 외래키 활성화."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """테이블 및 인덱스 생성."""
    conn = get_connection()
    try:
        # v3 마이그레이션: 기존 supply_score/sector_analysis 구조가 바뀌었으면 재생성
        _migrate_v3(conn)
        conn.executescript(SCHEMA_SQL)
        conn.executescript(INDEX_SQL)
        conn.commit()
        logger.info("DB 초기화 완료: %s", DB_PATH)
    finally:
        conn.close()


def _migrate_v3(conn):
    """v3 스키마 마이그레이션: supply_score에 is_inflow 컬럼이 없으면 재생성."""
    try:
        cols = conn.execute("PRAGMA table_info(supply_score)").fetchall()
        col_names = {c["name"] for c in cols}
        if cols and "is_inflow" not in col_names:
            logger.info("v3 마이그레이션: supply_score/sector_analysis 테이블 재생성")
            conn.executescript(MIGRATION_V3_SQL)
            conn.commit()
    except Exception:
        pass  # 테이블이 아직 없으면 무시


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_db()
    print(f"DB 초기화 완료: {DB_PATH}")
