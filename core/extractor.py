"""
core/extractor.py
─────────────────
Handles data extraction from source systems.
Supports PostgreSQL, MySQL, CSV, Parquet, and REST APIs.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
from sqlalchemy import create_engine, text

from core.logger import get_logger

logger = get_logger("Extractor")


class DataExtractor:
    """
    Extracts data from configured source systems.
    Supports full refresh and incremental (timestamp / id-based) strategies.
    """

    def __init__(self, config: Dict):
        self.config = config
        self.source_cfg = config.get("source", {})
        self._engine = None

    # ── Connection ────────────────────────────────────────────────────────────

    def _get_engine(self):
        if self._engine is None:
            src = self.source_cfg
            t   = src["type"]
            c   = src["connection"]
            if t == "postgresql":
                url = (f"postgresql+psycopg2://{c['username']}:{c['password']}"
                       f"@{c['host']}:{c['port']}/{c['database']}")
            elif t == "mysql":
                url = (f"mysql+pymysql://{c['username']}:{c['password']}"
                       f"@{c['host']}:{c['port']}/{c['database']}")
            else:
                raise ValueError(f"Unsupported source type: {t}")
            self._engine = create_engine(url, pool_pre_ping=True)
        return self._engine

    # ── Public API ────────────────────────────────────────────────────────────

    def extract_table(
        self,
        table_name: str,
        incremental_col: Optional[str] = None,
        strategy: str = "full_refresh",
    ) -> pd.DataFrame:
        """Extract a table using the specified strategy."""
        src_type = self.source_cfg.get("type")

        if src_type in ("postgresql", "mysql"):
            return self._extract_sql(table_name, incremental_col, strategy)
        elif src_type == "csv":
            return self._extract_csv(table_name)
        elif src_type == "parquet":
            return self._extract_parquet(table_name)
        elif src_type == "api":
            return self._extract_api(table_name)
        else:
            raise ValueError(f"Unknown source type: {src_type}")

    # ── SQL Extraction ────────────────────────────────────────────────────────

    def _extract_sql(
        self,
        table_name: str,
        incremental_col: Optional[str],
        strategy: str,
    ) -> pd.DataFrame:
        engine = self._get_engine()

        if strategy == "full_refresh" or not incremental_col:
            query = f"SELECT * FROM {table_name}"
            logger.debug(f"Full refresh: {query}")
            return pd.read_sql(text(query), engine)

        elif strategy == "timestamp":
            watermark = self._get_watermark(table_name)
            query = (f"SELECT * FROM {table_name} "
                     f"WHERE {incremental_col} > :wm")
            logger.debug(f"Incremental (timestamp) query for '{table_name}' since {watermark}")
            with engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params={"wm": watermark})
            self._save_watermark(table_name, df[incremental_col].max() if not df.empty else watermark)
            return df

        elif strategy == "id":
            watermark = self._get_watermark(table_name, default=0)
            query = (f"SELECT * FROM {table_name} "
                     f"WHERE {incremental_col} > :wm ORDER BY {incremental_col}")
            with engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params={"wm": int(watermark)})
            self._save_watermark(table_name, int(df[incremental_col].max()) if not df.empty else watermark)
            return df

        raise ValueError(f"Unknown incremental strategy: {strategy}")

    # ── File Extraction ───────────────────────────────────────────────────────

    def _extract_csv(self, table_name: str) -> pd.DataFrame:
        data_dir = self.source_cfg.get("data_dir", "./data")
        path = Path(data_dir) / f"{table_name}.csv"
        logger.info(f"Reading CSV: {path}")
        return pd.read_csv(path)

    def _extract_parquet(self, table_name: str) -> pd.DataFrame:
        data_dir = self.source_cfg.get("data_dir", "./data")
        path = Path(data_dir) / f"{table_name}.parquet"
        logger.info(f"Reading Parquet: {path}")
        return pd.read_parquet(path)

    # ── API Extraction ────────────────────────────────────────────────────────

    def _extract_api(self, table_name: str) -> pd.DataFrame:
        import requests
        api_cfg = self.source_cfg.get("api", {})
        base_url = api_cfg.get("base_url", "")
        endpoint = api_cfg.get("endpoints", {}).get(table_name, f"/{table_name}")
        headers  = api_cfg.get("headers", {})
        params   = api_cfg.get("params", {})

        url  = base_url.rstrip("/") + endpoint
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if isinstance(data, list):
            return pd.DataFrame(data)
        if isinstance(data, dict):
            key = api_cfg.get("data_key", "data")
            return pd.DataFrame(data.get(key, []))
        raise ValueError(f"Unexpected API response type: {type(data)}")

    # ── Watermark ─────────────────────────────────────────────────────────────

    def _watermark_path(self, table_name: str) -> Path:
        p = Path(".etl_state")
        p.mkdir(exist_ok=True)
        return p / f"{table_name}.watermark"

    def _get_watermark(self, table_name: str, default=None):
        path = self._watermark_path(table_name)
        if path.exists():
            return path.read_text().strip()
        return default or (datetime.utcnow() - timedelta(days=1)).isoformat()

    def _save_watermark(self, table_name: str, value):
        self._watermark_path(table_name).write_text(str(value))

    # ── Row Count Validation ──────────────────────────────────────────────────

    def get_source_row_count(self, table_name: str) -> int:
        """Returns source row count for reconciliation."""
        engine = self._get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            return result.scalar()
