"""
core/loader.py
──────────────
Loads transformed DataFrames into the target warehouse.
Supports: insert, upsert, and replace strategies.
"""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd
from sqlalchemy import create_engine, inspect, text

from core.logger import get_logger

logger = get_logger("Loader")


class DataLoader:
    """
    Loads DataFrames to the configured target database.
    Handles schema creation, duplicate prevention, and row-count reporting.
    """

    def __init__(self, config: Dict):
        self.config     = config
        self.target_cfg = config.get("target", {})
        self._engine    = None

    # ── Connection ────────────────────────────────────────────────────────────

    def _get_engine(self):
        if self._engine is None:
            t = self.target_cfg["type"]
            c = self.target_cfg["connection"]
            if t == "postgresql":
                url = (f"postgresql+psycopg2://{c['username']}:{c['password']}"
                       f"@{c['host']}:{c['port']}/{c['database']}")
            elif t == "mysql":
                url = (f"mysql+pymysql://{c['username']}:{c['password']}"
                       f"@{c['host']}:{c['port']}/{c['database']}")
            else:
                raise ValueError(f"Unsupported target type: {t}")
            self._engine = create_engine(url, pool_pre_ping=True)
        return self._engine

    # ── Public API ────────────────────────────────────────────────────────────

    def load(self, table_name: str, df: pd.DataFrame) -> Dict:
        """Load a DataFrame to the target, returning a result dict."""
        if df.empty:
            logger.warning(f"Empty DataFrame for '{table_name}' — skipping load")
            return {"rows_loaded": 0, "strategy": "skip"}

        strategy = self.target_cfg.get("load_strategy", "upsert")
        schema   = self.target_cfg.get("schema", "public")

        logger.info(f"Loading {len(df):,} rows into '{schema}.{table_name}' [{strategy}]")

        if strategy == "replace":
            rows = self._load_replace(df, table_name, schema)
        elif strategy == "upsert":
            rows = self._load_upsert(df, table_name, schema)
        else:  # insert
            rows = self._load_insert(df, table_name, schema)

        target_count = self._get_target_count(table_name, schema)
        logger.info(f"  '{schema}.{table_name}' now has {target_count:,} rows total")

        return {
            "rows_loaded":    rows,
            "strategy":       strategy,
            "target_count":   target_count,
            "table":          f"{schema}.{table_name}",
        }

    # ── Load Strategies ───────────────────────────────────────────────────────

    def _load_replace(self, df: pd.DataFrame, table: str, schema: str) -> int:
        """DROP and recreate table, then insert all rows."""
        df.to_sql(
            name=table,
            con=self._get_engine(),
            schema=schema,
            if_exists="replace",
            index=False,
            chunksize=10_000,
            method="multi",
        )
        return len(df)

    def _load_insert(self, df: pd.DataFrame, table: str, schema: str) -> int:
        """Append rows; will fail on unique constraint violations if present."""
        df.to_sql(
            name=table,
            con=self._get_engine(),
            schema=schema,
            if_exists="append",
            index=False,
            chunksize=10_000,
            method="multi",
        )
        return len(df)

    def _load_upsert(self, df: pd.DataFrame, table: str, schema: str) -> int:
        """
        Upsert using a staging table + MERGE / INSERT ON CONFLICT.
        Works for PostgreSQL.
        """
        engine    = self._get_engine()
        stage_tbl = f"_stage_{table}"

        # Determine PK from config
        pk = self._get_pk(table)

        with engine.begin() as conn:
            # Write to staging table
            df.to_sql(stage_tbl, conn, schema=schema,
                      if_exists="replace", index=False,
                      chunksize=10_000, method="multi")

            # Create target table if it doesn't exist
            if not inspect(engine).has_table(table, schema=schema):
                df.head(0).to_sql(table, conn, schema=schema,
                                  if_exists="fail", index=False)

            cols     = list(df.columns)
            col_list = ", ".join(cols)
            src_cols = ", ".join(f"s.{c}" for c in cols)
            updates  = ", ".join(
                f"{c} = EXCLUDED.{c}"
                for c in cols if c != pk
            )

            upsert_sql = f"""
                INSERT INTO {schema}.{table} ({col_list})
                SELECT {col_list} FROM {schema}.{stage_tbl}
                ON CONFLICT ({pk})
                DO UPDATE SET {updates};
            """
            conn.execute(text(upsert_sql))
            conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{stage_tbl}"))

        return len(df)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _get_pk(self, table_name: str) -> str:
        for tbl in self.config.get("source", {}).get("tables", []):
            if tbl["name"] == table_name:
                return tbl.get("primary_key", "id")
        return "id"

    def _get_target_count(self, table: str, schema: str) -> int:
        try:
            engine = self._get_engine()
            with engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT COUNT(*) FROM {schema}.{table}")
                )
                return result.scalar()
        except Exception:
            return -1

    def verify_no_duplicates(self, table: str, schema: str, pk: str) -> bool:
        """Returns True if there are no duplicate PKs in the target."""
        engine = self._get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM (
                    SELECT {pk}, COUNT(*) cnt
                    FROM {schema}.{table}
                    GROUP BY {pk}
                    HAVING COUNT(*) > 1
                ) dups
            """))
            dup_count = result.scalar()
            if dup_count > 0:
                logger.error(f"Found {dup_count} duplicate PKs in {schema}.{table}")
                return False
            return True
