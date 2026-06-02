"""
core/transformer.py
───────────────────
Applies transformation rules from config to DataFrames.
Supports: rename, cast, derive, filter, deduplicate, aggregate, join.
"""

from __future__ import annotations

from typing import Any, Dict, List

import numpy as np
import pandas as pd

from core.logger import get_logger

logger = get_logger("Transformer")


class DataTransformer:
    """
    Applies a chain of transformation operations to a DataFrame
    based on YAML-defined rules.
    """

    def __init__(self, config: Dict):
        self.config = config
        self.transform_rules: Dict[str, List[Dict]] = (
            config.get("transformations", {})
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def transform(self, table_name: str, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all transformation rules for the given table."""
        rules = self.transform_rules.get(table_name, [])
        if not rules:
            logger.info(f"No transformation rules for '{table_name}' — passthrough")
            return df.copy()

        result = df.copy()
        for rule in rules:
            op = rule.get("type")
            logger.debug(f"Applying '{op}' to '{table_name}'")
            try:
                result = self._apply_rule(result, rule, table_name)
            except Exception as e:
                logger.error(f"Transformation '{op}' failed on '{table_name}': {e}")
                raise
        logger.info(
            f"Transformed '{table_name}': {len(df):,} → {len(result):,} rows, "
            f"{len(df.columns)} → {len(result.columns)} cols"
        )
        return result

    # ── Rule Dispatcher ───────────────────────────────────────────────────────

    def _apply_rule(self, df: pd.DataFrame, rule: Dict, table: str) -> pd.DataFrame:
        op = rule["type"]
        dispatch = {
            "rename_columns":      self._rename_columns,
            "cast_types":          self._cast_types,
            "derive_column":       self._derive_column,
            "filter_rows":         self._filter_rows,
            "deduplicate":         self._deduplicate,
            "fill_nulls":          self._fill_nulls,
            "drop_columns":        self._drop_columns,
            "normalize_strings":   self._normalize_strings,
            "aggregate":           self._aggregate,
            "add_metadata":        self._add_metadata,
            "map_values":          self._map_values,
        }
        if op not in dispatch:
            logger.warning(f"Unknown transformation type '{op}' — skipping")
            return df
        return dispatch[op](df, rule)

    # ── Transformations ───────────────────────────────────────────────────────

    def _rename_columns(self, df: pd.DataFrame, rule: Dict) -> pd.DataFrame:
        mapping = rule.get("mapping", {})
        # Only rename columns that actually exist
        existing = {k: v for k, v in mapping.items() if k in df.columns}
        return df.rename(columns=existing)

    def _cast_types(self, df: pd.DataFrame, rule: Dict) -> pd.DataFrame:
        columns = rule.get("columns", {})
        for col, dtype in columns.items():
            if col not in df.columns:
                continue
            try:
                if dtype in ("datetime", "date"):
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                elif dtype in ("int", "integer"):
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                elif dtype in ("float", "double"):
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                elif dtype == "str":
                    df[col] = df[col].astype(str)
                elif dtype == "bool":
                    df[col] = df[col].astype(bool)
                else:
                    df[col] = df[col].astype(dtype)
            except Exception as e:
                logger.warning(f"Cast failed for column '{col}' to '{dtype}': {e}")
        return df

    def _derive_column(self, df: pd.DataFrame, rule: Dict) -> pd.DataFrame:
        name       = rule["name"]
        expression = rule["expression"]
        try:
            df[name] = df.eval(expression)
        except Exception:
            # Fallback: try as lambda string
            func = rule.get("function")
            if func:
                df[name] = df.apply(eval(func), axis=1)
        return df

    def _filter_rows(self, df: pd.DataFrame, rule: Dict) -> pd.DataFrame:
        condition = rule.get("condition", "")
        before = len(df)
        df = df.query(condition)
        logger.debug(f"Filter removed {before - len(df):,} rows")
        return df

    def _deduplicate(self, df: pd.DataFrame, rule: Dict) -> pd.DataFrame:
        subset = rule.get("subset")
        keep   = rule.get("keep", "last")
        before = len(df)
        df = df.drop_duplicates(subset=subset, keep=keep)
        logger.debug(f"Dedup removed {before - len(df):,} duplicate rows")
        return df

    def _fill_nulls(self, df: pd.DataFrame, rule: Dict) -> pd.DataFrame:
        columns = rule.get("columns", {})
        for col, fill_value in columns.items():
            if col in df.columns:
                df[col] = df[col].fillna(fill_value)
        return df

    def _drop_columns(self, df: pd.DataFrame, rule: Dict) -> pd.DataFrame:
        cols = [c for c in rule.get("columns", []) if c in df.columns]
        return df.drop(columns=cols)

    def _normalize_strings(self, df: pd.DataFrame, rule: Dict) -> pd.DataFrame:
        columns = rule.get("columns", [])
        for col in columns:
            if col in df.columns and df[col].dtype == object:
                if rule.get("strip"):
                    df[col] = df[col].str.strip()
                if rule.get("lowercase"):
                    df[col] = df[col].str.lower()
                if rule.get("uppercase"):
                    df[col] = df[col].str.upper()
        return df

    def _aggregate(self, df: pd.DataFrame, rule: Dict) -> pd.DataFrame:
        group_by = rule.get("group_by", [])
        agg      = rule.get("aggregations", {})
        return df.groupby(group_by).agg(agg).reset_index()

    def _add_metadata(self, df: pd.DataFrame, rule: Dict) -> pd.DataFrame:
        from datetime import datetime
        if rule.get("etl_timestamp"):
            df["_etl_loaded_at"] = datetime.utcnow()
        if rule.get("etl_source"):
            df["_etl_source"] = rule["etl_source"]
        return df

    def _map_values(self, df: pd.DataFrame, rule: Dict) -> pd.DataFrame:
        col     = rule.get("column")
        mapping = rule.get("mapping", {})
        default = rule.get("default", None)
        if col in df.columns:
            if default is not None:
                df[col] = df[col].map(mapping).fillna(default)
            else:
                df[col] = df[col].map(mapping)
        return df
