"""
core/ge_validator.py
────────────────────
Wraps Great Expectations to validate DataFrames at two pipeline stages:
  - "raw"         → raw_data_suite
  - "transformed" → transformed_data_suite

Provides:
  - Automatic expectation suite creation from config rules
  - Checkpoint execution
  - Result parsing with pass/fail/warning breakdown
  - Data Docs (HTML report) generation
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from core.logger import get_logger

logger = get_logger("GEValidator")


class GEValidator:
    """
    Great Expectations integration layer.
    Handles suite creation, checkpoint runs, and data docs generation.
    """

    SUITE_MAP = {
        "raw":         "raw_data_suite",
        "transformed": "transformed_data_suite",
    }

    def __init__(self, ge_config: Dict):
        self.ge_config = ge_config
        self.root_dir  = ge_config.get("root_dir", "./great_expectations")
        self._context  = None

    # ── Context ───────────────────────────────────────────────────────────────

    def _get_context(self):
        """Lazily initialise the GE data context."""
        if self._context is None:
            import great_expectations as gx
            context_root = Path(self.root_dir)
            if not (context_root / "great_expectations.yml").exists():
                logger.info("Initialising new Great Expectations context")
                self._context = gx.get_context(
                    mode="file",
                    project_root_dir=str(context_root.parent),
                )
            else:
                self._context = gx.get_context(context_root_dir=str(context_root))
        return self._context

    # ── Suite Builder ─────────────────────────────────────────────────────────

    def build_suite_for_table(
        self,
        table_name: str,
        df: pd.DataFrame,
        phase: str = "raw",
    ):
        """
        Programmatically create or update an expectation suite for a table.
        Generates expectations based on DataFrame schema + null rate.
        """
        import great_expectations as gx
        from great_expectations.core.expectation_suite import ExpectationSuite
        from great_expectations.expectations.expectation import ExpectationConfiguration

        ctx        = self._get_context()
        suite_name = f"{phase}_{table_name}_suite"

        try:
            suite = ctx.get_expectation_suite(suite_name)
        except Exception:
            suite = ctx.add_expectation_suite(suite_name)

        expectations = []

        for col in df.columns:
            # Not-null expectation for columns with < 5% nulls in sample
            null_rate = df[col].isnull().mean()
            if null_rate < 0.05:
                expectations.append(ExpectationConfiguration(
                    expectation_type="expect_column_values_to_not_be_null",
                    kwargs={"column": col},
                ))

            dtype = df[col].dtype
            # Type expectations
            if pd.api.types.is_integer_dtype(dtype):
                expectations.append(ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_of_type",
                    kwargs={"column": col, "type_": "int"},
                ))
            elif pd.api.types.is_float_dtype(dtype):
                expectations.append(ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_of_type",
                    kwargs={"column": col, "type_": "float"},
                ))

            # Min/max for numeric
            if pd.api.types.is_numeric_dtype(dtype) and not df[col].isnull().all():
                expectations.append(ExpectationConfiguration(
                    expectation_type="expect_column_min_to_be_between",
                    kwargs={"column": col,
                            "min_value": float(df[col].min()) * 0.5,
                            "max_value": float(df[col].max()) * 2.0},
                ))

        # Table-level: row count > 0
        expectations.append(ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_be_between",
            kwargs={"min_value": 1, "max_value": None},
        ))

        # Column set
        expectations.append(ExpectationConfiguration(
            expectation_type="expect_table_columns_to_match_ordered_list",
            kwargs={"column_list": list(df.columns)},
        ))

        suite.expectations = expectations
        ctx.save_expectation_suite(suite)
        logger.info(f"Saved suite '{suite_name}' with {len(expectations)} expectations")
        return suite_name

    # ── Checkpoint Runner ─────────────────────────────────────────────────────

    def run_checkpoint(
        self,
        dataframes: Dict[str, pd.DataFrame],
        phase: str,
    ) -> Dict[str, Dict]:
        """
        Validate all DataFrames in the given phase.
        Returns per-table pass/fail/warning counts and details.
        """
        results = {}
        for table_name, df in dataframes.items():
            results[table_name] = self._validate_dataframe(df, table_name, phase)
        return results

    def _validate_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        phase: str,
    ) -> Dict:
        """Run GE expectations against a single DataFrame."""
        try:
            import great_expectations as gx
            ctx = self._get_context()

            suite_name = f"{phase}_{table_name}_suite"

            # Auto-build suite if not present
            try:
                ctx.get_expectation_suite(suite_name)
            except Exception:
                logger.info(f"Suite '{suite_name}' not found — auto-generating")
                suite_name = self.build_suite_for_table(table_name, df, phase)

            # Run in-memory validation
            batch_request = self._make_batch_request(ctx, table_name, df)
            validator = ctx.get_validator(
                batch_request=batch_request,
                expectation_suite_name=suite_name,
            )
            results = validator.validate()

            return self._parse_validation_results(results, table_name)

        except Exception as e:
            logger.error(f"GE validation error for '{table_name}' ({phase}): {e}")
            return {
                "passed":   0,
                "failed":   1,
                "warnings": 0,
                "summary":  f"Validation error: {str(e)}",
                "details":  [],
            }

    def _make_batch_request(self, ctx, table_name: str, df: pd.DataFrame):
        """Configure an in-memory (Pandas) datasource batch request."""
        ds_name = f"runtime_ds_{table_name}"
        try:
            ctx.get_datasource(ds_name)
        except Exception:
            ctx.add_datasource(
                name=ds_name,
                class_name="Datasource",
                execution_engine={
                    "class_name": "PandasExecutionEngine",
                },
                data_connectors={
                    "runtime": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": ["batch_id"],
                    }
                },
            )

        from great_expectations.core.batch import RuntimeBatchRequest
        return RuntimeBatchRequest(
            datasource_name=ds_name,
            data_connector_name="runtime",
            data_asset_name=table_name,
            runtime_parameters={"batch_data": df},
            batch_identifiers={"batch_id": "run"},
        )

    # ── Result Parser ─────────────────────────────────────────────────────────

    def _parse_validation_results(self, results, table_name: str) -> Dict:
        passed   = 0
        failed   = 0
        warnings = 0
        details  = []

        for result in results.results:
            exp_type = result.expectation_config.expectation_type
            success  = result.success
            col      = result.expectation_config.kwargs.get("column", "table")

            if success:
                passed += 1
            else:
                failed += 1

            details.append({
                "expectation":    exp_type,
                "column":         col,
                "success":        success,
                "observed_value": str(result.result.get("observed_value", "")),
            })

        summary = (
            f"{passed} passed, {failed} failed, {warnings} warnings"
            f" | Overall: {'PASS' if results.success else 'FAIL'}"
        )
        logger.info(f"[{table_name}] GE results: {summary}")

        return {
            "passed":   passed,
            "failed":   failed,
            "warnings": warnings,
            "success":  results.success,
            "summary":  summary,
            "details":  details,
        }

    # ── Custom Expectations ───────────────────────────────────────────────────

    def add_custom_expectations(
        self,
        suite_name: str,
        expectations: List[Dict],
    ):
        """
        Add custom expectations to an existing suite.

        Example expectation dict:
          {
            "type": "expect_column_values_to_be_between",
            "kwargs": {"column": "price", "min_value": 0, "max_value": 99999}
          }
        """
        ctx = self._get_context()
        try:
            suite = ctx.get_expectation_suite(suite_name)
        except Exception:
            suite = ctx.add_expectation_suite(suite_name)

        from great_expectations.expectations.expectation import ExpectationConfiguration
        for exp in expectations:
            suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type=exp["type"],
                    kwargs=exp.get("kwargs", {}),
                )
            )
        ctx.save_expectation_suite(suite)

    # ── Data Docs ─────────────────────────────────────────────────────────────

    def build_data_docs(self) -> Optional[str]:
        """Build and return the path to GE Data Docs HTML."""
        try:
            ctx = self._get_context()
            ctx.build_data_docs()
            docs_path = Path(self.root_dir) / "uncommitted" / "data_docs" / "local_site" / "index.html"
            logger.info(f"Data Docs built: {docs_path}")
            return str(docs_path)
        except Exception as e:
            logger.warning(f"Data Docs build failed: {e}")
            return None

    # ── Negative / Injection Testing ─────────────────────────────────────────

    @staticmethod
    def inject_bad_data(df: pd.DataFrame, fault_type: str = "nulls") -> pd.DataFrame:
        """
        Utility for negative testing: inject known bad data.
        fault_type: nulls | out_of_range | wrong_type | duplicates
        """
        import numpy as np
        bad = df.copy()
        if fault_type == "nulls":
            col = bad.columns[0]
            bad.loc[bad.sample(frac=0.3).index, col] = None
        elif fault_type == "out_of_range":
            num_cols = bad.select_dtypes(include="number").columns
            if len(num_cols):
                bad.loc[bad.sample(frac=0.1).index, num_cols[0]] = -99999
        elif fault_type == "wrong_type":
            num_cols = bad.select_dtypes(include="number").columns
            if len(num_cols):
                bad[num_cols[0]] = "not_a_number"
        elif fault_type == "duplicates":
            bad = pd.concat([bad, bad.sample(frac=0.1)], ignore_index=True)
        return bad
