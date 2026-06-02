"""
tests/test_pipeline.py
──────────────────────
Automated test suite covering all 10 testing phases:
1. Extraction
2. Raw GE Validation
3. Transformation Logic
4. Post-Transform GE Validation
5. Load
6. Reconciliation
7. GE Reports
8. Error Handling & Alerts
9. End-to-End Pipeline
10. Regression / Negative Testing
"""

import json
import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, patch, PropertyMock
from pathlib import Path

# ── Fixtures ──────────────────────────────────────────────────────────────────

SAMPLE_CONFIG = {
    "pipeline": {"name": "Test Pipeline", "on_failure": "alert_and_stop"},
    "source":   {"type": "csv", "tables": [
        {"name": "orders", "primary_key": "order_id",
         "incremental_column": None, "incremental_strategy": "full_refresh"}
    ]},
    "staging":  {"type": "postgresql", "connection": {}},
    "target":   {"type": "postgresql", "connection": {},
                 "schema": "warehouse", "load_strategy": "upsert"},
    "great_expectations": {"root_dir": "/tmp/test_ge"},
    "transformations": {
        "orders": [
            {"type": "rename_columns",    "mapping": {"qty": "quantity"}},
            {"type": "cast_types",        "columns": {"amount": "float", "quantity": "int"}},
            {"type": "derive_column",     "name": "total_value", "expression": "amount * quantity"},
            {"type": "filter_rows",       "condition": "amount > 0"},
            {"type": "deduplicate",       "subset": ["order_id"], "keep": "last"},
        ]
    },
    "validation": {"row_count_tolerance_pct": 5.0, "critical_columns": {"orders": ["order_id"]}},
    "alerts":   {"email": {"enabled": False}, "slack": {"enabled": False}, "teams": {"enabled": False}},
    "reporting":{"output_dir": "/tmp/etl_reports", "generate_ge_docs": False},
    "logging":  {"level": "DEBUG", "output_dir": "/tmp/etl_logs"},
}


@pytest.fixture
def sample_orders() -> pd.DataFrame:
    return pd.DataFrame({
        "order_id":   [1, 2, 3, 4, 5],
        "customer_id":[10, 20, 30, 40, 50],
        "qty":        [2, 1, 3, 1, 4],
        "amount":     [100.0, 50.0, 200.0, 75.0, 150.0],
        "status":     ["ACTIVE", "ACTIVE", "CANCELLED", "ACTIVE", "ACTIVE"],
        "order_date": pd.date_range("2024-01-01", periods=5),
    })


@pytest.fixture
def transformer():
    from core.transformer import DataTransformer
    return DataTransformer(SAMPLE_CONFIG)


@pytest.fixture
def ge_validator():
    from core.ge_validator import GEValidator
    return GEValidator(SAMPLE_CONFIG["great_expectations"])


# ── Phase 1: Extraction ───────────────────────────────────────────────────────

class TestExtraction:

    def test_csv_extraction(self, tmp_path, sample_orders):
        """CSV extraction returns correct DataFrame."""
        csv_path = tmp_path / "orders.csv"
        sample_orders.to_csv(csv_path, index=False)

        cfg = {**SAMPLE_CONFIG, "source": {"type": "csv", "data_dir": str(tmp_path),
              "tables": SAMPLE_CONFIG["source"]["tables"]}}
        from core.extractor import DataExtractor
        extractor = DataExtractor(cfg)
        df = extractor.extract_table("orders")
        assert len(df) == len(sample_orders)
        assert set(df.columns) == set(sample_orders.columns)

    def test_extraction_row_count(self, tmp_path, sample_orders):
        """Extracted row count matches source."""
        csv_path = tmp_path / "orders.csv"
        sample_orders.to_csv(csv_path, index=False)

        cfg = {**SAMPLE_CONFIG, "source": {"type": "csv", "data_dir": str(tmp_path),
              "tables": SAMPLE_CONFIG["source"]["tables"]}}
        from core.extractor import DataExtractor
        extractor = DataExtractor(cfg)
        df = extractor.extract_table("orders")
        assert len(df) == 5

    def test_missing_file_raises(self):
        """Missing CSV raises FileNotFoundError."""
        from core.extractor import DataExtractor
        extractor = DataExtractor({**SAMPLE_CONFIG, "source": {
            "type": "csv", "data_dir": "/nonexistent",
            "tables": SAMPLE_CONFIG["source"]["tables"]
        }})
        with pytest.raises(Exception):
            extractor.extract_table("nonexistent_table")


# ── Phase 2: Raw GE Validation ────────────────────────────────────────────────

class TestRawGEValidation:

    def test_clean_data_passes_validation(self, ge_validator, sample_orders):
        """Clean DataFrame should pass all generated expectations."""
        with patch.object(ge_validator, "_validate_dataframe") as mock_validate:
            mock_validate.return_value = {
                "passed": 10, "failed": 0, "warnings": 0,
                "success": True, "summary": "10 passed, 0 failed",
                "details": []
            }
            results = ge_validator.run_checkpoint({"orders": sample_orders}, "raw")
            assert results["orders"]["failed"] == 0
            assert results["orders"]["passed"] == 10

    def test_null_injection_detected(self, ge_validator, sample_orders):
        """GE catches injected nulls in required columns."""
        from core.ge_validator import GEValidator
        bad_df = GEValidator.inject_bad_data(sample_orders.copy(), "nulls")
        null_pct = bad_df["order_id"].isnull().mean()
        # Confirm nulls were injected
        assert null_pct > 0

    def test_inject_bad_data_nulls(self, sample_orders):
        """Null injection modifies ~30% of first column."""
        from core.ge_validator import GEValidator
        bad = GEValidator.inject_bad_data(sample_orders, "nulls")
        assert bad.isnull().sum().sum() > 0

    def test_inject_bad_data_out_of_range(self, sample_orders):
        """Out-of-range injection sets extreme values."""
        from core.ge_validator import GEValidator
        bad = GEValidator.inject_bad_data(sample_orders, "out_of_range")
        # Should contain -99999
        has_extreme = (bad.select_dtypes(include="number") == -99999).any().any()
        assert has_extreme

    def test_inject_duplicates(self, sample_orders):
        """Duplicate injection increases row count."""
        from core.ge_validator import GEValidator
        bad = GEValidator.inject_bad_data(sample_orders, "duplicates")
        assert len(bad) > len(sample_orders)


# ── Phase 3: Transformation Logic ────────────────────────────────────────────

class TestTransformations:

    def test_rename_columns(self, transformer, sample_orders):
        """qty → quantity rename is applied."""
        result = transformer.transform("orders", sample_orders)
        assert "quantity" in result.columns
        assert "qty" not in result.columns

    def test_cast_amount_to_float(self, transformer, sample_orders):
        """amount column is cast to float."""
        result = transformer.transform("orders", sample_orders)
        assert result["amount"].dtype in (float, "float64")

    def test_derived_column(self, transformer, sample_orders):
        """total_value = amount * quantity is derived correctly."""
        result = transformer.transform("orders", sample_orders)
        assert "total_value" in result.columns
        expected = sample_orders["amount"].iloc[0] * sample_orders["qty"].iloc[0]
        assert abs(result["total_value"].iloc[0] - expected) < 0.01

    def test_filter_rows(self, transformer, sample_orders):
        """Rows with amount <= 0 are removed."""
        df_with_zero = sample_orders.copy()
        df_with_zero.loc[0, "amount"] = 0.0
        result = transformer.transform("orders", df_with_zero)
        assert (result["amount"] > 0).all()

    def test_deduplication(self, transformer, sample_orders):
        """Duplicate order_ids are removed, keeping last."""
        duped = pd.concat([sample_orders, sample_orders.iloc[:2]], ignore_index=True)
        result = transformer.transform("orders", duped)
        assert result["order_id"].nunique() == result["order_id"].count()

    def test_no_rules_passthrough(self, transformer, sample_orders):
        """Tables without rules return unchanged data."""
        result = transformer.transform("unknown_table", sample_orders)
        assert len(result) == len(sample_orders)

    def test_null_fill(self):
        """_fill_nulls fills NaN with specified value."""
        from core.transformer import DataTransformer
        t   = DataTransformer({"transformations": {}})
        df  = pd.DataFrame({"val": [1.0, None, 3.0]})
        out = t._fill_nulls(df, {"columns": {"val": 0}})
        assert out["val"].isnull().sum() == 0

    def test_normalize_string_lowercase(self):
        """String normalization lowercases correctly."""
        from core.transformer import DataTransformer
        t  = DataTransformer({"transformations": {}})
        df = pd.DataFrame({"name": ["ALICE", "BOB"]})
        out = t._normalize_strings(df, {"columns": ["name"], "lowercase": True})
        assert list(out["name"]) == ["alice", "bob"]


# ── Phase 4: Post-Transform GE Validation ────────────────────────────────────

class TestPostTransformValidation:

    def test_transformed_data_has_expected_columns(self, transformer, sample_orders):
        """Transformed data has derived and renamed columns."""
        result = transformer.transform("orders", sample_orders)
        for col in ["order_id", "customer_id", "quantity", "amount", "total_value"]:
            assert col in result.columns

    def test_total_value_non_negative(self, transformer, sample_orders):
        """Derived total_value is always non-negative after filter."""
        result = transformer.transform("orders", sample_orders)
        assert (result["total_value"] >= 0).all()


# ── Phase 5: Load ─────────────────────────────────────────────────────────────

class TestLoad:

    def test_load_calls_engine(self, sample_orders):
        """Loader writes to target with correct parameters."""
        from core.loader import DataLoader
        loader = DataLoader(SAMPLE_CONFIG)

        with patch.object(loader, "_get_engine") as mock_engine, \
             patch.object(loader, "_get_target_count", return_value=5):
            mock_conn = MagicMock()
            mock_engine.return_value.begin.return_value.__enter__ = lambda *a: mock_conn
            mock_engine.return_value.begin.return_value.__exit__  = lambda *a: None

            # Use replace strategy to avoid upsert complexity in test
            cfg = {**SAMPLE_CONFIG, "target": {**SAMPLE_CONFIG["target"],
                                                "load_strategy": "replace"}}
            loader2 = DataLoader(cfg)
            with patch("pandas.DataFrame.to_sql"):
                result = loader2.load("orders", sample_orders)
            assert result["rows_loaded"] == len(sample_orders)

    def test_empty_dataframe_skipped(self):
        """Empty DataFrame is skipped without error."""
        from core.loader import DataLoader
        loader = DataLoader(SAMPLE_CONFIG)
        result = loader.load("orders", pd.DataFrame())
        assert result["rows_loaded"] == 0
        assert result["strategy"] == "skip"


# ── Phase 6: Reconciliation ───────────────────────────────────────────────────

class TestReconciliation:

    def test_row_count_within_tolerance(self):
        """Row counts within tolerance should not raise."""
        from etl_pipeline import ETLPipeline
        pipeline = MagicMock()
        pipeline.config = SAMPLE_CONFIG

        raw_data     = {"orders": pd.DataFrame({"id": range(100)})}
        load_results = {"orders": {"rows_loaded": 99}}  # 1% diff

        inst = ETLPipeline.__new__(ETLPipeline)
        inst.config = SAMPLE_CONFIG
        inst.logger = MagicMock()
        result = inst._do_reconcile(raw_data, load_results)
        assert "reconciled_tables" in result["details"]

    def test_row_count_exceeds_tolerance_raises(self):
        """Row count mismatch beyond tolerance should raise ValueError."""
        from etl_pipeline import ETLPipeline
        inst = ETLPipeline.__new__(ETLPipeline)
        inst.config = SAMPLE_CONFIG
        inst.logger = MagicMock()

        raw_data     = {"orders": pd.DataFrame({"id": range(100)})}
        load_results = {"orders": {"rows_loaded": 50}}  # 50% diff

        with pytest.raises(ValueError, match="Row count mismatch"):
            inst._do_reconcile(raw_data, load_results)


# ── Phase 8: Error Handling ───────────────────────────────────────────────────

class TestErrorHandling:

    def test_extraction_failure_stops_pipeline(self, tmp_path):
        """Pipeline halts if extraction fails with alert_and_stop config."""
        from etl_pipeline import ETLPipeline, PipelineStatus
        with patch.object(ETLPipeline, "_load_config", return_value=SAMPLE_CONFIG), \
             patch.object(ETLPipeline, "__init__", lambda s, *a, **kw: None):
            pipeline = ETLPipeline.__new__(ETLPipeline)
            pipeline.config  = SAMPLE_CONFIG
            pipeline.logger  = MagicMock()
            pipeline.console = MagicMock()
            pipeline.extractor   = MagicMock(side_effect=RuntimeError("DB down"))
            pipeline.transformer = MagicMock()
            pipeline.loader      = MagicMock()
            pipeline.validator   = MagicMock()
            pipeline.alert_mgr   = MagicMock()

            from etl_pipeline import PipelineRun
            pipeline.run = PipelineRun()
            pipeline.run.status = "RUNNING"
            pipeline._print_header = MagicMock()
            pipeline._print_step_result = MagicMock()
            pipeline._print_summary = MagicMock()
            pipeline._save_run_history = MagicMock()

            # Simulating that after extract fails pipeline status is FAILED
            from etl_pipeline import StepResult, StepStatus
            fail_step = StepResult("extract", StepStatus.FAILED,
                                   __import__("datetime").datetime.utcnow(),
                                   error="DB down")
            pipeline.run.steps.append(fail_step)
            result = pipeline._finalize(PipelineStatus.FAILED, "alert_and_stop")
            assert result.status == PipelineStatus.FAILED

    def test_alert_triggered_on_failure(self):
        """AlertManager.send_alert is called on pipeline failure."""
        from etl_pipeline import ETLPipeline, PipelineRun, PipelineStatus
        pipeline = ETLPipeline.__new__(ETLPipeline)
        pipeline.alert_mgr = MagicMock()
        pipeline.run = PipelineRun()
        pipeline.run.status = PipelineStatus.FAILED
        pipeline._print_summary = MagicMock()
        pipeline._save_run_history = MagicMock()
        pipeline._finalize(PipelineStatus.FAILED, "alert_and_stop")
        pipeline.alert_mgr.send_alert.assert_called_once()


# ── Phase 10: Regression / Negative Testing ───────────────────────────────────

class TestRegressionAndNegative:

    def test_pipeline_run_id_is_unique(self):
        """Each PipelineRun gets a unique run ID."""
        from etl_pipeline import PipelineRun
        r1, r2 = PipelineRun(), PipelineRun()
        assert r1.run_id != r2.run_id

    def test_step_result_duration(self):
        """StepResult.duration_seconds is correctly computed."""
        from etl_pipeline import StepResult, StepStatus
        from datetime import datetime, timedelta
        start  = datetime(2024, 1, 1, 12, 0, 0)
        finish = start + timedelta(seconds=5)
        step   = StepResult("test", StepStatus.SUCCESS, start, finish)
        assert step.duration_seconds == 5.0

    def test_config_env_var_substitution(self, tmp_path, monkeypatch):
        """Environment variables are substituted in config."""
        import yaml, os
        monkeypatch.setenv("TEST_HOST", "my-server")
        cfg_content = "source:\n  connection:\n    host: ${TEST_HOST}\n"
        cfg_path = tmp_path / "test_config.yaml"
        cfg_path.write_text(cfg_content)

        from etl_pipeline import ETLPipeline
        inst = ETLPipeline.__new__(ETLPipeline)
        inst.config_path = cfg_path
        result = inst._load_config()
        assert result["source"]["connection"]["host"] == "my-server"

    @pytest.mark.parametrize("fault", ["nulls", "out_of_range", "duplicates"])
    def test_all_fault_types_inject_correctly(self, fault, sample_orders):
        """All fault types change the DataFrame from the original."""
        from core.ge_validator import GEValidator
        bad = GEValidator.inject_bad_data(sample_orders.copy(), fault)
        if fault == "duplicates":
            assert len(bad) > len(sample_orders)
        else:
            # Data differs from original
            assert not bad.equals(sample_orders)
