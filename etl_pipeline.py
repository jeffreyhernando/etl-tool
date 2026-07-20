"""
etl_pipeline.py
───────────────
Main ETL pipeline orchestrator.
Runs: Extract → Validate (Raw) → Transform → Validate (Post-Transform) → Load
"""

from __future__ import annotations

import json
import time
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import yaml
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table
from rich.text import Text

from core.extractor import DataExtractor
from core.transformer import DataTransformer
from core.loader import DataLoader
from core.ge_validator import GEValidator
from core.alerting import AlertManager
from core.logger import get_logger

# ── Enums ─────────────────────────────────────────────────────────────────────

class StepStatus(str, Enum):
    PENDING   = "PENDING"
    RUNNING   = "RUNNING"
    SUCCESS   = "SUCCESS"
    FAILED    = "FAILED"
    SKIPPED   = "SKIPPED"
    WARNING   = "WARNING"


class PipelineStatus(str, Enum):
    IDLE      = "IDLE"
    RUNNING   = "RUNNING"
    SUCCESS   = "SUCCESS"
    FAILED    = "FAILED"
    PARTIAL   = "PARTIAL"


# ── Data Classes ──────────────────────────────────────────────────────────────

@dataclass
class StepResult:
    step_name:    str
    status:       StepStatus
    started_at:   datetime
    finished_at:  Optional[datetime] = None
    rows_in:      int = 0
    rows_out:     int = 0
    ge_passed:    Optional[int] = None
    ge_failed:    Optional[int] = None
    ge_warnings:  Optional[int] = None
    error:        Optional[str] = None
    details:      Dict[str, Any] = field(default_factory=dict)

    @property
    def duration_seconds(self) -> float:
        if self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return 0.0

    def to_dict(self) -> Dict:
        return {
            "step_name":   self.step_name,
            "status":      self.status.value,
            "started_at":  self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "duration_s":  round(self.duration_seconds, 2),
            "rows_in":     self.rows_in,
            "rows_out":    self.rows_out,
            "ge_passed":   self.ge_passed,
            "ge_failed":   self.ge_failed,
            "ge_warnings": self.ge_warnings,
            "error":       self.error,
            "details":     self.details,
        }


@dataclass
class PipelineRun:
    run_id:     str = field(default_factory=lambda: str(uuid.uuid4())[:8].upper())
    started_at: datetime = field(default_factory=datetime.utcnow)
    status:     PipelineStatus = PipelineStatus.IDLE
    steps:      List[StepResult] = field(default_factory=list)
    config:     Dict = field(default_factory=dict)
    finished_at: Optional[datetime] = None
    triggered_by: str = "scheduler"

    @property
    def duration_seconds(self) -> float:
        if self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return 0.0

    @property
    def summary(self) -> Dict:
        passed = sum(1 for s in self.steps if s.status == StepStatus.SUCCESS)
        failed = sum(1 for s in self.steps if s.status == StepStatus.FAILED)
        total_rows_loaded = sum(
            s.rows_out for s in self.steps if s.step_name == "load"
        )
        total_ge_passed  = sum(s.ge_passed  or 0 for s in self.steps)
        total_ge_failed  = sum(s.ge_failed  or 0 for s in self.steps)
        return {
            "run_id":           self.run_id,
            "status":           self.status.value,
            "started_at":       self.started_at.isoformat(),
            "finished_at":      self.finished_at.isoformat() if self.finished_at else None,
            "duration_s":       round(self.duration_seconds, 2),
            "steps_passed":     passed,
            "steps_failed":     failed,
            "total_rows_loaded":total_rows_loaded,
            "ge_passed":        total_ge_passed,
            "ge_failed":        total_ge_failed,
            "triggered_by":     self.triggered_by,
        }

    def to_dict(self) -> Dict:
        return {
            **self.summary,
            "steps": [s.to_dict() for s in self.steps],
        }


# ── Pipeline Orchestrator ─────────────────────────────────────────────────────

class ETLPipeline:
    """
    Orchestrates the full ETL workflow:
      1. Extract
      2. Validate Raw (Great Expectations)
      3. Transform
      4. Validate Transformed (Great Expectations)
      5. Load
      6. Row-count reconciliation
      7. Report & Alert
    """

    def __init__(self, config_path: str = "config/pipeline_config.yaml"):
        self.config_path = Path(config_path)
        self.config      = self._load_config()
        self.logger      = get_logger("ETLPipeline")
        self.console     = Console()
        self.alert_mgr   = AlertManager(self.config.get("alerts", {}))
        self.run: Optional[PipelineRun] = None

        # Wire up components
        self.extractor   = DataExtractor(self.config)
        self.transformer = DataTransformer(self.config)
        self.loader      = DataLoader(self.config)
        self.validator   = GEValidator(self.config.get("great_expectations", {}))

    # ── Config ────────────────────────────────────────────────────────────────

    def _load_config(self) -> Dict:
        import os
        from dotenv import load_dotenv
        load_dotenv()
        with open(self.config_path) as f:
            raw = f.read()
        # Expand env vars like ${VAR_NAME}
        import re
        for match in re.finditer(r"\$\{(\w+)\}", raw):
            var = match.group(1)
            raw = raw.replace(match.group(0), os.environ.get(var, f"<{var}>"))
        return yaml.safe_load(raw)

    # ── Step Runner ───────────────────────────────────────────────────────────

    def _run_step(self, step_name: str, fn, *args, **kwargs) -> StepResult:
        result = StepResult(step_name=step_name, status=StepStatus.RUNNING,
                            started_at=datetime.utcnow())
        self.logger.info(f"[{self.run.run_id}] Starting: {step_name}")
        try:
            outcome = fn(*args, **kwargs)
            if isinstance(outcome, dict):
                result.rows_in      = outcome.get("rows_in", 0)
                result.rows_out     = outcome.get("rows_out", 0)
                result.ge_passed    = outcome.get("ge_passed")
                result.ge_failed    = outcome.get("ge_failed")
                result.ge_warnings  = outcome.get("ge_warnings")
                result.details      = outcome.get("details", {})
                result.status       = (
                    StepStatus.WARNING if outcome.get("has_warnings")
                    else StepStatus.SUCCESS
                )
            else:
                result.status = StepStatus.SUCCESS
        except Exception as exc:
            result.status = StepStatus.FAILED
            result.error  = str(exc)
            self.logger.error(
                f"[{self.run.run_id}] Step '{step_name}' failed: {exc}\n"
                + traceback.format_exc()
            )
        finally:
            result.finished_at = datetime.utcnow()

        self.run.steps.append(result)
        self._print_step_result(result)
        return result

    # ── Main Run ──────────────────────────────────────────────────────────────

    def run_pipeline(self, triggered_by: str = "manual") -> PipelineRun:
        self.run = PipelineRun(triggered_by=triggered_by)
        self.run.status = PipelineStatus.RUNNING
        self.run.config = {"pipeline_name": self.config["pipeline"]["name"]}

        self._print_header()
        cfg = self.config["pipeline"]
        fail_mode = cfg.get("on_failure", "alert_and_stop")

        tables   = self.config["source"]["tables"]
        raw_data: Dict[str, pd.DataFrame]         = {}
        transformed: Dict[str, pd.DataFrame]      = {}
        load_results: Dict[str, Dict]              = {}

        # ── STEP 1: EXTRACT ───────────────────────────────────────────────────
        step = self._run_step("extract", self._do_extract, tables, raw_data)
        if step.status == StepStatus.FAILED:
            return self._finalize(PipelineStatus.FAILED, fail_mode)

        # ── STEP 2: VALIDATE RAW ──────────────────────────────────────────────
        step = self._run_step("validate_raw", self._do_validate,
                              raw_data, "raw")
        if step.status == StepStatus.FAILED and fail_mode == "alert_and_stop":
            return self._finalize(PipelineStatus.FAILED, fail_mode)

        # ── STEP 3: TRANSFORM ─────────────────────────────────────────────────
        step = self._run_step("transform", self._do_transform,
                              raw_data, transformed)
        if step.status == StepStatus.FAILED:
            return self._finalize(PipelineStatus.FAILED, fail_mode)

        # ── STEP 4: VALIDATE TRANSFORMED ──────────────────────────────────────
        step = self._run_step("validate_transformed", self._do_validate,
                              transformed, "transformed")
        if step.status == StepStatus.FAILED and fail_mode == "alert_and_stop":
            return self._finalize(PipelineStatus.FAILED, fail_mode)

        # ── STEP 5: LOAD ──────────────────────────────────────────────────────
        step = self._run_step("load", self._do_load, transformed, load_results)
        if step.status == StepStatus.FAILED:
            return self._finalize(PipelineStatus.FAILED, fail_mode)

        # ── STEP 6: RECONCILE ROW COUNTS ──────────────────────────────────────
        step = self._run_step("reconcile", self._do_reconcile,
                              raw_data, load_results)

        # ── STEP 7: GENERATE REPORT ───────────────────────────────────────────
        self._run_step("report", self._do_report)

        any_failed = any(s.status == StepStatus.FAILED for s in self.run.steps)
        any_warn   = any(s.status == StepStatus.WARNING for s in self.run.steps)
        final = (PipelineStatus.FAILED if any_failed
                 else PipelineStatus.PARTIAL if any_warn
                 else PipelineStatus.SUCCESS)
        return self._finalize(final, fail_mode)

    # ── Step Implementations ──────────────────────────────────────────────────

    def _do_extract(self, tables, raw_data: dict) -> dict:
        total_rows = 0
        for tbl in tables:
            df = self.extractor.extract_table(tbl["name"],
                                              incremental_col=tbl.get("incremental_column"),
                                              strategy=tbl.get("incremental_strategy", "full_refresh"))
            raw_data[tbl["name"]] = df
            total_rows += len(df)
            self.logger.info(f"  Extracted {len(df):,} rows from '{tbl['name']}'")
        return {"rows_out": total_rows,
                "details": {t: len(raw_data[t]) for t in raw_data}}

    def _do_validate(self, dataframes: dict, phase: str) -> dict:
        results = self.validator.run_checkpoint(dataframes, phase)
        passed  = sum(r["passed"] for r in results.values())
        failed  = sum(r["failed"] for r in results.values())
        warns   = sum(r.get("warnings", 0) for r in results.values())
        has_fail = failed > 0
        return {
            "ge_passed":    passed,
            "ge_failed":    failed,
            "ge_warnings":  warns,
            "has_warnings": has_fail,
            "details":      {t: r["summary"] for t, r in results.items()},
        }

    def _do_transform(self, raw_data: dict, transformed: dict) -> dict:
        total_in = total_out = 0
        for name, df in raw_data.items():
            total_in += len(df)
            tdf = self.transformer.transform(name, df)
            transformed[name] = tdf
            total_out += len(tdf)
            self.logger.info(
                f"  Transformed '{name}': {len(df):,} → {len(tdf):,} rows"
            )
        return {"rows_in": total_in, "rows_out": total_out,
                "details": {t: {"in": len(raw_data[t]),
                                "out": len(transformed[t])}
                            for t in transformed}}

    def _do_load(self, transformed: dict, load_results: dict) -> dict:
        total = 0
        for name, df in transformed.items():
            res = self.loader.load(name, df)
            load_results[name] = res
            total += res.get("rows_loaded", 0)
        return {"rows_out": total,
                "details": load_results}

    def _do_reconcile(self, raw_data: dict, load_results: dict) -> dict:
        tol = self.config["validation"].get("row_count_tolerance_pct", 1.0)
        issues = []
        for name in raw_data:
            src  = len(raw_data[name])
            tgt  = load_results.get(name, {}).get("rows_loaded", 0)
            diff = abs(src - tgt)
            pct  = (diff / src * 100) if src else 0
            if pct > tol:
                issues.append(f"{name}: src={src} tgt={tgt} diff={pct:.1f}%")
        if issues:
            raise ValueError("Row count mismatch beyond tolerance: " + "; ".join(issues))
        return {"details": {"reconciled_tables": list(raw_data.keys())}}

    def _do_report(self) -> dict:
        report_dir = Path(self.config["reporting"]["output_dir"])
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / f"run_{self.run.run_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        report_path.write_text(json.dumps(self.run.to_dict(), indent=2))
        if self.config["reporting"].get("generate_ge_docs", True):
            self.validator.build_data_docs()
        return {"details": {"report_path": str(report_path)}}

    # ── Finalize ──────────────────────────────────────────────────────────────

    def _finalize(self, status: PipelineStatus, fail_mode: str) -> PipelineRun:
        self.run.status      = status
        self.run.finished_at = datetime.utcnow()
        self._print_summary()
        if status != PipelineStatus.SUCCESS:
            self.alert_mgr.send_alert(self.run)
        self._save_run_history()
        return self.run

    def _save_run_history(self):
        history_path = Path("logs/run_history.jsonl")
        history_path.parent.mkdir(parents=True, exist_ok=True)
        with open(history_path, "a") as f:
            f.write(json.dumps(self.run.summary) + "\n")

    # ── Console Output ────────────────────────────────────────────────────────

    def _print_header(self):
        name = self.config["pipeline"]["name"]
        self.console.print(Panel(
            f"[bold cyan]{name}[/bold cyan]\n"
            f"Run ID: [yellow]{self.run.run_id}[/yellow]  │  "
            f"Started: [dim]{self.run.started_at.strftime('%Y-%m-%d %H:%M:%S UTC')}[/dim]",
            title="[bold]ETL Pipeline[/bold]", border_style="cyan"
        ))

    def _print_step_result(self, result: StepResult):
        icons = {
            StepStatus.SUCCESS: "[green]✓[/green]",
            StepStatus.FAILED:  "[red]✗[/red]",
            StepStatus.WARNING: "[yellow]⚠[/yellow]",
            StepStatus.RUNNING: "[blue]…[/blue]",
        }
        icon = icons.get(result.status, "?")
        dur  = f"{result.duration_seconds:.1f}s"
        msg  = f"{icon} {result.step_name:<25} [{dur}]"
        if result.rows_out:
            msg += f"  rows={result.rows_out:,}"
        if result.ge_failed:
            msg += f"  [red]GE_FAIL={result.ge_failed}[/red]"
        if result.error:
            msg += f"\n   [red]{result.error[:120]}[/red]"
        self.console.print(msg)

    def _print_summary(self):
        s = self.run.summary
        color = {"SUCCESS": "green", "FAILED": "red", "PARTIAL": "yellow"}.get(
            s["status"], "white"
        )
        tbl = Table(title="Pipeline Run Summary", border_style="dim")
        tbl.add_column("Metric", style="bold")
        tbl.add_column("Value")
        tbl.add_row("Run ID",         s["run_id"])
        tbl.add_row("Status",         f"[{color}]{s['status']}[/{color}]")
        tbl.add_row("Duration",       f"{s['duration_s']}s")
        tbl.add_row("Steps Passed",   str(s["steps_passed"]))
        tbl.add_row("Steps Failed",   str(s["steps_failed"]))
        tbl.add_row("Rows Loaded",    f"{s['total_rows_loaded']:,}")
        tbl.add_row("GE Passed",      str(s["ge_passed"]))
        tbl.add_row("GE Failed",      str(s["ge_failed"]))
        self.console.print(tbl)


# ── CLI Entry Point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import click

    @click.command()
    @click.option("--config", default="config/pipeline_config.yaml",
                  help="Path to pipeline config YAML")
    @click.option("--trigger", default="manual",
                  help="Who/what triggered this run")
    def main(config: str, trigger: str):
        """Run the ETL pipeline."""
        pipeline = ETLPipeline(config_path=config)
        run = pipeline.run_pipeline(triggered_by=trigger)
        exit(0 if run.status == PipelineStatus.SUCCESS else 1)

    main()
