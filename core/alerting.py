"""
core/alerting.py
────────────────
Sends pipeline failure/warning alerts via Email, Slack, or Teams.
"""

from __future__ import annotations

import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List

from core.logger import get_logger

logger = get_logger("AlertManager")


class AlertManager:
    def __init__(self, alert_config: Dict):
        self.config = alert_config

    def send_alert(self, run) -> None:
        """Send alerts for a failed or partial pipeline run."""
        summary = run.summary
        subject = f"[ETL ALERT] {summary['status']} — Run {summary['run_id']}"

        body = self._build_body(run)

        if self.config.get("email", {}).get("enabled"):
            self._send_email(subject, body)

        if self.config.get("slack", {}).get("enabled"):
            self._send_slack(summary, run)

        if self.config.get("teams", {}).get("enabled"):
            self._send_teams(summary, run)

    # ── Email ─────────────────────────────────────────────────────────────────

    def _send_email(self, subject: str, body: str):
        cfg = self.config["email"]
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = cfg["from"]
        msg["To"]      = ", ".join(cfg["to"])
        msg.attach(MIMEText(body, "html"))

        try:
            with smtplib.SMTP(cfg["smtp_host"], cfg["smtp_port"]) as smtp:
                smtp.starttls()
                if cfg.get("username"):
                    smtp.login(cfg["username"], cfg["password"])
                smtp.sendmail(cfg["from"], cfg["to"], msg.as_string())
            logger.info(f"Alert email sent to {cfg['to']}")
        except Exception as e:
            logger.error(f"Failed to send alert email: {e}")

    # ── Slack ─────────────────────────────────────────────────────────────────

    def _send_slack(self, summary: Dict, run):
        import requests
        cfg     = self.config["slack"]
        color   = "#FF0000" if summary["status"] == "FAILED" else "#FFA500"
        payload = {
            "channel": cfg.get("channel", "#data-alerts"),
            "attachments": [{
                "color":  color,
                "title":  f"ETL Pipeline — {summary['status']}",
                "fields": [
                    {"title": "Run ID",        "value": summary["run_id"],           "short": True},
                    {"title": "Duration",       "value": f"{summary['duration_s']}s", "short": True},
                    {"title": "Rows Loaded",    "value": str(summary["total_rows_loaded"]), "short": True},
                    {"title": "GE Failures",    "value": str(summary["ge_failed"]),   "short": True},
                    {"title": "Steps Failed",   "value": str(summary["steps_failed"]), "short": True},
                ],
                "footer": "ETL Automation",
                "ts":     int(__import__("time").time()),
            }],
        }
        try:
            resp = requests.post(cfg["webhook_url"], json=payload, timeout=10)
            resp.raise_for_status()
            logger.info("Slack alert sent")
        except Exception as e:
            logger.error(f"Slack alert failed: {e}")

    # ── Teams ─────────────────────────────────────────────────────────────────

    def _send_teams(self, summary: Dict, run):
        import requests
        cfg = self.config["teams"]
        color = "FF0000" if summary["status"] == "FAILED" else "FFA500"
        payload = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": color,
            "summary": f"ETL Pipeline {summary['status']}",
            "sections": [{
                "activityTitle": f"ETL Pipeline — {summary['status']}",
                "facts": [
                    {"name": "Run ID",      "value": summary["run_id"]},
                    {"name": "Status",      "value": summary["status"]},
                    {"name": "Duration",    "value": f"{summary['duration_s']}s"},
                    {"name": "Rows Loaded", "value": str(summary["total_rows_loaded"])},
                    {"name": "GE Failed",   "value": str(summary["ge_failed"])},
                ],
            }],
        }
        try:
            resp = requests.post(cfg["webhook_url"], json=payload, timeout=10)
            resp.raise_for_status()
            logger.info("Teams alert sent")
        except Exception as e:
            logger.error(f"Teams alert failed: {e}")

    # ── HTML Body ─────────────────────────────────────────────────────────────

    def _build_body(self, run) -> str:
        s = run.summary
        rows = "".join(
            f"<tr><td>{st['step_name']}</td>"
            f"<td style='color:{'green' if st['status']=='SUCCESS' else 'red'}'>"
            f"{st['status']}</td>"
            f"<td>{st['duration_s']}s</td>"
            f"<td>{st.get('rows_out', '')}</td>"
            f"<td>{st.get('error', '') or ''}</td></tr>"
            for st in run.to_dict()["steps"]
        )
        return f"""
        <html><body style="font-family:monospace">
        <h2>ETL Pipeline Alert — {s['status']}</h2>
        <table border="1" cellpadding="5">
          <tr><th>Metric</th><th>Value</th></tr>
          <tr><td>Run ID</td><td>{s['run_id']}</td></tr>
          <tr><td>Status</td><td>{s['status']}</td></tr>
          <tr><td>Duration</td><td>{s['duration_s']}s</td></tr>
          <tr><td>Rows Loaded</td><td>{s['total_rows_loaded']:,}</td></tr>
          <tr><td>GE Passed</td><td>{s['ge_passed']}</td></tr>
          <tr><td>GE Failed</td><td>{s['ge_failed']}</td></tr>
        </table>
        <br>
        <h3>Step Details</h3>
        <table border="1" cellpadding="5">
          <tr><th>Step</th><th>Status</th><th>Duration</th>
              <th>Rows</th><th>Error</th></tr>
          {rows}
        </table>
        </body></html>
        """
