"""
ETL Engine v9.2 - UPDATED ADDRESS + ADDRESS2 BUSINESS RULES
Changes from v9.1:
  - address_normalized_strict: now strips suite/unit/building/floor suffixes
    before comparing, so "1400 E BOULDER STREET,SUITE 2508" matches "1400 E BOULDER ST"
  - address2_suite_ignored: new rule — treats null/empty source vs suite-only target
    as MATCHED (suite data is expected to live in address field on source side)
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from starlette.background import BackgroundTask
from pydantic import BaseModel

import pyodbc
import pandas as pd
import uuid
import os
import traceback
import json
import sqlite3
import re
from datetime import datetime, date as date_cls

from typing import Dict, Any, List, Optional
from threading import Thread, Lock, Event

app = FastAPI(
    title="ETL Engine Enterprise - Optimized (v9.2 with Business Rules)",
    version="9.2-BR"
)

# ================= CORS =================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ================= STORAGE =================

os.makedirs("outputs", exist_ok=True)

jobs = {}
cancel_events = {}
execution_signatures = set()
lock = Lock()

# ================= PERSISTENT DB =================
# ETL_DB_PATH lets deployments point this at a persistent volume/disk so the
# history database survives redeploys instead of living in the (ephemeral)
# app directory. Defaults to the old relative path for local dev.

DB_FILE = os.environ.get("ETL_DB_PATH", "etl_history.db")
_db_dir = os.path.dirname(DB_FILE)
if _db_dir:
    os.makedirs(_db_dir, exist_ok=True)

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS execution_history (
            job_id            TEXT PRIMARY KEY,
            test_case         TEXT,
            execution_date    TEXT,
            status            TEXT,
            accuracy          REAL,
            key_match_rate    REAL,
            field_accuracy    REAL,
            total_records     INTEGER,
            matched           INTEGER,
            mismatched        INTEGER,
            source_only       INTEGER,
            target_only       INTEGER,
            null_mismatches   INTEGER,
            source_duplicates INTEGER,
            target_duplicates INTEGER,
            src_row_count     INTEGER,
            tgt_row_count     INTEGER,
            keys_in_both      INTEGER,
            keys_src_only     INTEGER,
            keys_tgt_only     INTEGER
        )
    """)

    existing = {row[1] for row in cur.execute("PRAGMA table_info(execution_history)").fetchall()}
    for col, typedef in [
        ("key_match_rate",    "REAL    DEFAULT 0"),
        ("field_accuracy",    "REAL    DEFAULT 0"),
        ("keys_in_both",      "INTEGER DEFAULT 0"),
        ("keys_src_only",     "INTEGER DEFAULT 0"),
        ("keys_tgt_only",     "INTEGER DEFAULT 0"),
        # v9.3 Observability: quality-gate verdict + serialized anomaly alerts
        ("gate_status",       "TEXT    DEFAULT ''"),   # PASS | WARNING | FAIL | ''
        ("alerts",            "TEXT    DEFAULT ''"),    # JSON list of anomaly dicts
    ]:
        if col not in existing:
            cur.execute(f"ALTER TABLE execution_history ADD COLUMN {col} {typedef}")

    # v9.3 Observability: user-defined data contracts / quality gates
    cur.execute("""
        CREATE TABLE IF NOT EXISTS quality_gates (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            test_case    TEXT,            -- '' or '*' = applies to every test case
            metric       TEXT NOT NULL,   -- field_accuracy | key_match_rate | null_rate
                                          -- | mismatch_count | row_count_diff_pct
            field        TEXT DEFAULT '', -- optional: column name for per-field metrics
            operator     TEXT NOT NULL,   -- gte | lte | gt | lt | eq
            threshold    REAL NOT NULL,
            severity     TEXT DEFAULT 'fail',  -- fail | warn
            enabled      INTEGER DEFAULT 1,
            created_date TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS execution_results (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id       TEXT NOT NULL,
            key_value    TEXT,
            field        TEXT,
            source_value TEXT,
            target_value TEXT,
            status       TEXT,
            FOREIGN KEY (job_id) REFERENCES execution_history(job_id)
        )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_job_results ON execution_results(job_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_job_status  ON execution_results(job_id, status)")

    conn.commit()
    conn.close()

init_db()


# ================= RELOAD COMPLETED JOBS ON STARTUP =================

def reload_jobs_from_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        SELECT job_id, status, accuracy, key_match_rate, field_accuracy,
               total_records, matched, mismatched, source_only, target_only,
               null_mismatches, source_duplicates, target_duplicates,
               src_row_count, tgt_row_count,
               keys_in_both, keys_src_only, keys_tgt_only
        FROM execution_history
        ORDER BY execution_date DESC
        LIMIT 500
    """)
    rows = cur.fetchall()
    conn.close()

    restored = 0
    for row in rows:
        job_id = row[0]
        if job_id not in jobs:
            jobs[job_id] = {
                "status":   row[1],
                "progress": 100,
                "error":    None,
                "stats": {
                    "accuracy":          row[2],
                    "key_match_rate":    row[3],
                    "field_accuracy":    row[4],
                    "total":             row[5],
                    "matched":           row[6],
                    "mismatched":        row[7],
                    "source_only":       row[8],
                    "target_only":       row[9],
                    "null_mismatches":   row[10],
                    "source_duplicates": row[11],
                    "target_duplicates": row[12],
                    "src_row_count":     row[13],
                    "tgt_row_count":     row[14],
                    "keys_in_both":      row[15] or 0,
                    "keys_src_only":     row[16] or 0,
                    "keys_tgt_only":     row[17] or 0,
                }
            }
            restored += 1

    print(f"[startup] Restored {restored} jobs from SQLite.")

reload_jobs_from_db()


# =========================================================
#  ADDRESS NORMALIZATION  (v9.2 -- suite-strip + abbrev expand)
# =========================================================

# Regex that matches trailing suite / unit / building / floor info.
# Covers patterns like:
#   ,SUITE 2508   , STE 100B   ,UNIT 101   ,BUILDING 1   ,BLDG B
#   ,1ST FLOOR    ,FL 3        ,APT 4C     ,# 200
# The comma before is optional so bare "SUITE …" at end also strips.
_SUITE_SUFFIX_RE = re.compile(
    r'(?:'
    # Pattern A: comma + suite keyword + optional value  e.g. ",SUITE 2508" ",STE A" ",BUILDING B"
    r'[,]\s*(?:SUITE|STE|UNIT|APT|APARTMENT|BUILDING|BLDG|FLOOR|FL|RM|ROOM|#|NO\.?)[\s#]*[\w\-/]*(?:\s+[\w\-/]+)*'
    r'|'
    # Pattern B: comma + ordinal + floor/building keyword  e.g. ",1ST FLOOR" ",2ND FLOOR"
    r'[,]\s*\d+(?:ST|ND|RD|TH)\s+(?:FLOOR|FL|BUILDING|BLDG)'
    r'|'
    # Pattern C: space-separated, but ONLY if suite keyword is followed by a digit
    #            Prevents "DR" / "ST" street types from being stripped
    r'\s+(?:SUITE|STE|UNIT|APT|APARTMENT|BUILDING|BLDG|FLOOR|FL|RM|ROOM|#|NO\.?)\s+[\d][\w\-/]*(?:\s+[\w\-/]+)*'
    r'|'
    # Pattern D: space + ordinal + FLOOR at end (no comma)  e.g. " 1ST FLOOR"
    r'\s+\d+(?:ST|ND|RD|TH)\s+(?:FLOOR|FL)'
    r')'
    r'\s*$',
    re.IGNORECASE
)

# Also strip a lone trailing comma (e.g. "13445 VOYAGER PARKWAY,")
_TRAILING_COMMA_RE = re.compile(r',\s*$')

# Street-type abbreviation expansions (applied before comparison)
_ABBREV_MAP = [
    (r'\bAVE\.?\b',      'AVENUE'),
    (r'\bST\.?\b',       'STREET'),
    (r'\bDR\.?\b',       'DRIVE'),
    (r'\bRD\.?\b',       'ROAD'),
    (r'\bPL\.?\b',       'PLACE'),
    (r'\bBLVD\.?\b',     'BOULEVARD'),
    (r'\bPKWY\.?\b',     'PARKWAY'),
    (r'\bCT\.?\b',       'COURT'),
    (r'\bLN\.?\b',       'LANE'),
    (r'\bCIR\.?\b',      'CIRCLE'),
    (r'\bTER\.?\b',      'TERRACE'),
    (r'\bHWY\.?\b',      'HIGHWAY'),
    (r'\bVW\.?\b',       'VIEW'),
    (r'\bPT\.?\b',       'POINT'),
    (r'\bMT\.?\b',       'MOUNT'),
    (r'\bFT\.?\b',       'FORT'),
    (r'\bGRV\.?\b',      'GROVE'),
    (r'\bPLZ\.?\b',      'PLAZA'),
    (r'\bE\.?\b',        'EAST'),
    (r'\bW\.?\b',        'WEST'),
    (r'\bN\.?\b',        'NORTH'),
    (r'\bS\.?\b',        'SOUTH'),
]
_ABBREV_COMPILED = [(re.compile(p, re.IGNORECASE), r) for p, r in _ABBREV_MAP]

# =========================================================
#  SUITE-ONLY VALUE DETECTOR (for address2 rule)
# =========================================================
_SUITE_ONLY_RE = re.compile(
    r'^\s*('
    r'(?:SUITE|STE|UNIT|APT|APARTMENT|BUILDING|BLDG|FLOOR|FL|RM|ROOM|#|NO\.?)[\s#]*[\w\-/]*(?:\s+[\w\-/]+)*'
    r'|'
    r'\d+(?:ST|ND|RD|TH)\s+(?:FLOOR|FL)'    # 1ST FLOOR, 3RD FLOOR
    r'|'
    r'\d+(?:TH|ST|ND|RD)?\s+FL'             # 6TH FL
    r'|'
    r'[A-Z]{1,3}\d{2,}'                      # F1051, M1921
    r'|'
    r'\d+\.\d+(?:\s+\d+\.\d+)*'             # 1.220 1.222
    r')\s*$',
    re.IGNORECASE
)

def is_suite_only_value(val: str) -> bool:
    """True if value is purely secondary-unit info: STE 100, F1051, 1ST FLOOR, 6TH FL, etc."""
    return bool(_SUITE_ONLY_RE.match(str(val).strip()))


def normalize_address(addr: str, strip_suite: bool = True) -> str:
    """
    Normalize an address for comparison:
      1. Strip suite/unit/building/floor suffix (suite strip runs BEFORE abbrev expansion
         so directional abbreviations like E/W/N/S inside suite labels are already gone)
      2. Expand street abbreviations
      3. Collapse whitespace, remove punctuation, uppercase
    """
    if addr is None:
        return ""
    addr = str(addr).strip()
    if strip_suite:
        addr = _SUITE_SUFFIX_RE.sub('', addr)
        addr = _TRAILING_COMMA_RE.sub('', addr)
    for pattern, replacement in _ABBREV_COMPILED:
        addr = pattern.sub(replacement, addr)
    addr = re.sub(r'\s+', ' ', addr)
    addr = re.sub(r'[.,!?;:\'"()]', '', addr)
    return addr.upper().strip()


def addresses_match(src: str, tgt: str) -> bool:
    """True when both addresses refer to the same street, ignoring suite/unit/floor/building."""
    return normalize_address(src) == normalize_address(tgt)


# =========================================================
#  NAME HANDLING
# =========================================================
def extract_name_components(full_name: str) -> Dict[str, str]:
    if not full_name:
        return {"first": "", "middle": "", "last": ""}
    parts = str(full_name).strip().split()
    if len(parts) == 1:
        return {"first": parts[0], "middle": "", "last": ""}
    elif len(parts) == 2:
        return {"first": parts[0], "middle": "", "last": parts[1]}
    else:
        return {"first": parts[0], "middle": " ".join(parts[1:-1]), "last": parts[-1]}


def names_match_with_middle(src: str, tgt: str) -> bool:
    if not src or not tgt:
        return False
    sp = extract_name_components(src)
    tp = extract_name_components(tgt)
    sf, sm, sl = sp["first"].lower(), sp["middle"].lower(), sp["last"].lower()
    tf, tm, tl = tp["first"].lower(), tp["middle"].lower(), tp["last"].lower()
    if sf == tf and sm == tm and sl == tl:
        return True
    if sf == tf and sl == tl:
        return True
    return False


def _normalize_name_token(tok: str) -> str:
    return tok.strip().strip('.').upper()


def first_name_tokens_compatible(src: str, tgt: str) -> bool:
    """
    True when one side's whitespace-separated tokens are an exact
    prefix of the other's, e.g.:
      MICHAEL         vs MICHAEL P.               (embedded middle initial)
      HEATHER         vs HEATHER ELAINE            (embedded middle name)
      MARIA CRISTINA  vs MARIA CRISTINA MALAMUG    (embedded second/last name)
      JESSI-ANN       vs JESSI-ANN B'DOT
    Trailing periods on tokens (initials) are ignored during comparison.
    """
    if not src or not tgt:
        return False
    src_tokens = [_normalize_name_token(t) for t in str(src).strip().split()]
    tgt_tokens = [_normalize_name_token(t) for t in str(tgt).strip().split()]
    if not src_tokens or not tgt_tokens:
        return False
    shorter, longer = (src_tokens, tgt_tokens) if len(src_tokens) <= len(tgt_tokens) else (tgt_tokens, src_tokens)
    return longer[:len(shorter)] == shorter


# =========================================================
#  BOOLEAN COMPARISON
# =========================================================
def to_boolean(val: Any) -> Optional[bool]:
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    val_str = str(val).strip().lower()
    if val_str in ("true", "t", "1", "yes", "y", "on", "enabled", "active"):
        return True
    elif val_str in ("false", "f", "0", "no", "n", "off", "disabled", "inactive", ""):
        return False
    return None


def booleans_match(src: Any, tgt: Any) -> bool:
    sb = to_boolean(src)
    tb = to_boolean(tgt)
    if sb is None or tb is None:
        return False
    return sb == tb


# =========================================================
#  GENDER / SEX
# =========================================================
def canonicalize_gender(val: Any) -> Optional[str]:
    if val is None:
        return None
    v = str(val).strip().upper()
    if v in ("M", "MALE"):
        return "M"
    if v in ("F", "FEMALE"):
        return "F"
    if v in ("U", "UNKNOWN"):
        return "U"
    return None


def genders_match(src: Any, tgt: Any) -> bool:
    sg = canonicalize_gender(src)
    tg = canonicalize_gender(tgt)
    if sg is None or tg is None:
        return False
    return sg == tg


# =========================================================
#  NUMERIC / PHONE
# =========================================================
def strip_numeric_punctuation(val: str) -> str:
    if not val:
        return ""
    return re.sub(r'[\-\(\)\s]', '', str(val))


# Column-name pattern for fields that are "numeric flexible" by nature -- when
# one of these is used as a join KEY, punctuation differences (e.g. a phone
# number formatted "386-425-4700" vs "3864254700") must not break the match.
NUMERIC_KEY_FIELD_RE = re.compile(
    r'(phone|telephone|fax|mobile|cellphone|npi|taxid|ssn|ein|tin|zip)', re.IGNORECASE
)


def normalize_key_component(col_name: str, value: Any) -> Any:
    if value is None:
        return None
    if col_name and NUMERIC_KEY_FIELD_RE.search(col_name):
        return re.sub(r'[\-\(\)\[\]\s,]', '', str(value))
    return value


def numeric_with_punct_match(src: Any, tgt: Any, tolerance: float = 0.01) -> bool:
    try:
        sc = strip_numeric_punctuation(str(src))
        tc = strip_numeric_punctuation(str(tgt))
        if sc == tc:
            return True
        sn = float(re.sub(r'[^\d.]', '', sc))
        tn = float(re.sub(r'[^\d.]', '', tc))
        return abs(sn - tn) <= tolerance
    except (ValueError, AttributeError):
        return False


# =========================================================
#  DATES
# =========================================================
# US-oriented formats only (no day/month-swapped formats) so an actually
# different date never silently parses as a match.
_DATE_FORMATS = [
    "%m/%d/%Y", "%m/%d/%y",
    "%m-%d-%Y", "%m-%d-%y",
    "%Y-%m-%d", "%Y/%m/%d",
    "%Y%m%d",
]


def parse_flexible_date(val: Any):
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date_cls):
        return val
    s = str(val).strip()
    if not s:
        return None
    if " " in s and ":" in s:
        s = s.split(" ")[0]  # drop a trailing time component
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def dates_match(src: Any, tgt: Any) -> bool:
    sd = parse_flexible_date(src)
    td = parse_flexible_date(tgt)
    if sd is not None and td is not None:
        return sd == td
    return str(src).strip() == str(tgt).strip()


# =========================================================
#  NULL/EMPTY
# =========================================================
def is_null_or_empty(val: Any) -> bool:
    if val is None:
        return True
    if isinstance(val, str):
        return len(val.strip()) == 0
    return False


# =========================================================
#  UNIFIED COMPARISON LOGIC  (v9.2)
# =========================================================
def compare_field_values(src: Any, tgt: Any, field_name: str, rule: str) -> str:
    """
    Returns: "MATCHED", "MISMATCH", or "NULL_MISMATCH"

    address_normalized_strict  -- strip suite/bldg/floor + expand abbrevs, compare street only
    address2_suite_ignored     -- null src vs STE/UNIT/FLOOR/F1051/M1921 target => MATCHED
    first_name_with_initial    -- flexible middle name / embedded initial / compound first name
    last_name_with_suffix      -- embedded suffix (Jr, Sr, II, III) / compound last name
    date_flexible              -- compares calendar date regardless of format/zero-padding
    numeric_flexible           -- strips ()-. for phones, IDs, numbers
    boolean_match              -- Y/N, true/false, 1/0, yes/no
    gender_match               -- MALE/M, FEMALE/F, UNKNOWN/U, case-insensitive
    exact                      -- case-sensitive exact match
    string_normalized          -- case-insensitive exact match
    """

    src_null = is_null_or_empty(src)
    tgt_null = is_null_or_empty(tgt)

    # ADDRESS LINE 2 / SUITE-IGNORED
    if rule == "address2_suite_ignored":
        if src_null and tgt_null:
            return "MATCHED"
        if src_null and not tgt_null:
            if is_suite_only_value(str(tgt)):
                return "MATCHED"
            return "NULL_MISMATCH"
        if not src_null and tgt_null:
            return "NULL_MISMATCH"
        return "MATCHED" if addresses_match(str(src), str(tgt)) else "MISMATCH"

    # STANDARD NULL CHECK (all other rules)
    if src_null and tgt_null:
        return "MATCHED"
    elif src_null or tgt_null:
        return "NULL_MISMATCH"

    # DATE RULES
    if rule == "date_flexible":
        return "MATCHED" if dates_match(src, tgt) else "MISMATCH"

    # ADDRESS RULES
    if rule == "address_normalized_strict":
        return "MATCHED" if addresses_match(str(src), str(tgt)) else "MISMATCH"

    if rule == "address_component":
        return "MATCHED" if str(src).strip().lower() == str(tgt).strip().lower() else "MISMATCH"

    # NAME RULES
    if rule == "first_name_with_initial":
        if names_match_with_middle(src, tgt):
            return "MATCHED"
        if first_name_tokens_compatible(src, tgt):
            return "MATCHED"
        return "MISMATCH"

    if rule == "last_name_with_suffix":
        return "MATCHED" if first_name_tokens_compatible(src, tgt) else "MISMATCH"

    # NUMERIC / PHONE RULES
    if rule == "numeric_flexible":
        try:
            src_clean = re.sub(r'[\-\(\)\[\]\s,]', '', str(src))
            tgt_clean = re.sub(r'[\-\(\)\[\]\s,]', '', str(tgt))
            if src_clean == tgt_clean:
                return "MATCHED"
            src_num = float(re.sub(r'[^\d.]', '', src_clean))
            tgt_num = float(re.sub(r'[^\d.]', '', tgt_clean))
            return "MATCHED" if abs(src_num - tgt_num) <= 0.01 else "MISMATCH"
        except (ValueError, AttributeError):
            return "MISMATCH"

    # BOOLEAN RULES
    if "boolean" in rule.lower():
        return "MATCHED" if booleans_match(src, tgt) else "MISMATCH"

    if "is_" in field_name.lower() or "flag" in field_name.lower():
        return "MATCHED" if booleans_match(src, tgt) else "MISMATCH"

    src_bool_attempt = to_boolean(src)
    tgt_bool_attempt = to_boolean(tgt)
    if src_bool_attempt is not None and tgt_bool_attempt is not None:
        return "MATCHED" if src_bool_attempt == tgt_bool_attempt else "MISMATCH"

    # GENDER / SEX RULES  (FEMALE<->F, MALE<->M, case-insensitive)
    if rule == "gender_match" or "gender" in field_name.lower() or field_name.lower() in ("sex", "sex_code", "gender_code"):
        sg = canonicalize_gender(src)
        tg = canonicalize_gender(tgt)
        if sg is not None and tg is not None:
            return "MATCHED" if sg == tg else "MISMATCH"
        if rule == "gender_match":
            return "MISMATCH"

    # DEFAULT RULES
    if rule == "exact":
        return "MATCHED" if str(src).strip() == str(tgt).strip() else "MISMATCH"

    elif rule == "string_normalized":
        return "MATCHED" if str(src).strip().lower() == str(tgt).strip().lower() else "MISMATCH"

    elif rule == "numeric_tolerance":
        try:
            src_num = float(str(src).replace(",", ""))
            tgt_num = float(str(tgt).replace(",", ""))
            return "MATCHED" if abs(src_num - tgt_num) <= 0.01 else "MISMATCH"
        except (ValueError, AttributeError):
            return "MISMATCH"

    elif rule == "numeric_ignore_punctuation":
        return "MATCHED" if numeric_with_punct_match(src, tgt) else "MISMATCH"

    # Default fallback
    return "MATCHED" if str(src).strip() == str(tgt).strip() else "MISMATCH"


# ================= CONFIG =================

class DBConfig(BaseModel):
    host: str
    database: str
    username: str
    password: str
    port: int = 1433

# ================= DB CONNECTION =================

def get_connection(cfg: DBConfig):
    server = cfg.host if "\\" in cfg.host else f"{cfg.host},{cfg.port}"
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};DATABASE={cfg.database};"
        f"UID={cfg.username};PWD={cfg.password};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

# ================= SQL FILTER BUILDER =================

def build_where_clause(filters, col_prefix=""):
    if not filters:
        return "", []
    conditions, params = [], []
    for f in filters:
        field    = f.get("field")
        operator = f.get("operator")
        value    = f.get("value")
        if not field:
            continue
        col      = f"[{field}]" if not col_prefix else f"{col_prefix}.[{field}]"
        safe_col = f"TRY_CONVERT(NVARCHAR(500), {col})"
        val_is_null = (value is None or str(value).strip().lower() in ("null", ""))
        if val_is_null:
            if operator in ("equals", ""):
                conditions.append(f"{col} IS NULL")
            elif operator == "not_equals":
                conditions.append(f"{col} IS NOT NULL")
            continue
        if operator == "equals":
            conditions.append(f"{safe_col} = ?");        params.append(str(value))
        elif operator == "not_equals":
            conditions.append(f"({safe_col} != ? OR {col} IS NULL)"); params.append(str(value))
        elif operator == "contains":
            conditions.append(f"{safe_col} LIKE ?");     params.append(f"%{value}%")
        elif operator == "starts_with":
            conditions.append(f"{safe_col} LIKE ?");     params.append(f"{value}%")
        elif operator == "greater_than":
            conditions.append(f"{col} > ?");             params.append(value)
        elif operator == "less_than":
            conditions.append(f"{col} < ?");             params.append(value)
    return (" AND " + " AND ".join(conditions), params) if conditions else ("", [])

# ================= SAFE LIMIT =================

def safe_limit(v):
    try:
        v = int(v)
        return 1000000 if v <= 0 else min(v, 100000)
    except:
        return 100

# ================= BATCH FETCH =================

def fetch_with_filters(cur, schema, table, cols, limit, filters=None,
                       batch_size=10000, cancel_event=None):
    col_sql = ", ".join(f"[{c}]" for c in cols)
    where_clause, filter_params = build_where_clause(filters)
    all_rows, offset = [], 0
    while True:
        if cancel_event and cancel_event.is_set():
            raise Exception("Job cancelled by user")
        actual_batch = batch_size if limit >= 1000000 else min(batch_size, limit - offset)
        if actual_batch <= 0:
            break
        sql = (
            f"SELECT {col_sql} FROM [{schema}].[{table}]"
            f" WHERE 1=1 {where_clause}"
            f" ORDER BY (SELECT NULL)"
            f" OFFSET {offset} ROWS FETCH NEXT {actual_batch} ROWS ONLY"
        )
        cur.execute(sql, filter_params)
        batch_rows = cur.fetchall()
        if not batch_rows:
            break
        all_rows.extend([dict(zip(cols, row)) for row in batch_rows])
        offset += len(batch_rows)
        if len(batch_rows) < actual_batch or (limit < 1000000 and offset >= limit):
            break
    print(f"Fetched {len(all_rows)} rows from [{schema}].[{table}]")
    return all_rows

# ================= DEDUPLICATION =================

def dedupe_rows(rows: List[Dict[str, Any]], fields: List[str]):
    """
    Keeps the first row per distinct combination of the given field values
    (case-insensitive, whitespace-trimmed). Returns (deduped_rows, removed_count).
    """
    if not fields:
        return rows, 0
    seen = set()
    deduped = []
    for r in rows:
        sig = tuple(
            str(r.get(f)).strip().lower() if r.get(f) is not None else None
            for f in fields
        )
        if sig in seen:
            continue
        seen.add(sig)
        deduped.append(r)
    return deduped, len(rows) - len(deduped)


def _dedupe_group_key(row, fields):
    return tuple(
        str(row.get(f)).strip().lower() if row.get(f) is not None else None
        for f in fields
    )


def _max_of(values):
    non_null = [v for v in values if v is not None]
    if not non_null:
        return None
    try:
        return max(non_null, key=lambda v: float(v))
    except (TypeError, ValueError):
        return max(non_null, key=lambda v: str(v))


def dedupe_rows_max(rows: List[Dict[str, Any]], fields: List[str]):
    """
    Groups rows by the given field values (case-insensitive, whitespace-trimmed).
    Groups with a single row pass through unchanged. Groups with duplicates are
    collapsed into one row by taking the MAX value per column across the group
    (numeric max when possible, else lexical max). Returns (rows, removed_count).
    """
    if not fields:
        return rows, 0
    groups, order = {}, []
    for r in rows:
        sig = _dedupe_group_key(r, fields)
        if sig not in groups:
            groups[sig] = []
            order.append(sig)
        groups[sig].append(r)

    result, removed = [], 0
    for sig in order:
        group = groups[sig]
        if len(group) == 1:
            result.append(group[0])
            continue
        removed += len(group) - 1
        cols = group[0].keys()
        result.append({c: _max_of([r.get(c) for r in group]) for c in cols})
    return result, removed


def _norm_field_name(name):
    return re.sub(r'[^a-z0-9]', '', str(name or '').lower())


# ETL standard: when the key mapping includes outer_code, outer_num, and taxid
# (by whatever name they go by) on both sides, dedupe on exactly those 3
# fields and collapse target duplicates by taking the MAX value per column
# (instead of first-row-wins).
STANDARD_DEDUPE_CONCEPTS = [
    ["outercode"],
    ["outernum", "outernumber"],
    ["taxid", "tin"],
]


def resolve_standard_dedupe_fields(key_map):
    """
    Returns (src_fields, tgt_fields) for the 3 standard dedupe concepts if ALL
    of them are present (by normalized name, allowing known aliases) on both
    sides of the key mapping, else (None, None).
    """
    def find(side, aliases):
        for m in key_map:
            name = m.get(side)
            if name and _norm_field_name(name) in aliases:
                return name
        return None

    src_fields = [find("src", aliases) for aliases in STANDARD_DEDUPE_CONCEPTS]
    tgt_fields = [find("tgt", aliases) for aliases in STANDARD_DEDUPE_CONCEPTS]
    if all(src_fields) and all(tgt_fields):
        return src_fields, tgt_fields
    return None, None

# ================= DB HELPERS =================

def save_to_history(job_id, test_case, status, accuracy, stats,
                    gate_status="", alerts=None):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO execution_history
        (job_id, test_case, execution_date, status, accuracy,
         key_match_rate, field_accuracy,
         total_records, matched, mismatched, source_only, target_only,
         null_mismatches, source_duplicates, target_duplicates,
         src_row_count, tgt_row_count,
         keys_in_both, keys_src_only, keys_tgt_only,
         gate_status, alerts)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        job_id, test_case, datetime.now().isoformat(), status,
        accuracy,
        stats.get("key_match_rate", 0),
        stats.get("field_accuracy",  0),
        stats.get("total",             0),
        stats.get("matched",           0),
        stats.get("mismatched",        0),
        stats.get("source_only",       0),
        stats.get("target_only",       0),
        stats.get("null_mismatches",   0),
        stats.get("source_duplicates", 0),
        stats.get("target_duplicates", 0),
        stats.get("src_row_count",     0),
        stats.get("tgt_row_count",     0),
        stats.get("keys_in_both",      0),
        stats.get("keys_src_only",     0),
        stats.get("keys_tgt_only",     0),
        gate_status or "",
        json.dumps(alerts or []),
    ))
    conn.commit()
    conn.close()


def save_results_batch(job_id, results_batch):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.executemany("""
        INSERT INTO execution_results
        (job_id, key_value, field, source_value, target_value, status)
        VALUES (?,?,?,?,?,?)
    """, [
        (job_id, r.get("key",""), r.get("field",""),
         r.get("source",""), r.get("target",""), r.get("status",""))
        for r in results_batch
    ])
    conn.commit()
    conn.close()


def get_results_page(job_id, page=1, page_size=100, status_filter=None, field_filter=None):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    offset = (page - 1) * page_size
    params = [job_id]
    where_extra = ""
    if status_filter:
        where_extra += " AND status = ?"
        params.append(status_filter)
    if field_filter:
        where_extra += " AND field = ?"
        params.append(field_filter)
    cur.execute(
        f"SELECT key_value, field, source_value, target_value, status "
        f"FROM execution_results WHERE job_id = ?{where_extra} LIMIT ? OFFSET ?",
        params + [page_size, offset]
    )
    rows = cur.fetchall()
    count_params = [job_id]
    if status_filter:
        count_params.append(status_filter)
    if field_filter:
        count_params.append(field_filter)
    cur.execute(
        f"SELECT COUNT(*) FROM execution_results WHERE job_id = ?{where_extra}",
        count_params
    )
    total = cur.fetchone()[0]
    conn.close()
    return {
        "results": [
            {"key": r[0], "field": r[1], "source": r[2], "target": r[3], "status": r[4]}
            for r in rows
        ],
        "page": page, "pageSize": page_size,
        "total": total,
        "totalPages": max(1, (total + page_size - 1) // page_size)
    }


def load_history():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        SELECT job_id, test_case, execution_date, status,
               accuracy, key_match_rate, field_accuracy,
               total_records, matched, mismatched,
               source_only, target_only, null_mismatches,
               source_duplicates, target_duplicates,
               src_row_count, tgt_row_count,
               keys_in_both, keys_src_only, keys_tgt_only,
               gate_status, alerts
        FROM execution_history
        ORDER BY execution_date DESC
        LIMIT 200
    """)
    rows = cur.fetchall()
    conn.close()
    return [
        {
            "jobId": r[0], "testCase": r[1], "date": r[2], "status": r[3],
            "accuracy":       r[4],  "keyMatchRate":  r[5],  "fieldAccuracy": r[6],
            "totalRecords":   r[7],  "matched":       r[8],  "mismatched":    r[9],
            "sourceOnly":     r[10], "targetOnly":    r[11], "nullMismatches":r[12],
            "sourceDuplicates":r[13],"targetDuplicates":r[14],
            "srcRowCount":    r[15], "tgtRowCount":   r[16],
            "keysInBoth":     r[17] or 0,
            "keysSrcOnly":    r[18] or 0,
            "keysTgtOnly":    r[19] or 0,
            "gateStatus":     r[20] or "",
            "alerts":         _parse_alerts(r[21]),
        }
        for r in rows
    ]


def _parse_alerts(raw):
    if not raw:
        return []
    try:
        return json.loads(raw)
    except Exception:
        return []

# =========================================================
#  v9.3 OBSERVABILITY — QUALITY GATES + ANOMALY DETECTION
# =========================================================

# How far a metric must move from the rolling baseline before we raise an alert.
ANOMALY_DEFAULTS = {
    "accuracy_drop_pts":   10.0,   # field-accuracy drop (percentage points)
    "key_rate_drop_pts":   10.0,   # key-match-rate drop (percentage points)
    "row_count_pct":       20.0,   # source row-count swing (%)
    "mismatch_spike_pct":  50.0,   # mismatch-count increase (%)
    "null_rate_jump_pts":  15.0,   # null-mismatch-rate jump (percentage points)
    "min_history":         2,      # need at least this many prior runs to compare
}


def get_recent_runs(test_case, exclude_job=None, limit=5):
    """Most-recent COMPLETED runs for a test case (newest first), excluding one job."""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        SELECT job_id, execution_date, field_accuracy, key_match_rate,
               mismatched, null_mismatches, total_records,
               src_row_count, tgt_row_count
        FROM execution_history
        WHERE test_case = ? AND status = 'COMPLETED' AND job_id != ?
        ORDER BY execution_date DESC
        LIMIT ?
    """, [test_case, exclude_job or "", limit])
    rows = cur.fetchall()
    conn.close()
    return [{
        "jobId": r[0], "date": r[1],
        "fieldAccuracy": r[2] or 0, "keyMatchRate": r[3] or 0,
        "mismatched": r[4] or 0, "nullMismatches": r[5] or 0,
        "totalRecords": r[6] or 0, "srcRowCount": r[7] or 0, "tgtRowCount": r[8] or 0,
    } for r in rows]


def _avg(vals):
    vals = [v for v in vals if v is not None]
    return sum(vals) / len(vals) if vals else 0


def detect_anomalies(test_case, job_id, stats):
    """Compare this run's stats to the rolling average of prior runs of the same
    test case. Returns a list of alert dicts (may be empty)."""
    history = get_recent_runs(test_case, exclude_job=job_id, limit=5)
    if len(history) < ANOMALY_DEFAULTS["min_history"]:
        return []

    alerts = []
    baseline_n = len(history)
    fa   = stats.get("field_accuracy", 0)
    kmr  = stats.get("key_match_rate", 0)
    mm   = stats.get("mismatched", 0)
    src  = stats.get("src_row_count", 0)
    total = stats.get("total", 0) or 1
    null_rate = round(stats.get("null_mismatches", 0) / total * 100, 2)

    avg_fa  = _avg([h["fieldAccuracy"] for h in history])
    avg_kmr = _avg([h["keyMatchRate"] for h in history])
    avg_mm  = _avg([h["mismatched"] for h in history])
    avg_src = _avg([h["srcRowCount"] for h in history])
    avg_null_rate = _avg([
        (h["nullMismatches"] / (h["totalRecords"] or 1) * 100) for h in history
    ])

    def pct_change(cur, base):
        return abs(cur - base) / base * 100 if base else 0

    if avg_fa - fa >= ANOMALY_DEFAULTS["accuracy_drop_pts"]:
        alerts.append({
            "kind": "anomaly", "severity": "high", "metric": "field_accuracy",
            "title": "Field accuracy dropped",
            "detail": f"Field accuracy fell to {fa}% from a {baseline_n}-run average of {round(avg_fa,1)}% "
                      f"(−{round(avg_fa-fa,1)} pts)."
        })
    if avg_kmr - kmr >= ANOMALY_DEFAULTS["key_rate_drop_pts"]:
        alerts.append({
            "kind": "anomaly", "severity": "high", "metric": "key_match_rate",
            "title": "Key match rate dropped",
            "detail": f"Key match rate fell to {kmr}% from a {baseline_n}-run average of {round(avg_kmr,1)}% "
                      f"(−{round(avg_kmr-kmr,1)} pts)."
        })
    if avg_src and pct_change(src, avg_src) >= ANOMALY_DEFAULTS["row_count_pct"]:
        direction = "dropped" if src < avg_src else "jumped"
        alerts.append({
            "kind": "anomaly", "severity": "medium", "metric": "src_row_count",
            "title": f"Source row count {direction}",
            "detail": f"Source row count {direction} to {src:,} from an average of {int(avg_src):,} "
                      f"({round(pct_change(src, avg_src),1)}% change) — possible failed/partial load."
        })
    if avg_mm and mm > avg_mm and pct_change(mm, avg_mm) >= ANOMALY_DEFAULTS["mismatch_spike_pct"]:
        alerts.append({
            "kind": "anomaly", "severity": "medium", "metric": "mismatched",
            "title": "Mismatch spike",
            "detail": f"Mismatched fields rose to {mm:,} from an average of {int(avg_mm):,} "
                      f"(+{round(pct_change(mm, avg_mm),1)}%)."
        })
    if null_rate - avg_null_rate >= ANOMALY_DEFAULTS["null_rate_jump_pts"]:
        alerts.append({
            "kind": "anomaly", "severity": "medium", "metric": "null_rate",
            "title": "Null mismatch rate spiked",
            "detail": f"Null-mismatch rate rose to {null_rate}% from an average of {round(avg_null_rate,1)}%."
        })
    return alerts


def get_gates(test_case):
    """Enabled gates that apply to this test case (exact match or global '*'/'')."""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        SELECT id, test_case, metric, field, operator, threshold, severity, enabled
        FROM quality_gates
        WHERE enabled = 1 AND (test_case = ? OR test_case = '*' OR test_case = '')
        ORDER BY id
    """, [test_case])
    rows = cur.fetchall()
    conn.close()
    return [{
        "id": r[0], "testCase": r[1], "metric": r[2], "field": r[3] or "",
        "operator": r[4], "threshold": r[5], "severity": r[6] or "fail",
        "enabled": bool(r[7]),
    } for r in rows]


def field_accuracy_for(job_id, field):
    """Per-field accuracy (%) from stored results, or None if the field has no rows."""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        SELECT
            SUM(CASE WHEN status='MATCHED'       THEN 1 ELSE 0 END),
            SUM(CASE WHEN status='MISMATCH'      THEN 1 ELSE 0 END),
            SUM(CASE WHEN status='NULL_MISMATCH' THEN 1 ELSE 0 END)
        FROM execution_results
        WHERE job_id = ? AND field = ?
    """, [job_id, field])
    row = cur.fetchone()
    conn.close()
    matched, mism, nullm = (row[0] or 0), (row[1] or 0), (row[2] or 0)
    denom = matched + mism + nullm
    return round(matched / denom * 100, 2) if denom else None


def gate_metric_value(gate, job_id, stats):
    """Resolve the observed value for a gate's metric. Returns None if N/A."""
    metric = gate["metric"]
    total  = stats.get("total", 0) or 1
    if metric == "field_accuracy":
        if gate.get("field"):
            return field_accuracy_for(job_id, gate["field"])
        return stats.get("field_accuracy", 0)
    if metric == "key_match_rate":
        return stats.get("key_match_rate", 0)
    if metric == "null_rate":
        return round(stats.get("null_mismatches", 0) / total * 100, 2)
    if metric == "mismatch_count":
        return stats.get("mismatched", 0)
    if metric == "row_count_diff_pct":
        src = stats.get("src_row_count", 0)
        tgt = stats.get("tgt_row_count", 0)
        base = max(src, tgt, 1)
        return round(abs(src - tgt) / base * 100, 2)
    return None


def _gate_passes(value, operator, threshold):
    if value is None:
        return True  # metric not applicable -> don't fail the gate
    if operator == "gte": return value >= threshold
    if operator == "lte": return value <= threshold
    if operator == "gt":  return value > threshold
    if operator == "lt":  return value < threshold
    if operator == "eq":  return value == threshold
    return True


_OP_TEXT = {"gte": "≥", "lte": "≤", "gt": ">", "lt": "<", "eq": "="}


def evaluate_gates(test_case, job_id, stats):
    """Evaluate all applicable gates. Returns (gate_status, alerts_list).
    gate_status is FAIL / WARNING / PASS / '' (empty when no gates defined)."""
    gates = get_gates(test_case)
    if not gates:
        return "", []
    alerts = []
    any_fail = any_warn = False
    for g in gates:
        value = gate_metric_value(g, job_id, stats)
        passed = _gate_passes(value, g["operator"], g["threshold"])
        if passed:
            continue
        sev = "high" if g["severity"] == "fail" else "medium"
        if g["severity"] == "fail": any_fail = True
        else:                        any_warn = True
        label = g["metric"] + (f" [{g['field']}]" if g.get("field") else "")
        shown = "n/a" if value is None else value
        alerts.append({
            "kind": "gate", "severity": sev, "metric": g["metric"],
            "field": g.get("field", ""),
            "title": f"Quality gate {'failed' if g['severity']=='fail' else 'warning'}",
            "detail": f"{label} = {shown} violates rule "
                      f"{_OP_TEXT.get(g['operator'], g['operator'])} {g['threshold']}."
        })
    gate_status = "FAIL" if any_fail else "WARNING" if any_warn else "PASS"
    return gate_status, alerts


# ================= ENDPOINTS =================

@app.get("/health")
def health():
    return {"status": "ok", "version": "9.2-BR", "businessRules": [
        "address_normalized_strict",   # Strip suite suffix + expand abbrevs + case-insensitive
        "address2_suite_ignored",      # NEW: null src vs STE/UNIT target => MATCHED
        "address_component",           # Strict component match
        "first_name_with_initial",     # Handles embedded middle initial + flexible names
        "last_name_with_suffix",       # Handles embedded suffix (Jr, Sr, II, III)
        "date_flexible",               # Compares calendar date regardless of format/zero-padding
        "numeric_flexible",            # Phone, IDs, numbers -- strips punctuation
        "boolean_match",               # Auto-detects Y/N, true/false, 1/0
        "gender_match",                # Auto-detects MALE/M, FEMALE/F, UNKNOWN/U
        "exact",                       # Exact string match
        "string_normalized"            # Case-insensitive exact match
    ]}

@app.post("/db/connect")
def connect(cfg: DBConfig):
    try:
        conn = get_connection(cfg); conn.close()
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/db/tables")
def tables(cfg: DBConfig):
    try:
        conn = get_connection(cfg)
        cur  = conn.cursor()
        cur.execute("""
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE='BASE TABLE'
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """)
        data = [{"schema": r.TABLE_SCHEMA, "name": r.TABLE_NAME} for r in cur.fetchall()]
        conn.close()
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/db/columns")
def columns(req: Dict[str, Any]):
    try:
        cfg  = DBConfig(**req["config"])
        conn = get_connection(cfg)
        cur  = conn.cursor()
        cur.execute("""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA=? AND TABLE_NAME=?
            ORDER BY ORDINAL_POSITION
        """, req["schema"], req["table"])
        cols = [{"name": r.COLUMN_NAME, "type": r.DATA_TYPE} for r in cur.fetchall()]
        conn.close()
        return cols
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/db/rowcount")
def rowcount(req: Dict[str, Any]):
    try:
        cfg  = DBConfig(**req["config"])
        conn = get_connection(cfg)
        cur  = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM [{req['schema']}].[{req['table']}]")
        count = cur.fetchone()[0]
        conn.close()
        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/db/preview")
def preview_table(req: Dict[str, Any]):
    try:
        cfg    = DBConfig(**req["config"])
        schema = req["schema"]
        table  = req["table"]
        limit  = int(req.get("limit", 20))
        conn   = get_connection(cfg)
        cur    = conn.cursor()
        cur.execute(f"SELECT TOP {limit} * FROM [{schema}].[{table}]")
        cols = [col[0] for col in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        conn.close()
        return {"columns": cols, "rows": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/db/share/{job_id}")
def share_job(job_id: str):
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    cur.execute("""
        SELECT job_id, test_case, execution_date, status,
               accuracy, key_match_rate, field_accuracy,
               total_records, matched, mismatched,
               source_only, target_only, null_mismatches,
               source_duplicates, target_duplicates,
               src_row_count, tgt_row_count,
               keys_in_both, keys_src_only, keys_tgt_only,
               gate_status, alerts
        FROM execution_history WHERE job_id = ?
    """, [job_id])
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    return {
        "jobId":            row[0],  "testCase":         row[1],
        "date":             row[2],  "status":           row[3],
        "accuracy":         row[4],  "keyMatchRate":     row[5],
        "fieldAccuracy":    row[6],  "totalRecords":     row[7],
        "matched":          row[8],  "mismatched":       row[9],
        "sourceOnly":       row[10], "targetOnly":       row[11],
        "nullMismatches":   row[12], "sourceDuplicates": row[13],
        "targetDuplicates": row[14], "srcRowCount":      row[15],
        "tgtRowCount":      row[16], "keysInBoth":       row[17] or 0,
        "keysSrcOnly":      row[18] or 0, "keysTgtOnly": row[19] or 0,
        "gateStatus":       row[20] or "",
        "alerts":           _parse_alerts(row[21]),
    }

# ================= MAIN COMPARE ENDPOINT =================

@app.post("/db/compare")
def compare(req: Dict[str, Any]):
    key_map   = req.get("keyMappings", [])
    field_map = req.get("fieldMappings", req.get("comparisonRules", []))
    filters   = req.get("filters", [])
    src_filters = [f for f in filters if f.get("side") == "source"]
    tgt_filters = [f for f in filters if f.get("side") == "target"]
    # Dedup fields are the key-mapping fields themselves (the fields used to
    # match/join source and target records) -- no separate config needed.
    # Exception: the ETL standard dedupe key is outer_code + outer_num + taxid
    # when all 3 are present as keys -- in that case dedupe scope narrows to
    # exactly those 3 fields, and target duplicates collapse via MAX per column
    # instead of first-row-wins.
    std_src_fields, std_tgt_fields = resolve_standard_dedupe_fields(key_map)
    dedupe_is_standard = std_src_fields is not None
    if dedupe_is_standard:
        src_dedupe_fields = std_src_fields
        tgt_dedupe_fields = std_tgt_fields
    else:
        src_dedupe_fields = [m.get("src") for m in key_map if m.get("src")]
        tgt_dedupe_fields = [m.get("tgt") for m in key_map if m.get("tgt")]

    signature = json.dumps({
        "src": req.get("srcTable"), "tgt": req.get("tgtTable"),
        "keys": key_map, "fields": field_map,
        "limit": req.get("limit"), "filters": filters
    }, sort_keys=True)

    with lock:
        if signature in execution_signatures:
            return {"error": "Duplicate execution detected. Please wait for current execution to finish."}
        execution_signatures.add(signature)
        job_id = str(uuid.uuid4())
        cancel_ev = Event()
        cancel_events[job_id] = cancel_ev
        jobs[job_id] = {"status": "RUNNING", "progress": 0, "error": None}

    def worker():
        TIMEOUT_SECONDS = int(req.get("timeoutSeconds", 1800))
        deadline = datetime.now().timestamp() + TIMEOUT_SECONDS

        def check_cancel():
            if cancel_ev.is_set():
                raise Exception("Job cancelled by user")
            if datetime.now().timestamp() > deadline:
                raise Exception(f"Job timed out after {TIMEOUT_SECONDS}s")

        try:
            print(f"\n=== ETL START (v9.2-BR) | JOB {job_id} ===")
            src_cfg = DBConfig(**req["srcConfig"])
            tgt_cfg = DBConfig(**req["tgtConfig"])
            limit   = safe_limit(req.get("limit", 100))
            src_conn = get_connection(src_cfg)
            tgt_conn = get_connection(tgt_cfg)
            src_cur  = src_conn.cursor()
            tgt_cur  = tgt_conn.cursor()
            src_cols = list({x.get("src") for x in (key_map + field_map) if x.get("src")})
            tgt_cols = list({x.get("tgt") for x in (key_map + field_map) if x.get("tgt")})
            if not src_cols or not tgt_cols:
                raise Exception("No valid columns selected")
            with lock: jobs[job_id]["progress"] = 10
            check_cancel()
            src_where, src_params = build_where_clause(src_filters)
            tgt_where, tgt_params = build_where_clause(tgt_filters)
            src_cur.execute(f"SELECT COUNT(*) FROM [{req['srcTable']['schema']}].[{req['srcTable']['name']}] WHERE 1=1 {src_where}", src_params)
            src_row_count = src_cur.fetchone()[0]
            tgt_cur.execute(f"SELECT COUNT(*) FROM [{req['tgtTable']['schema']}].[{req['tgtTable']['name']}] WHERE 1=1 {tgt_where}", tgt_params)
            tgt_row_count = tgt_cur.fetchone()[0]
            with lock: jobs[job_id]["progress"] = 20
            check_cancel()
            src_rows = fetch_with_filters(src_cur, req["srcTable"]["schema"], req["srcTable"]["name"], src_cols, limit, src_filters, cancel_event=cancel_ev)
            with lock: jobs[job_id]["progress"] = 40
            check_cancel()
            tgt_rows = fetch_with_filters(tgt_cur, req["tgtTable"]["schema"], req["tgtTable"]["name"], tgt_cols, limit, tgt_filters, cancel_event=cancel_ev)
            src_conn.close(); tgt_conn.close()
            with lock: jobs[job_id]["progress"] = 60
            check_cancel()

            src_rows, src_dedupe_removed = dedupe_rows(src_rows, src_dedupe_fields)
            if dedupe_is_standard:
                tgt_rows, tgt_dedupe_removed = dedupe_rows_max(tgt_rows, tgt_dedupe_fields)
            else:
                tgt_rows, tgt_dedupe_removed = dedupe_rows(tgt_rows, tgt_dedupe_fields)

            def build_key(row, mappings, side):
                if not mappings: return ("NO_KEY",)
                return tuple(normalize_key_component(m.get(side), row.get(m.get(side))) for m in mappings)

            def build_src_dup_key(row):
                # A source row is a "distinct" duplicate only when another row matches
                # on BOTH the key-mapping fields AND every selected map field.
                return (
                    build_key(row, key_map, "src"),
                    tuple(row.get(f.get("src")) for f in field_map),
                )

            tgt_lookup, tgt_dup_keys = {}, set()
            for r in tgt_rows:
                k = build_key(r, key_map, "tgt")
                if k in tgt_lookup: tgt_dup_keys.add(k)
                else: tgt_lookup[k] = r

            # src_lookup keeps the first row per key (for target matching); source
            # duplicates are tracked on the composite key+map-fields signature.
            src_lookup, src_dup_keys, src_dup_seen = {}, set(), set()
            for r in src_rows:
                k = build_key(r, key_map, "src")
                if k not in src_lookup: src_lookup[k] = r
                dk = build_src_dup_key(r)
                if dk in src_dup_seen: src_dup_keys.add(dk)
                else: src_dup_seen.add(dk)

            BATCH = 1000
            results_batch = []
            matched_count = mismatch_count = source_only_count = 0
            target_only_count = null_mismatch_count = 0
            source_dup_count  = target_dup_count    = 0
            keys_in_both = keys_src_only = 0
            total_src = len(src_rows)
            processed_keys = set()

            for idx, s in enumerate(src_rows):
                check_cancel()
                key = build_key(s, key_map, "src")
                processed_keys.add(key)
                t   = tgt_lookup.get(key)

                if build_src_dup_key(s) in src_dup_keys:
                    results_batch.append({"key": str(key), "field": "KEY_CHECK", "source": "DUPLICATE", "target": "-", "status": "SOURCE_DUPLICATE"})
                    source_dup_count += 1
                    if len(results_batch) >= BATCH: save_results_batch(job_id, results_batch); results_batch = []
                    continue

                if key in tgt_dup_keys:
                    results_batch.append({"key": str(key), "field": "KEY_CHECK", "source": "-", "target": "DUPLICATE", "status": "TARGET_DUPLICATE"})
                    target_dup_count += 1
                    if len(results_batch) >= BATCH: save_results_batch(job_id, results_batch); results_batch = []
                    continue

                if t is None:
                    results_batch.append({"key": str(key), "field": "KEY_CHECK", "source": "EXISTS", "target": "MISSING", "status": "SOURCE_ONLY"})
                    source_only_count += 1
                    keys_src_only     += 1
                    if len(results_batch) >= BATCH: save_results_batch(job_id, results_batch); results_batch = []
                    continue

                keys_in_both += 1

                for f in field_map:
                    src_col = f.get("src")
                    tgt_col = f.get("tgt")
                    rule    = f.get("rule", "exact")
                    sv      = s.get(src_col)
                    tv      = t.get(tgt_col)
                    status  = compare_field_values(sv, tv, src_col, rule)
                    results_batch.append({
                        "key":    str(key),
                        "field":  src_col,
                        "source": str(sv) if sv is not None else "NULL",
                        "target": str(tv) if tv is not None else "NULL",
                        "status": status
                    })
                    if status == "MATCHED":        matched_count       += 1
                    elif status == "MISMATCH":     mismatch_count      += 1
                    elif status == "NULL_MISMATCH":null_mismatch_count += 1
                    if len(results_batch) >= BATCH: save_results_batch(job_id, results_batch); results_batch = []

                with lock:
                    jobs[job_id]["progress"] = int(60 + ((idx+1)/total_src)*25) if total_src else 85

            with lock: jobs[job_id]["progress"] = 90
            check_cancel()

            keys_tgt_only = 0
            for t in tgt_rows:
                key = build_key(t, key_map, "tgt")
                if key not in processed_keys and key not in tgt_dup_keys:
                    results_batch.append({"key": str(key), "field": "KEY_CHECK", "source": "MISSING", "target": "EXISTS", "status": "TARGET_ONLY"})
                    target_only_count += 1
                    keys_tgt_only     += 1
                    if len(results_batch) >= BATCH: save_results_batch(job_id, results_batch); results_batch = []

            if results_batch:
                save_results_batch(job_id, results_batch)

            total_keys     = keys_in_both + keys_src_only + keys_tgt_only
            key_match_rate = round((keys_in_both / total_keys) * 100, 2) if total_keys else 0
            field_total    = matched_count + mismatch_count + null_mismatch_count
            field_accuracy = round((matched_count / field_total) * 100, 2) if field_total else 0
            accuracy       = field_accuracy
            total_count    = (matched_count + mismatch_count + source_only_count +
                              target_only_count + null_mismatch_count +
                              source_dup_count  + target_dup_count)
            stats = {
                "total":             total_count,
                "matched":           matched_count,
                "mismatched":        mismatch_count,
                "source_only":       source_only_count,
                "target_only":       target_only_count,
                "null_mismatches":   null_mismatch_count,
                "source_duplicates": source_dup_count,
                "target_duplicates": target_dup_count,
                "src_row_count":     src_row_count,
                "tgt_row_count":     tgt_row_count,
                "accuracy":          accuracy,
                "key_match_rate":    key_match_rate,
                "field_accuracy":    field_accuracy,
                "keys_in_both":      keys_in_both,
                "keys_src_only":     keys_src_only,
                "keys_tgt_only":     keys_tgt_only,
                "src_dedupe_removed": src_dedupe_removed,
                "tgt_dedupe_removed": tgt_dedupe_removed,
                "dedupe_mode":        "standard_max" if dedupe_is_standard else "generic_first",
            }
            # ── v9.3 Observability: evaluate quality gates + detect anomalies ──
            test_case_name = req.get("testCaseNo", "N/A")
            try:
                gate_status, gate_alerts = evaluate_gates(test_case_name, job_id, stats)
            except Exception as ge:
                print(f"[gates] evaluation failed: {ge}")
                gate_status, gate_alerts = "", []
            try:
                anomaly_alerts = detect_anomalies(test_case_name, job_id, stats)
            except Exception as ae:
                print(f"[anomaly] detection failed: {ae}")
                anomaly_alerts = []
            all_alerts = gate_alerts + anomaly_alerts
            stats["gate_status"] = gate_status
            stats["alerts"]      = all_alerts

            save_to_history(job_id, test_case_name, "COMPLETED", accuracy, stats,
                            gate_status=gate_status, alerts=all_alerts)
            with lock:
                jobs[job_id]["status"]   = "COMPLETED"
                jobs[job_id]["progress"] = 100
                jobs[job_id]["stats"]    = stats
                execution_signatures.discard(signature)
                cancel_events.pop(job_id, None)
            print(f"=== ETL DONE (v9.2-BR) | kmr={key_match_rate}% acc={field_accuracy}% "
                  f"gate={gate_status or '-'} alerts={len(all_alerts)} ===\n")

        except Exception as e:
            is_cancel = "cancelled" in str(e).lower()
            print(f"\n=== ETL {'CANCELLED' if is_cancel else 'ERROR'} ===\n{traceback.format_exc()}")
            final_status = "CANCELLED" if is_cancel else "FAILED"
            save_to_history(job_id, req.get("testCaseNo", "N/A"), final_status, 0, {})
            with lock:
                execution_signatures.discard(signature)
                cancel_events.pop(job_id, None)
                jobs[job_id]["status"] = final_status
                jobs[job_id]["error"]  = str(e)
                jobs[job_id]["progress"] = 100

    t = Thread(target=worker, daemon=True)
    t.start()
    return {"jobId": job_id}

@app.get("/db/job/{job_id}")
def job_status(job_id: str):
    job_data = jobs.get(job_id)
    if not job_data:
        reload_jobs_from_db()
        job_data = jobs.get(job_id)
    if not job_data:
        return {"status": "NOT_FOUND"}
    return {
        "status":   job_data.get("status"),
        "progress": job_data.get("progress"),
        "error":    job_data.get("error"),
        "stats":    job_data.get("stats"),
    }

@app.post("/db/job/{job_id}/cancel")
def cancel_job(job_id: str):
    ev = cancel_events.get(job_id)
    if ev:
        ev.set()
        with lock:
            if job_id in jobs:
                jobs[job_id]["status"] = "CANCELLING"
        return {"success": True, "message": "Cancel signal sent"}
    job_data = jobs.get(job_id)
    if job_data and job_data.get("status") in ("COMPLETED", "FAILED", "CANCELLED"):
        return {"success": False, "message": f"Job already {job_data['status']}"}
    return {"success": False, "message": "Job not found or already finished"}

@app.get("/db/results/{job_id}")
def get_results(
    job_id:   str,
    page:     int           = Query(default=1,   ge=1),
    pageSize: int           = Query(default=100, ge=1, le=1000),
    status:   Optional[str] = Query(default=None),
    field:    Optional[str] = Query(default=None)
):
    try:
        return get_results_page(job_id, page, pageSize, status, field)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/db/field-summary/{job_id}")
def field_summary(job_id: str):
    try:
        conn = sqlite3.connect(DB_FILE)
        cur  = conn.cursor()
        cur.execute("""
            SELECT
                field,
                SUM(CASE WHEN status = 'MATCHED'       THEN 1 ELSE 0 END) AS matched,
                SUM(CASE WHEN status = 'MISMATCH'      THEN 1 ELSE 0 END) AS mismatched,
                SUM(CASE WHEN status = 'NULL_MISMATCH' THEN 1 ELSE 0 END) AS null_mismatch,
                COUNT(*) AS total
            FROM execution_results
            WHERE job_id = ?
              AND field != 'KEY_CHECK'
            GROUP BY field
            ORDER BY mismatched DESC, null_mismatch DESC
        """, [job_id])
        rows = cur.fetchall()
        conn.close()
        summary = []
        for row in rows:
            field, matched, mismatched, null_mismatch, total = row
            field_total = matched + mismatched + null_mismatch
            accuracy = round((matched / field_total) * 100, 1) if field_total else 0
            summary.append({
                "field":        field,
                "matched":      matched,
                "mismatched":   mismatched,
                "null_mismatch":null_mismatch,
                "total":        total,
                "accuracy":     accuracy
            })
        return {"summary": summary}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/db/history")
def history():
    return load_history()

@app.post("/db/download")
def download(req: Dict[str, Any]):
    try:
        job_id = req.get("jobId")
        if not job_id:
            raise Exception("Job ID required")
        conn = sqlite3.connect(DB_FILE)
        cur  = conn.cursor()
        cur.execute("""
            SELECT key_value, field, source_value, target_value, status
            FROM execution_results WHERE job_id = ?
        """, [job_id])
        rows = cur.fetchall()
        conn.close()
        if not rows:
            raise Exception("No results found for this job ID")
        df   = pd.DataFrame(rows, columns=["key","field","source","target","status"])
        file = f"outputs/{uuid.uuid4()}.csv"
        df.to_csv(file, index=False)
        return FileResponse(file, media_type="text/csv",
                            filename=f"etl_results_{job_id[:8]}.csv",
                            background=BackgroundTask(os.remove, file))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# =========================================================
#  v9.3 OBSERVABILITY ENDPOINTS
# =========================================================

@app.post("/db/profile")
def profile_table(req: Dict[str, Any]):
    """Data profiling: per-column stats for a table (nulls, distinct, min/max,
    top values, simple pattern hint). Optional 'columns' list limits the scan."""
    try:
        cfg    = DBConfig(**req["config"])
        schema = req["schema"]
        table  = req["table"]
        only   = req.get("columns")  # optional list of column names
        conn   = get_connection(cfg)
        cur    = conn.cursor()

        cur.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
        total_rows = cur.fetchone()[0] or 0

        cur.execute("""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA=? AND TABLE_NAME=?
            ORDER BY ORDINAL_POSITION
        """, schema, table)
        col_defs = [(r.COLUMN_NAME, r.DATA_TYPE) for r in cur.fetchall()]
        if only:
            only_set = {c.lower() for c in only}
            col_defs = [c for c in col_defs if c[0].lower() in only_set]

        columns = []
        for name, dtype in col_defs:
            cv = f"TRY_CONVERT(NVARCHAR(4000), [{name}])"
            try:
                cur.execute(f"""
                    SELECT COUNT(*) AS total,
                           COUNT(cv) AS non_null,
                           COUNT(DISTINCT cv) AS distinct_count,
                           MIN(cv) AS min_v,
                           MAX(cv) AS max_v
                    FROM (SELECT {cv} AS cv FROM [{schema}].[{table}]) x
                """)
                t, non_null, distinct_count, min_v, max_v = cur.fetchone()
                t = t or 0
                non_null = non_null or 0
                nulls = t - non_null
                top = []
                try:
                    cur.execute(f"""
                        SELECT TOP 5 cv, COUNT(*) AS c
                        FROM (SELECT {cv} AS cv FROM [{schema}].[{table}]) x
                        WHERE cv IS NOT NULL
                        GROUP BY cv ORDER BY c DESC
                    """)
                    top = [{"value": r[0], "count": r[1]} for r in cur.fetchall()]
                except Exception:
                    top = []
                columns.append({
                    "name": name, "type": dtype,
                    "total": t, "nulls": nulls,
                    "nullPct": round(nulls / t * 100, 1) if t else 0,
                    "distinct": distinct_count or 0,
                    "distinctPct": round((distinct_count or 0) / non_null * 100, 1) if non_null else 0,
                    "min": min_v, "max": max_v,
                    "topValues": top,
                })
            except Exception as ce:
                columns.append({
                    "name": name, "type": dtype, "total": total_rows,
                    "nulls": None, "nullPct": None, "distinct": None,
                    "distinctPct": None, "min": None, "max": None,
                    "topValues": [], "error": str(ce)[:200],
                })
        conn.close()
        return {"schema": schema, "table": table, "totalRows": total_rows, "columns": columns}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/db/trends")
def trends_list():
    """Distinct test cases with run counts, for the trend picker."""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        SELECT test_case, COUNT(*) AS runs, MAX(execution_date) AS last_run
        FROM execution_history
        WHERE status = 'COMPLETED' AND test_case IS NOT NULL AND test_case != ''
        GROUP BY test_case
        ORDER BY last_run DESC
    """)
    rows = cur.fetchall()
    conn.close()
    return [{"testCase": r[0], "runs": r[1], "lastRun": r[2]} for r in rows]


@app.get("/db/trend")
def trend_series(testCase: str = Query(...), limit: int = Query(default=50, ge=1, le=500)):
    """Time-ordered metric series for a single test case (oldest -> newest)."""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        SELECT job_id, execution_date, field_accuracy, key_match_rate,
               mismatched, null_mismatches, total_records,
               src_row_count, tgt_row_count, gate_status
        FROM execution_history
        WHERE test_case = ? AND status = 'COMPLETED'
        ORDER BY execution_date DESC
        LIMIT ?
    """, [testCase, limit])
    rows = cur.fetchall()
    conn.close()
    rows = list(reversed(rows))  # oldest first for charting
    return {
        "testCase": testCase,
        "points": [{
            "jobId": r[0], "date": r[1],
            "fieldAccuracy": r[2] or 0, "keyMatchRate": r[3] or 0,
            "mismatched": r[4] or 0, "nullMismatches": r[5] or 0,
            "totalRecords": r[6] or 0, "srcRowCount": r[7] or 0,
            "tgtRowCount": r[8] or 0, "gateStatus": r[9] or "",
        } for r in rows]
    }


@app.get("/db/root-cause/{job_id}")
def root_cause(job_id: str, field: str = Query(...), limit: int = Query(default=5000, ge=100, le=50000)):
    """Pattern analysis over mismatch rows for a field: top source/target values,
    key-prefix concentration, null direction, and heuristic flags."""
    try:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        cur.execute("""
            SELECT key_value, source_value, target_value, status
            FROM execution_results
            WHERE job_id = ? AND field = ?
              AND status IN ('MISMATCH', 'NULL_MISMATCH')
            LIMIT ?
        """, [job_id, field, limit])
        rows = cur.fetchall()
        conn.close()

        if not rows:
            return {"field": field, "totalMismatches": 0, "insights": [], "topSource": [],
                    "topTarget": [], "keyPrefixes": [], "sampled": 0}

        from collections import Counter
        total = len(rows)
        src_counter    = Counter()
        tgt_counter    = Counter()
        prefix_counter = Counter()
        src_null = tgt_null = 0
        suite_src_only = 0
        _suite_kw = ("SUITE", "STE ", "STE.", "UNIT", "APT", "BLDG", "FLOOR", " FL ", "#")

        for key_value, sv, tv, status in rows:
            sv_s = "" if sv is None else str(sv)
            tv_s = "" if tv is None else str(tv)
            if sv_s.strip() and len(sv_s) < 80:
                src_counter[sv_s] += 1
            if tv_s.strip() and len(tv_s) < 80:
                tgt_counter[tv_s] += 1
            # key-prefix concentration (strip tuple punctuation)
            cleaned = re.sub(r"[^A-Za-z0-9]", "", key_value or "")
            if cleaned:
                prefix_counter[cleaned[:4].upper()] += 1
            if status == "NULL_MISMATCH":
                if not sv_s.strip() or sv_s == "NULL": src_null += 1
                if not tv_s.strip() or tv_s == "NULL": tgt_null += 1
            su = sv_s.upper()
            tu = tv_s.upper()
            if any(k in su for k in _suite_kw) and not any(k in tu for k in _suite_kw):
                suite_src_only += 1

        def pct(n):
            return round(n / total * 100, 1)

        top_source = [{"value": v, "count": c, "pct": pct(c)} for v, c in src_counter.most_common(5)]
        top_target = [{"value": v, "count": c, "pct": pct(c)} for v, c in tgt_counter.most_common(5)]
        key_prefixes = [{"prefix": p, "count": c, "pct": pct(c)} for p, c in prefix_counter.most_common(5)]

        insights = []
        if key_prefixes and key_prefixes[0]["pct"] >= 40:
            kp = key_prefixes[0]
            insights.append(f"{kp['pct']}% of mismatches are on keys starting with '{kp['prefix']}' "
                            f"({kp['count']:,} rows) — likely a localized data issue.")
        if suite_src_only and pct(suite_src_only) >= 30:
            insights.append(f"{pct(suite_src_only)}% of mismatches have suite/unit info in the source "
                            f"but not the target — the target may be stripping secondary-address data.")
        if src_null or tgt_null:
            if src_null > tgt_null:
                insights.append(f"{pct(src_null)}% of null mismatches are NULL on the source side "
                                f"(target has a value) — source may be missing data.")
            elif tgt_null > src_null:
                insights.append(f"{pct(tgt_null)}% of null mismatches are NULL on the target side "
                                f"(source has a value) — target load may have dropped this field.")
        if top_target and top_target[0]["pct"] >= 30:
            tt = top_target[0]
            insights.append(f"{tt['pct']}% of mismatches share the same target value "
                            f"(\"{tt['value'][:40]}\") — possible default/placeholder overwrite.")
        if not insights:
            insights.append("No dominant single pattern found; mismatches look distributed across keys and values.")

        return {
            "field": field, "totalMismatches": total, "sampled": total,
            "capped": total >= limit,
            "nullDirection": {"sourceNull": src_null, "targetNull": tgt_null},
            "suiteSrcOnly": suite_src_only,
            "topSource": top_source, "topTarget": top_target,
            "keyPrefixes": key_prefixes, "insights": insights,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class GateModel(BaseModel):
    testCase: str = ""
    metric:   str
    field:    str = ""
    operator: str = "gte"
    threshold: float = 0
    severity: str = "fail"
    enabled:  bool = True


@app.get("/db/gates")
def list_gates(testCase: Optional[str] = Query(default=None)):
    """All gates, or (when testCase given) those that apply to it including globals."""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    if testCase is None:
        cur.execute("""
            SELECT id, test_case, metric, field, operator, threshold, severity, enabled
            FROM quality_gates ORDER BY id
        """)
    else:
        cur.execute("""
            SELECT id, test_case, metric, field, operator, threshold, severity, enabled
            FROM quality_gates
            WHERE test_case = ? OR test_case = '*' OR test_case = ''
            ORDER BY id
        """, [testCase])
    rows = cur.fetchall()
    conn.close()
    return [{
        "id": r[0], "testCase": r[1], "metric": r[2], "field": r[3] or "",
        "operator": r[4], "threshold": r[5], "severity": r[6] or "fail",
        "enabled": bool(r[7]),
    } for r in rows]


@app.post("/db/gates")
def create_gate(g: GateModel):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO quality_gates
        (test_case, metric, field, operator, threshold, severity, enabled, created_date)
        VALUES (?,?,?,?,?,?,?,?)
    """, [g.testCase or "", g.metric, g.field or "", g.operator, g.threshold,
          g.severity, 1 if g.enabled else 0, datetime.now().isoformat()])
    gate_id = cur.lastrowid
    conn.commit()
    conn.close()
    return {"success": True, "id": gate_id}


@app.delete("/db/gates/{gate_id}")
def delete_gate(gate_id: int):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("DELETE FROM quality_gates WHERE id = ?", [gate_id])
    conn.commit()
    conn.close()
    return {"success": True}


# ================= START =================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api_server:app", host="0.0.0.0", port=8000, reload=True)