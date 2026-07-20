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
from pydantic import BaseModel

import pyodbc
import pandas as pd
import uuid
import os
import traceback
import json
import sqlite3
import re
from datetime import datetime

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

DB_FILE = "etl_history.db"

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
    ]:
        if col not in existing:
            cur.execute(f"ALTER TABLE execution_history ADD COLUMN {col} {typedef}")

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
#  NUMERIC / PHONE
# =========================================================
def strip_numeric_punctuation(val: str) -> str:
    if not val:
        return ""
    return re.sub(r'[\-\(\)\s]', '', str(val))


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
    first_name_with_initial    -- flexible middle name / embedded initial
    numeric_flexible           -- strips ()-. for phones, IDs, numbers
    boolean_match              -- Y/N, true/false, 1/0, yes/no
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

    # ADDRESS RULES
    if rule == "address_normalized_strict":
        return "MATCHED" if addresses_match(str(src), str(tgt)) else "MISMATCH"

    if rule == "address_component":
        return "MATCHED" if str(src).strip().lower() == str(tgt).strip().lower() else "MISMATCH"

    # NAME RULES
    if rule == "first_name_with_initial":
        src_clean = str(src).strip().upper()
        tgt_clean = str(tgt).strip().upper()
        if names_match_with_middle(src, tgt):
            return "MATCHED"
        tgt_stripped = re.sub(r'\s+[A-Z]$', '', tgt_clean)
        if src_clean == tgt_stripped:
            return "MATCHED"
        src_stripped = re.sub(r'\s+[A-Z]$', '', src_clean)
        if src_stripped == tgt_clean:
            return "MATCHED"
        return "MISMATCH"

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

# ================= DB HELPERS =================

def save_to_history(job_id, test_case, status, accuracy, stats):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO execution_history
        (job_id, test_case, execution_date, status, accuracy,
         key_match_rate, field_accuracy,
         total_records, matched, mismatched, source_only, target_only,
         null_mismatches, source_duplicates, target_duplicates,
         src_row_count, tgt_row_count,
         keys_in_both, keys_src_only, keys_tgt_only)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
               keys_in_both, keys_src_only, keys_tgt_only
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
        }
        for r in rows
    ]

# ================= ENDPOINTS =================

@app.get("/health")
def health():
    return {"status": "ok", "version": "9.2-BR", "businessRules": [
        "address_normalized_strict",   # Strip suite suffix + expand abbrevs + case-insensitive
        "address2_suite_ignored",      # NEW: null src vs STE/UNIT target => MATCHED
        "address_component",           # Strict component match
        "first_name_with_initial",     # Handles embedded middle initial + flexible names
        "numeric_flexible",            # Phone, IDs, numbers -- strips punctuation
        "boolean_match",               # Auto-detects Y/N, true/false, 1/0
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
               keys_in_both, keys_src_only, keys_tgt_only
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
    }

# ================= MAIN COMPARE ENDPOINT =================

@app.post("/db/compare")
def compare(req: Dict[str, Any]):
    key_map   = req.get("keyMappings", [])
    field_map = req.get("fieldMappings", req.get("comparisonRules", []))
    filters   = req.get("filters", [])
    src_filters = [f for f in filters if f.get("side") == "source"]
    tgt_filters = [f for f in filters if f.get("side") == "target"]

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

            def build_key(row, mappings, side):
                if not mappings: return ("NO_KEY",)
                return tuple(row.get(m.get(side)) for m in mappings)

            tgt_lookup, tgt_dup_keys = {}, set()
            for r in tgt_rows:
                k = build_key(r, key_map, "tgt")
                if k in tgt_lookup: tgt_dup_keys.add(k)
                else: tgt_lookup[k] = r

            src_lookup, src_dup_keys = {}, set()
            for r in src_rows:
                k = build_key(r, key_map, "src")
                if k in src_lookup: src_dup_keys.add(k)
                else: src_lookup[k] = r

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

                if key in src_dup_keys:
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
            }
            save_to_history(job_id, req.get("testCaseNo", "N/A"), "COMPLETED", accuracy, stats)
            with lock:
                jobs[job_id]["status"]   = "COMPLETED"
                jobs[job_id]["progress"] = 100
                jobs[job_id]["stats"]    = stats
                execution_signatures.discard(signature)
                cancel_events.pop(job_id, None)
            print(f"=== ETL DONE (v9.2-BR) | kmr={key_match_rate}% acc={field_accuracy}% ===\n")

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
                            filename=f"etl_results_{job_id[:8]}.csv")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ================= START =================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api_server:app", host="0.0.0.0", port=8000, reload=True)