"""
core/set_compare.py
───────────────────
Set comparison for one-to-many checks, per parent key. Each side is either:
  list -- one column holding a list (JSON array / CSV), e.g. SpecialtyJSON
  rows -- one or more item columns spread over many rows per key, e.g.
          Compass_ProviderSpecialty.Spec_ID, or raw Specialty1 + Specialty2

Examples:
  SpecialtyJSON (list)        vs Compass_ProviderSpecialty.Spec_ID (rows)
  Raw Specialty1/2 text (rows) vs SpecialtyJSON (list), via a text → SpecId crosswalk

Items are compared as sets per key, with crosswalk translation (pasted or
loaded from a mapping table), remap pairing (one missing + one extra =
REMAPPED) and optional reference-lookup enrichment.

Pure functions only -- DB access lives in api_server.py.
"""

import json
import re
from collections import Counter
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

# ── Result statuses ──────────────────────────────────────────────────────────
MATCHED               = "MATCHED"
SET_MISSING           = "SET_MISSING"            # in source, not in target
SET_EXTRA             = "SET_EXTRA"              # in target, not in source
SET_EXTRA_ALLOWED     = "SET_EXTRA_ALLOWED"      # extra, but on the allowed-exceptions list
SET_REMAPPED          = "SET_REMAPPED"           # missing x paired with extra y
SET_REMAPPED_CATEGORY = "SET_REMAPPED_CATEGORY"  # remap that also changes lookup category
UNMAPPED              = "UNMAPPED"               # source item has no crosswalk entry
INVALID_SOURCE_JSON   = "INVALID_SOURCE_JSON"
INVALID_TARGET_JSON   = "INVALID_TARGET_JSON"
EMPTY_SOURCE_LIST     = "EMPTY_SOURCE_LIST"      # no source items, but target has items
PRIMARY_NOT_LOADED    = "PRIMARY_NOT_LOADED"     # e.g. Spec1_id not in target
INVALID_ITEM_ID       = "INVALID_ITEM_ID"        # item not found in reference lookup

# Statuses that count against field accuracy / mismatch totals.
PROBLEM_STATUSES = (
    SET_MISSING, SET_EXTRA, SET_REMAPPED, SET_REMAPPED_CATEGORY, UNMAPPED,
    INVALID_SOURCE_JSON, INVALID_TARGET_JSON, EMPTY_SOURCE_LIST,
    PRIMARY_NOT_LOADED, INVALID_ITEM_ID,
)
ALL_SET_STATUSES = (MATCHED, SET_EXTRA_ALLOWED) + PROBLEM_STATUSES

# Statuses grouped by /db/set-summary (one line per distinct src → tgt item pair).
SUMMARY_STATUSES = (
    SET_REMAPPED_CATEGORY, SET_REMAPPED, UNMAPPED, SET_MISSING, SET_EXTRA, SET_EXTRA_ALLOWED,
)

LABEL_SEP = " · "
_LIST_SPLIT_RE = re.compile(r"[,;|\n]")
_CROSSWALK_SEPS = ("->", "=>", "→", "=", ":")


# ── Item normalization ───────────────────────────────────────────────────────

def normalize_item(value: Any, cast: str = "int") -> Any:
    """
    Normalizes one list item / target cell for set membership.
      int    -- "50", " 50 ", 50, 50.0, Decimal('50') -> 50; non-numeric kept as text
      string -- trimmed, inner whitespace collapsed, case-insensitive (upper-cased)
    Blank / None -> None (skipped), so an empty CSV never turns into a fake 0.
    """
    if value is None:
        return None
    s = re.sub(r"\s+", " ", str(value)).strip()
    if not s or s.lower() == "null":
        return None
    if cast == "int":
        try:
            return int(s)
        except ValueError:
            try:
                f = float(s)
                if f.is_integer():
                    return int(f)
            except ValueError:
                pass
    return s.upper()


def _get_ci(d: Dict[str, Any], key: str) -> Any:
    """Case-insensitive dict lookup (JSON keys like SpecId / specid / SPECID)."""
    if not key:
        return None
    if key in d:
        return d[key]
    lk = key.lower()
    for k, v in d.items():
        if str(k).lower() == lk:
            return v
    return None


def _clean_label(v: Any) -> str:
    return re.sub(r"\s+", " ", str(v)).strip() if v is not None else ""


def _dedupe_items(elements: Iterable[Tuple[Any, Any]], cast: str) -> List[Tuple[Any, str]]:
    items, seen = [], set()
    for val, label in elements:
        item = normalize_item(val, cast)
        if item is None or item in seen:
            continue
        seen.add(item)
        items.append((item, _clean_label(label)))
    return items


def parse_list_items(raw: Any, fmt: str = "json", item_key: str = "",
                     label_key: str = "", cast: str = "int"
                     ) -> Tuple[List[Tuple[Any, str]], Optional[str]]:
    """
    Parses a list column into [(item, label), ...] (deduped, order kept).
    Returns (items, error). error is set only for unparseable JSON.

      json -- array of objects ([{"SpecId":"50","Specialty":"..."}]) or scalars
              ([50, 64]); a single object is treated as a one-item array.
              JSON escapes (\\u0026) are decoded by json.loads.
      csv  -- "50,64,70" (also accepts ; | or newline as separators)
    """
    if raw is None or not str(raw).strip():
        return [], None

    elements: List[Tuple[Any, Any]] = []
    if (fmt or "json").lower() == "csv":
        elements = [(part, None) for part in _LIST_SPLIT_RE.split(str(raw))]
    else:
        try:
            data = json.loads(str(raw))
        except ValueError as e:
            return [], f"Invalid JSON: {e}"
        if isinstance(data, dict):
            data = [data]
        if not isinstance(data, list):
            return [], f"Invalid JSON: expected an array, got {type(data).__name__}"
        for el in data:
            if isinstance(el, dict):
                elements.append((_get_ci(el, item_key), _get_ci(el, label_key)))
            else:
                elements.append((el, None))
    return _dedupe_items(elements, cast), None


def items_from_row(row: Dict[str, Any], side: Dict[str, Any]
                   ) -> Tuple[List[Tuple[Any, str]], Optional[str]]:
    """
    Items one row contributes for a side config:
      {"shape": "list", "listCol", "format", "itemKey", "itemLabel", "cast"}
      {"shape": "rows", "itemCols": [...], "labelCol", "cast"}
    """
    cast = side.get("cast") or "int"
    if side.get("shape") == "list":
        return parse_list_items(row.get(side.get("listCol")), side.get("format") or "json",
                                side.get("itemKey") or "", side.get("itemLabel") or "", cast)
    label_col = side.get("labelCol") or ""
    label = row.get(label_col) if label_col else None
    return _dedupe_items(((row.get(c), label) for c in side.get("itemCols") or []), cast), None


def merge_items(into: List[Tuple[Any, str]], items: Iterable[Tuple[Any, str]]) -> None:
    """Adds items not already present (by item), keeping first-seen order/label."""
    seen = {i for i, _ in into}
    for item, label in items:
        if item not in seen:
            seen.add(item)
            into.append((item, label))


# ── Crosswalk / allowed list ─────────────────────────────────────────────────

def _split_pair(entry: str) -> Optional[Tuple[str, str]]:
    for sep in _CROSSWALK_SEPS:
        if sep in entry:
            a, b = entry.split(sep, 1)
            return a, b
    return None


def build_crosswalk(pairs: Iterable[Tuple[Any, Any]], cast: str = "int",
                    value_cast: Optional[str] = None) -> Dict[Any, Any]:
    """
    Source item → expected target item. A source item mapped to several target
    items keeps all of them as a tuple (any one of them counts as loaded).
    """
    value_cast = value_cast or cast
    multi: Dict[Any, List[Any]] = {}
    for a, b in pairs:
        na, nb = normalize_item(a, cast), normalize_item(b, value_cast)
        if na is None or nb is None:
            continue
        vals = multi.setdefault(na, [])
        if nb not in vals:
            vals.append(nb)
    return {k: v[0] if len(v) == 1 else tuple(v) for k, v in multi.items()}


def parse_crosswalk(spec: Any, cast: str = "int", value_cast: Optional[str] = None) -> Dict[Any, Any]:
    """
    Pasted crosswalk: a dict, or text like "712=100, 301->717" / one pair per
    line. A line is split on , ; | only when every piece is itself a pair, so
    specialty names containing commas ("SURGERY, GENERAL=310") stay intact.
    Separators: -> => → = :
    """
    if not spec:
        return {}
    if isinstance(spec, dict):
        return build_crosswalk(spec.items(), cast, value_cast)
    entries: List[str] = []
    for line in str(spec).split("\n"):
        pieces = [p.strip() for p in re.split(r"[,;|]", line) if p.strip()]
        if len(pieces) > 1 and all(_split_pair(p) for p in pieces):
            entries.extend(pieces)
        elif line.strip():
            entries.append(line.strip())
    pairs = [p for p in (_split_pair(e) for e in entries) if p]
    return build_crosswalk(pairs, cast, value_cast)


def merge_crosswalks(*crosswalks: Dict[Any, Any]) -> Dict[Any, Any]:
    """Later crosswalks add targets to earlier ones (pasted entries + table)."""
    pairs = []
    for xw in crosswalks:
        for k, v in xw.items():
            pairs.extend((k, x) for x in (v if isinstance(v, tuple) else (v,)))
    multi: Dict[Any, List[Any]] = {}
    for k, v in pairs:
        vals = multi.setdefault(k, [])
        if v not in vals:
            vals.append(v)
    return {k: v[0] if len(v) == 1 else tuple(v) for k, v in multi.items()}


def parse_id_list(spec: Any, cast: str = "int") -> Set[Any]:
    """Allowed-exceptions list: "100, 240" / list -> normalized set."""
    if not spec:
        return set()
    values = spec if isinstance(spec, (list, tuple, set)) else re.split(r"[,;|\n]+", str(spec))
    return {n for n in (normalize_item(v, cast) for v in values) if n is not None}


# ── Per-key comparison ───────────────────────────────────────────────────────

def format_item(item: Any, label: str = "") -> str:
    return f"{item}{LABEL_SEP}{label}" if label else str(item)


def _ids(items: Iterable[Tuple[Any, str]]) -> str:
    return ", ".join(str(i) for i, _ in items) or "-"


def compare_item_sets(src_items: List[Tuple[Any, str]],
                      tgt_items: List[Tuple[Any, str]],
                      crosswalk: Optional[Dict[Any, Any]] = None,
                      allowed_extras: Optional[Set[Any]] = None,
                      lookup: Optional[Dict[Any, Dict[str, Any]]] = None,
                      primary: Any = None,
                      require_crosswalk: bool = False) -> List[Dict[str, Any]]:
    """
    Compares one parent key's source items against its target items.

    Returns outcome dicts: {kind, status, source, target}
      kind -- "list" (item rows), "primary" (primary-item check),
              "lookup" (reference-lookup validity)
    crosswalk         -- {src item: tgt item | (tgt items...)}
    require_crosswalk -- source items without a crosswalk entry are UNMAPPED
                         (text → ID checks) instead of compared as-is
    lookup            -- {tgt item: {"label": str, "category": tuple}} or None
                         when no reference lookup is configured
    """
    crosswalk = crosswalk or {}
    allowed_extras = allowed_extras or set()
    out: List[Dict[str, Any]] = []

    def label_of(item, given=""):
        if given:
            return given
        if lookup and item in lookup:
            return lookup[item].get("label") or ""
        return ""

    def category_of(item):
        if lookup and item in lookup:
            return lookup[item].get("category")
        return None

    tgt_labels: Dict[Any, str] = {}
    for item, label in tgt_items:
        tgt_labels.setdefault(item, label)

    def resolve(item):
        """Expected target items for a source item ([] when unmapped). With
        several mapped targets (e.g. fType + pType), every one that was loaded
        counts as a match; if none was, the first is reported as missing."""
        if item not in crosswalk:
            return [] if require_crosswalk else [item]
        v = crosswalk[item]
        if isinstance(v, tuple):
            return [c for c in v if c in tgt_labels] or [v[0]]
        return [v]

    def src_display(orig, label, exp):
        disp = format_item(orig, label)
        return disp if exp == orig else f"{disp} → {exp}"

    if not src_items:
        if tgt_items:
            out.append({"kind": "list", "status": EMPTY_SOURCE_LIST, "source": "(empty)",
                        "target": ", ".join(format_item(i, label_of(i, l)) for i, l in tgt_items)})
        return out

    # Translate source items through the crosswalk before matching.
    expected: Dict[Any, Tuple[Any, str]] = {}   # expected tgt item -> (orig src item, label)
    for item, label in src_items:
        exps = resolve(item)
        if not exps:
            out.append({"kind": "list", "status": UNMAPPED,
                        "source": format_item(item, label), "target": "no crosswalk entry"})
            continue
        for exp in exps:
            expected.setdefault(exp, (item, label))

    missing: List[Tuple[Any, Any, str]] = []    # (expected, orig, label)
    for exp, (orig, label) in expected.items():
        if exp in tgt_labels:
            out.append({"kind": "list", "status": MATCHED,
                        "source": src_display(orig, label_of(orig, label), exp),
                        "target": format_item(exp, label_of(exp, tgt_labels[exp]))})
        else:
            missing.append((exp, orig, label))

    extras = [(i, l) for i, l in tgt_items if i not in expected]

    # Pair missing with extra as remaps: same lookup category first, then in order.
    pairs: List[Tuple[Tuple[Any, Any, str], Tuple[Any, str]]] = []
    if lookup:
        for m in list(missing):
            cat = category_of(m[0])
            if cat is None:
                continue
            hit = next((e for e in extras if category_of(e[0]) == cat), None)
            if hit:
                pairs.append((m, hit)); missing.remove(m); extras.remove(hit)
    while missing and extras:
        pairs.append((missing.pop(0), extras.pop(0)))

    remapped_to: Dict[Any, Any] = {}
    for (exp, orig, label), (t_item, t_label) in pairs:
        c_src, c_tgt = category_of(exp), category_of(t_item)
        status = (SET_REMAPPED_CATEGORY
                  if c_src is not None and c_tgt is not None and c_src != c_tgt
                  else SET_REMAPPED)
        remapped_to[exp] = t_item
        out.append({"kind": "list", "status": status,
                    "source": src_display(orig, label_of(orig, label), exp),
                    "target": format_item(t_item, label_of(t_item, t_label))})

    for exp, orig, label in missing:
        out.append({"kind": "list", "status": SET_MISSING,
                    "source": src_display(orig, label_of(orig, label), exp), "target": "-"})
    for t_item, t_label in extras:
        out.append({"kind": "list",
                    "status": SET_EXTRA_ALLOWED if t_item in allowed_extras else SET_EXTRA,
                    "source": "-", "target": format_item(t_item, label_of(t_item, t_label))})

    if primary is not None:
        p_exp = crosswalk.get(primary, primary)
        if isinstance(p_exp, tuple):
            p_exp = next((c for c in p_exp if c in tgt_labels), p_exp[0])
        if p_exp not in tgt_labels:
            note = (f"remapped to {remapped_to[p_exp]}" if p_exp in remapped_to
                    else "not loaded")
            out.append({"kind": "primary", "status": PRIMARY_NOT_LOADED,
                        "source": format_item(primary, label_of(primary)),
                        "target": f"{note} (loaded: {_ids(tgt_items)})"})

    if lookup is not None:
        for exp in expected:
            if exp not in lookup:
                out.append({"kind": "lookup", "status": INVALID_ITEM_ID,
                            "source": f"{exp} (source)", "target": "not in lookup"})
        for item, _ in tgt_items:
            if item not in lookup:
                out.append({"kind": "lookup", "status": INVALID_ITEM_ID,
                            "source": "not in lookup", "target": f"{item} (target)"})
    return out


# ── Job-level summary ────────────────────────────────────────────────────────

def _split_display(value: str) -> Tuple[str, str]:
    if not value or value == "-":
        return "", ""
    item, _, label = str(value).partition(LABEL_SEP)
    return item.strip(), label.strip()


def summarize_set_outcomes(rows: Iterable[Tuple[str, str, str]]) -> List[Dict[str, Any]]:
    """
    Groups (source_value, target_value, status) result rows into one line per
    distinct (status, source item, target item) with a key count -- a
    consistent crosswalk rule shows up as one line, inconsistent mappings as
    several lines for the same source item.
    """
    groups: Counter = Counter()
    labels: Dict[Tuple[str, str, str], Tuple[str, str]] = {}
    for source, target, status in rows:
        if status not in SUMMARY_STATUSES:
            continue
        s_item, s_label = _split_display(source)
        t_item, t_label = _split_display(target)
        if status == UNMAPPED:
            t_item, t_label = "", ""
        k = (status, s_item, t_item)
        groups[k] += 1
        labels.setdefault(k, (s_label, t_label))
    order = {s: i for i, s in enumerate(SUMMARY_STATUSES)}
    return [
        {"status": st, "srcItem": si, "srcLabel": labels[(st, si, ti)][0],
         "tgtItem": ti, "tgtLabel": labels[(st, si, ti)][1], "count": n}
        for (st, si, ti), n in sorted(groups.items(),
                                      key=lambda kv: (order[kv[0][0]], -kv[1], kv[0][1], kv[0][2]))
    ]
