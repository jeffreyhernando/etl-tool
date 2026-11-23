"""
tests/test_set_compare.py
─────────────────────────
Set compare (v9.4): source list column (SpecialtyJSON / CSV) vs one target row
per item (Compass_ProviderSpecialty-style), per parent key.
Scenarios mirror the manual SpecialtyJSON → Compass_ProviderSpecialty checks.
"""

import importlib
import json
import re
import sys

import pytest

from core.set_compare import (
    MATCHED, SET_MISSING, SET_EXTRA, SET_EXTRA_ALLOWED, SET_REMAPPED,
    SET_REMAPPED_CATEGORY, EMPTY_SOURCE_LIST, PRIMARY_NOT_LOADED, INVALID_ITEM_ID,
    UNMAPPED, normalize_item, parse_list_items, parse_crosswalk, parse_id_list,
    items_from_row, merge_crosswalks, compare_item_sets, summarize_set_outcomes,
)


def statuses(outcomes):
    return sorted(o["status"] for o in outcomes)


# ── Parsing / normalization ──────────────────────────────────────────────────

class TestParsing:

    def test_json_string_ids_cast_to_int(self):
        raw = '[{"SpecId":"102","Specialty":"Orthopedic"},{"SpecId":"163","Specialty":"Orthopedic Surgery"}]'
        items, err = parse_list_items(raw, "json", "SpecId", "Specialty", "int")
        assert err is None
        assert items == [(102, "Orthopedic"), (163, "Orthopedic Surgery")]

    def test_json_keys_are_case_insensitive(self):
        items, _ = parse_list_items('[{"specid":"70"}]', "json", "SpecId", "", "int")
        assert items == [(70, "")]

    def test_json_unicode_escape_decoded_and_label_trimmed(self):
        raw = '[{"SpecId":"94","Specialty":"Obstetrics \\u0026 Gynecology "}]'
        items, _ = parse_list_items(raw, "json", "SpecId", "Specialty", "int")
        assert items == [(94, "Obstetrics & Gynecology")]

    def test_invalid_json_reports_error_instead_of_raising(self):
        items, err = parse_list_items(".[{bad", "json", "SpecId")
        assert items == [] and err.startswith("Invalid JSON")

    def test_blank_csv_is_empty_not_zero(self):
        assert parse_list_items("", "csv") == ([], None)
        assert parse_list_items("50,64,", "csv")[0] == [(50, ""), (64, "")]

    def test_normalize_item(self):
        assert normalize_item("50") == 50
        assert normalize_item(" 50 ") == 50
        assert normalize_item(50.0) == 50
        assert normalize_item("") is None
        assert normalize_item("abc") == "ABC"
        assert normalize_item(" Cardiology ", "string") == "CARDIOLOGY"

    def test_crosswalk_and_allowed_list(self):
        assert parse_crosswalk("712=100, 301->717\n602:240") == {712: 100, 301: 717, 602: 240}
        assert parse_id_list("100, 240") == {100, 240}


# ── Per-key comparison ───────────────────────────────────────────────────────

class TestCompareItemSets:

    def test_all_items_loaded(self):
        out = compare_item_sets([(102, "Orthopedic"), (163, "Orthopedic Surgery")],
                                [(102, ""), (163, "")], primary=163)
        assert statuses(out) == [MATCHED, MATCHED]

    def test_one_missing_one_extra_pairs_as_remap(self):
        out = compare_item_sets([(712, "OPHTHALMOLOGY / RETINA")], [(100, "")])
        assert statuses(out) == [SET_REMAPPED]
        assert out[0]["source"].startswith("712") and out[0]["target"].startswith("100")

    def test_crosswalk_makes_approved_remap_a_match(self):
        out = compare_item_sets([(712, "")], [(100, "")], crosswalk={712: 100})
        assert statuses(out) == [MATCHED]
        assert out[0]["source"] == "712 → 100"

    def test_unpaired_missing_and_extra(self):
        assert statuses(compare_item_sets([(50, ""), (64, "")], [(50, "")])) == [MATCHED, SET_MISSING]
        assert statuses(compare_item_sets([(50, "")], [(50, ""), (102, "")])) == [MATCHED, SET_EXTRA]

    def test_allowed_extra(self):
        out = compare_item_sets([(50, "")], [(50, ""), (100, "")], allowed_extras={100})
        assert statuses(out) == [MATCHED, SET_EXTRA_ALLOWED]

    def test_empty_source_list_with_target_rows(self):
        out = compare_item_sets([], [(102, "")])
        assert statuses(out) == [EMPTY_SOURCE_LIST]
        assert compare_item_sets([], []) == []

    def test_primary_remapped_is_flagged(self):
        out = compare_item_sets([(602, ""), (609, "")], [(240, ""), (609, "")], primary=602)
        assert statuses(out) == [MATCHED, PRIMARY_NOT_LOADED, SET_REMAPPED]
        primary = next(o for o in out if o["kind"] == "primary")
        assert "remapped to 240" in primary["target"]

    def test_lookup_flags_category_change_and_pairs_same_category_first(self):
        lookup = {
            602: {"label": "DIABETES SERVICES", "category": ("10", "SP")},
            240: {"label": "DIABETES CLINIC",   "category": ("5", "AN")},
            301: {"label": "FOOT & ANKLE SURGERY",              "category": ("15", "SP")},
            717: {"label": "ORTHOPEDIC SURGERY - FOOT & ANKLE", "category": ("15", "SP")},
        }
        # Extras listed so in-order pairing would mismatch them; category pairing must win.
        out = compare_item_sets([(301, ""), (602, "")], [(240, ""), (717, "")], lookup=lookup)
        by_src = {o["source"].split(" · ")[0]: o for o in out}
        assert by_src["301"]["status"] == SET_REMAPPED and by_src["301"]["target"].startswith("717")
        assert by_src["602"]["status"] == SET_REMAPPED_CATEGORY and by_src["602"]["target"].startswith("240")
        assert "DIABETES CLINIC" in by_src["602"]["target"]      # label filled from lookup

    def test_lookup_flags_unknown_item(self):
        out = compare_item_sets([(999, "")], [(999, "")], lookup={})
        assert statuses(out) == [INVALID_ITEM_ID, INVALID_ITEM_ID, MATCHED]


class TestSummary:

    def test_groups_consistent_remap_into_one_line(self):
        rows = [("712 · OPHTHALMOLOGY / RETINA", "100 · OPHTHALMOLOGY", SET_REMAPPED)] * 3 + [
            ("602 · DIABETES SERVICES", "240 · DIABETES CLINIC", SET_REMAPPED_CATEGORY),
            ("50 · X", "50 · X", MATCHED),
        ]
        summary = summarize_set_outcomes(rows)
        assert summary[0]["status"] == SET_REMAPPED_CATEGORY          # suspicious first
        assert summary[1] == {"status": SET_REMAPPED, "srcItem": "712", "srcLabel": "OPHTHALMOLOGY / RETINA",
                              "tgtItem": "100", "tgtLabel": "OPHTHALMOLOGY", "count": 3}
        assert len(summary) == 2


# ── End to end through api_server.run_set_compare (fake SQL Server cursor) ──

class FakeCursor:
    """Answers the COUNT / paged SELECT / IN-list SELECT statements the tool issues."""

    def __init__(self, tables):
        self.tables, self._rows = tables, []

    def execute(self, sql, params=()):
        params = list(params)
        table = re.search(r"FROM (\S+)", sql).group(1).split(".")[-1].strip("[]")
        rows = self.tables[table]
        if sql.startswith("SELECT COUNT(*)"):
            self._rows = [(len(rows),)]
            return
        cols = [c.strip().strip("[]") for c in re.match(r"SELECT (.+?) FROM", sql).group(1).split(",")]
        m = re.search(r"WHERE \[?(\w+)\]? IN \(", sql)
        eq = re.search(r"WHERE \[(\w+)\] = \?", sql)
        if m:
            wanted = set(params)
            rows = [r for r in rows if r.get(m.group(1)) in wanted]
        elif eq:
            rows = [r for r in rows if str(r.get(eq.group(1))) == params[0]]
        elif "OFFSET" in sql:
            off, n = map(int, re.search(r"OFFSET (\d+) ROWS FETCH NEXT (\d+)", sql).groups())
            rows = rows[off:off + n]
        self._rows = [tuple(r.get(c) for c in cols) for r in rows]

    def fetchone(self):
        return self._rows[0]

    def fetchall(self):
        return self._rows


class FakeConn:
    def __init__(self, tables):
        self.tables = tables

    def cursor(self):
        return FakeCursor(self.tables)

    def close(self):
        pass


@pytest.fixture()
def api(tmp_path, monkeypatch):
    monkeypatch.setenv("ETL_DB_PATH", str(tmp_path / "etl_history_test.db"))
    sys.modules.pop("api_server", None)
    module = importlib.import_module("api_server")
    yield module
    sys.modules.pop("api_server", None)


def spec_json(*pairs):
    return json.dumps([{"SpecId": str(i), "Specialty": s} for i, s in pairs])


def test_run_set_compare_end_to_end(api):
    master = [
        {"Tothpdpyid": 1, "SpecialtyJSON": spec_json((102, "Orthopedic"), (163, "Orthopedic Surgery")), "Spec1_id": 163},
        {"Tothpdpyid": 2, "SpecialtyJSON": spec_json((602, "DIABETES SERVICES"), (609, "Endocrine")), "Spec1_id": 602},
        {"Tothpdpyid": 3, "SpecialtyJSON": spec_json((712, "OPHTHALMOLOGY / RETINA")), "Spec1_id": 712},
        {"Tothpdpyid": 4, "SpecialtyJSON": ".not json", "Spec1_id": 70},
        {"Tothpdpyid": 5, "SpecialtyJSON": spec_json((70, "Cardiology")), "Spec1_id": 70},
    ]
    final = [
        {"TothpdpyId": 1, "Spec_ID": 102}, {"TothpdpyId": 1, "Spec_ID": 163},
        {"TothpdpyId": 2, "Spec_ID": 240}, {"TothpdpyId": 2, "Spec_ID": 609},
        {"TothpdpyId": 3, "Spec_ID": 100},
        {"TothpdpyId": 4, "Spec_ID": 70},
        {"TothpdpyId": 99, "Spec_ID": 50},     # other source code -- must not be fetched
    ]
    listing = [
        {"spec_id": 100, "specialty": "OPHTHALMOLOGY", "cat1_id": 26},
        {"spec_id": 712, "specialty": "OPHTHALMOLOGY / RETINA", "cat1_id": 26},
        {"spec_id": 602, "specialty": "DIABETES SERVICES", "cat1_id": 10},
        {"spec_id": 240, "specialty": "DIABETES CLINIC", "cat1_id": 5},
        {"spec_id": 102, "specialty": "ORTHOPEDIC", "cat1_id": 15},
        {"spec_id": 163, "specialty": "ORTHOPEDIC SURGERY", "cat1_id": 15},
        {"spec_id": 609, "specialty": "ENDOCRINE & METABOLISM", "cat1_id": 10},
        {"spec_id": 70, "specialty": "CARDIOLOGY", "cat1_id": 3},
    ]
    src = FakeConn({"Compass_MASTER_Provider": master})
    tgt = FakeConn({"Compass_ProviderSpecialty": final, "Specialty_Listing": listing})
    req = {"srcTable": {"schema": "dbo", "name": "Compass_MASTER_Provider"},
           "tgtTable": {"schema": "dbo", "name": "Compass_ProviderSpecialty"}}
    key_map = [{"src": "Tothpdpyid", "tgt": "TothpdpyId"}]
    set_cfg = {"enabled": True, "srcListCol": "SpecialtyJSON", "srcFormat": "json",
               "itemKey": "SpecId", "itemLabel": "Specialty", "tgtItemCol": "Spec_ID",
               "itemCast": "int", "srcPrimaryCol": "Spec1_id",
               "lookup": {"table": "CAS.dbo.Specialty_Listing", "keyCol": "spec_id",
                          "labelCol": "specialty", "categoryCols": "cat1_id"}}
    job_id = "test-set-job"
    api.jobs[job_id] = {"status": "RUNNING", "progress": 0, "error": None}

    stats = api.run_set_compare(job_id, req, key_map, set_cfg, [], [], src, tgt,
                                api.safe_limit(0), lambda: None, None)

    c = stats["set_counts"]
    assert stats["compare_mode"] == "set" and stats["target_scoped"] is True
    assert c["MATCHED"] == 3                      # 102, 163, 609
    assert c["SET_REMAPPED"] == 1                 # 712 → 100 (same category)
    assert c["SET_REMAPPED_CATEGORY"] == 1        # 602 → 240 (category 10 → 5)
    assert c["PRIMARY_NOT_LOADED"] == 3           # Spec1_id 602, 712 remapped; 70 never loaded
    assert c["INVALID_SOURCE_JSON"] == 1          # key 4
    # Key 5 has JSON items but no target rows (stale JSON): each item is missing.
    assert c["SET_MISSING"] == 1
    assert c["SET_EXTRA"] == c["INVALID_ITEM_ID"] == 0
    assert stats["source_only"] == 0 and stats["keys_src_only"] == 1
    assert stats["target_only"] == 0              # key 99 never fetched
    assert stats["mismatched"] == 7
    assert stats["lookup_error"] == ""

    summary = api.set_summary(job_id)["summary"]
    assert [(s["status"], s["srcItem"], s["tgtItem"]) for s in summary] == [
        ("SET_REMAPPED_CATEGORY", "602", "240"), ("SET_REMAPPED", "712", "100"),
        ("SET_MISSING", "70", "")]

    fs = {f["field"]: f for f in api.field_summary(job_id)["summary"]}
    assert fs["SpecialtyJSON"]["mismatched"] == 4  # 2 remaps + invalid JSON + stale item
    assert fs["Spec1_id"]["mismatched"] == 3


# ── Raw specialty text → SpecialtyJSON (item rows on the source side) ───────

class TestTextCrosswalk:

    def test_text_crosswalk_newlines_keep_commas(self):
        xw = parse_crosswalk("NURSE PRACTITIONER=57\nSURGERY, GENERAL=310\n family medicine physician = 89",
                             "string", "int")
        assert xw == {"NURSE PRACTITIONER": 57, "SURGERY, GENERAL": 310, "FAMILY MEDICINE PHYSICIAN": 89}

    def test_text_with_colon_prefers_equals_separator(self):
        assert parse_crosswalk("OPHTHALMOLOGY: RETINA=712", "string", "int") == {"OPHTHALMOLOGY: RETINA": 712}

    def test_items_from_row_multiple_columns(self):
        side = {"shape": "rows", "itemCols": ["Specialty1", "Specialty2"], "cast": "string"}
        items, err = items_from_row({"Specialty1": "Nurse Practitioner ", "Specialty2": ""}, side)
        assert err is None and items == [("NURSE PRACTITIONER", "")]

    def test_required_crosswalk_reports_unmapped(self):
        out = compare_item_sets([("NURSE PRACTITIONER", ""), ("ACUPUNCTURE", "")], [(57, "")],
                                crosswalk={"NURSE PRACTITIONER": 57}, require_crosswalk=True)
        assert statuses(out) == [MATCHED, UNMAPPED]
        assert next(o for o in out if o["status"] == MATCHED)["source"] == "NURSE PRACTITIONER → 57"

    def test_text_mapped_to_several_ids_matches_any(self):
        xw = merge_crosswalks({"CARDIOLOGY": 70}, {"CARDIOLOGY": 71})
        assert xw == {"CARDIOLOGY": (70, 71)}
        assert statuses(compare_item_sets([("CARDIOLOGY", "")], [(71, "")], crosswalk=xw)) == [MATCHED]

    def test_wrong_mapping_shows_expected_vs_actual(self):
        out = compare_item_sets([("FAMILY MEDICINE PHYSICIAN", "")], [(57, "")],
                                crosswalk={"FAMILY MEDICINE PHYSICIAN": 89}, require_crosswalk=True)
        assert statuses(out) == [SET_REMAPPED]
        assert out[0]["source"] == "FAMILY MEDICINE PHYSICIAN → 89" and out[0]["target"] == "57"


def test_run_set_compare_raw_text_to_json(api):
    providers = [   # several raw rows per provider; Specialty1 + Specialty2 both count
        {"outer_code": "PA", "outer_num": "001", "Specialty1": "NURSE PRACTITIONER", "Specialty2": ""},
        {"outer_code": "PA", "outer_num": "001", "Specialty1": "nurse practitioner ", "Specialty2": "FAMILY MEDICINE PHYSICIAN"},
        {"outer_code": "PA", "outer_num": "002", "Specialty1": "INTERNAL MEDICINE PHYSICIAN", "Specialty2": None},
        {"outer_code": "PB", "outer_num": "003", "Specialty1": "ACUPUNCTURE", "Specialty2": ""},
        {"outer_code": "PB", "outer_num": "004", "Specialty1": "NURSE PRACTITIONER", "Specialty2": ""},
        {"outer_code": "PB", "outer_num": "005", "Specialty1": "NURSE PRACTITIONER", "Specialty2": ""},
    ]
    master = [
        {"Outer_Code": "PA", "Outer_Num": "001", "SpecialtyJSON": spec_json((57, "Certified Nurse Practitioner"),
                                                                            (89, "Family Practice / General Practice"))},
        {"Outer_Code": "PA", "Outer_Num": "002", "SpecialtyJSON": spec_json((70, "Cardiology"))},   # wrong ID
        {"Outer_Code": "PB", "Outer_Num": "003", "SpecialtyJSON": "[]"},
        {"Outer_Code": "PB", "Outer_Num": "004", "SpecialtyJSON": "{broken"},
        # PB/005 never reached the master table
    ]
    mapping = [
        {"RawSpecialty": "NURSE PRACTITIONER", "Spec_ID": 57, "SourceCode": "AB227"},
        {"RawSpecialty": "FAMILY MEDICINE PHYSICIAN", "Spec_ID": 89, "SourceCode": "AB227"},
        {"RawSpecialty": "INTERNAL MEDICINE PHYSICIAN", "Spec_ID": 581, "SourceCode": "AB227"},
        {"RawSpecialty": "ACUPUNCTURE", "Spec_ID": 999, "SourceCode": "OTHER"},   # filtered out
    ]
    src = FakeConn({"Providers": providers, "SpecialtyCrosswalk": mapping})
    tgt = FakeConn({"Compass_MASTER_Provider": master})
    req = {"srcTable": {"schema": "dbo", "name": "Providers"},
           "tgtTable": {"schema": "dbo", "name": "Compass_MASTER_Provider"}}
    key_map = [{"src": "outer_code", "tgt": "Outer_Code"}, {"src": "outer_num", "tgt": "Outer_Num"}]
    set_cfg = {"enabled": True,
               "src": {"shape": "rows", "itemCols": ["Specialty1", "Specialty2"], "cast": "string"},
               "tgt": {"shape": "list", "listCol": "SpecialtyJSON", "format": "json",
                       "itemKey": "SpecId", "itemLabel": "Specialty", "cast": "int"},
               "crosswalkRequired": True,
               "crosswalkTable": {"table": "ProvConv.dbo.SpecialtyCrosswalk", "fromCol": "RawSpecialty",
                                  "toCol": "Spec_ID", "filterCol": "SourceCode", "filterValue": "AB227"}}
    job_id = "test-text-job"
    api.jobs[job_id] = {"status": "RUNNING", "progress": 0, "error": None}

    stats = api.run_set_compare(job_id, req, key_map, set_cfg, [], [], src, tgt,
                                api.safe_limit(0), lambda: None, None)

    c = stats["set_counts"]
    assert stats["crosswalk_entries"] == 3
    assert c["MATCHED"] == 2                  # PA/001: 57 and 89, merged across two raw rows
    assert c["SET_REMAPPED"] == 1             # PA/002: expected 581, JSON has 70
    assert c["UNMAPPED"] == 1                 # PB/003: ACUPUNCTURE has no AB227 mapping
    assert c["INVALID_TARGET_JSON"] == 1      # PB/004
    assert stats["source_only"] == 1          # PB/005 not in the master table
    assert c["SET_MISSING"] == c["SET_EXTRA"] == 0

    summary = api.set_summary(job_id)["summary"]
    assert ("SET_REMAPPED", "INTERNAL MEDICINE PHYSICIAN → 581", "70") in \
        [(s["status"], s["srcItem"], s["tgtItem"]) for s in summary]
    assert ("UNMAPPED", "ACUPUNCTURE", "") in [(s["status"], s["srcItem"], s["tgtItem"]) for s in summary]


# ── Key normalization (trim + case-insensitive key matching) ──────────────────
def test_key_component_trims_and_ignores_case(api):
    n = api.normalize_key_component
    assert n("Plan", "CHIP      ") == n("Plans", "CHIP")
    assert n("ChipID", "AbcDeF    ") == n("ChipID", "ABCDEF")
    assert n("AddressLine1", "100 Main St") == n("Addressline1", "100 MAIN ST")
    assert n("NPI", " 123-456 7890") == n("NPI", "1234567890")
    assert n("ZipCode", "77001") == "77001"
    assert n("Plan", None) is None
    assert n("Plan", 42) == 42                     # non-text values unchanged
    assert n("Plan", "CHIP") != n("Plan", "STAR")  # real differences still differ


# ── Filters: explicit Is Null / Is Not Null operators ─────────────────────────
def test_where_clause_is_null_operators(api):
    w, p = api.build_where_clause([{"field": "TermDate", "operator": "is_null", "value": ""},
                                   {"field": "SourceCode", "operator": "equals", "value": "AB254"}])
    assert "[TermDate] IS NULL" in w and p == ["AB254"]
    w, p = api.build_where_clause([{"field": "TermDate", "operator": "is_not_null"}])
    assert w == " AND [TermDate] IS NOT NULL" and p == []


def test_multi_target_crosswalk_matches_every_loaded_id():
    # Raw text maps to fType 102 and pType 163; both loaded -> both matched, no extras.
    xw = {"ORTHOPEDIC": (102, 163)}
    out = compare_item_sets([("ORTHOPEDIC", "")], [(102, ""), (163, "")],
                            crosswalk=xw, require_crosswalk=True)
    assert statuses(out) == [MATCHED, MATCHED]
    # Only one loaded -> still matched, nothing extra.
    out = compare_item_sets([("ORTHOPEDIC", "")], [(163, "")], crosswalk=xw, require_crosswalk=True)
    assert statuses(out) == [MATCHED]
    # Neither loaded -> remapped to whatever is there.
    out = compare_item_sets([("ORTHOPEDIC", "")], [(45, "")], crosswalk=xw, require_crosswalk=True)
    assert statuses(out) == [SET_REMAPPED]


def test_value_in_list_rule_for_row_compare():
    from api_server import compare_field_values as cmp
    pairs = [
        ("Orthopedic Surgery", '[{"SpecId":"102","Specialty":"Orthopedic"},{"SpecId":"163","Specialty":"Orthopedic Surgery"}]'),
        ("Psychiatry", '[{"SpecId":"142","Specialty":"Psychiatry"}]'),
        ("Endocrinology", '[{"SpecId":"75","Specialty":"Endocrinology"}]'),
        ("Anesthesiology (DO)", '[{"SpecId":"68","Specialty":"Anesthesiology"},{"SpecId":"103","Specialty":"Pain Management"}]'),
        ("Family Practice", '[{"SpecId":"89","Specialty":"Family Practice / General Practice"}]'),
        ("Physical Medicine & Rehabilitation", '[{"SpecId":"138","Specialty":"PHYSICAL MEDICINE \u0026 REHABILITATION"}]'),
        ("102", '[{"SpecId":"102","Specialty":"Orthopedic"}]'),
        ("Radiology", "Neurology, Radiology"),
    ]
    for src, tgt in pairs:
        assert cmp(src, tgt, "Specialty1", "value_in_list") == "MATCHED", src
    # Genuinely different names still mismatch.
    assert cmp("Family Medicine", '[{"SpecId":"89","Specialty":"Family Practice / General Practice"}]',
               "Specialty1", "value_in_list") == "MISMATCH"
    assert cmp("Psychiatry", '[{"SpecId":"142"', "Specialty1", "value_in_list") == "MISMATCH"   # bad JSON
    assert cmp(None, '[{"SpecId":"142"}]', "Specialty1", "value_in_list") == "NULL_MISMATCH"


def test_value_in_list_loose_names_and_translation_fallback():
    from api_server import compare_field_values as cmp
    J = lambda *p: "[" + ",".join('{"SpecId":"%s","Specialty":"%s"}' % x for x in p) + "]"
    by_name = [
        ("OB/GYN-Obstetrics and Gynecology", J(("93", "Obstetrics"), ("94", "Obstetrics \u0026 Gynecology"))),
        ("Pediatrics", J(("104", "Pediatric"))),
        ("Pediatrics (DO)", J(("104", "Pediatric"))),
        ("Podiatry-Foot and Ankle surgery", J(("140", "Podiatry"), ("301", "FOOT \u0026 ANKLE SURGERY"))),
        ("Allergy - Immunology", J(("67", "Allergy \u0026 Immunology"))),
        ("Oral Maxillo-Facial Surgery", J(("162", "Oral \u0026 Maxillofacial Surgery"))),
        ("Critical Care Medicine", J(("581", "Internal Medicine"), ("600", "Critical Care"))),
        ("Cardio-Vascular Disease (DO)", J(("71", "Cardiovascular Disease"))),
        ("Orthopaedic Sports Medicine", J(("102", "Orthopedic"), ("150", "Sports Medicine"))),
        ("Diagnostic Radiology", J(("294", "Radiology"))),
        ("Plastic Surgery", J(("139", "Plastic / Reconstructive Surgery"))),
    ]
    for src, tgt in by_name:
        assert cmp(src, tgt, "Specialty1", "value_in_list") == "MATCHED", src

    fam = J(("89", "Family Practice / General Practice"), ("150", "Sports Medicine"))
    assert cmp("Family Medicine", fam, "Specialty1", "value_in_list") == "MISMATCH"
    xw = {"FAMILY MEDICINE": {"89"}, "CLINICAL NEUROPSYCHOLOGY": {"143"}}
    assert cmp("Family Medicine", fam, "Specialty1", "value_in_list", xw) == "MATCHED"
    assert cmp("Clinical Neuropsychology", J(("92", "Neurology")), "Specialty1", "value_in_list", xw) == "MISMATCH"
    assert cmp("Economist", J(("17", "NAPRAPATH")), "Specialty1", "value_in_list", xw) == "MISMATCH"


def test_load_list_translation_multi_columns():
    from api_server import load_list_translation

    class Cur:
        def execute(self, sql, params): self.sql = sql
        def fetchall(self): return [("Family Medicine ", 89, None), ("PAIN MEDICINE", "103", 138.0), (None, 1, 2)]
    c = Cur()
    xw = load_list_translation(c, {"table": "CAS.dbo.Specialty_Conversions", "fromCol": "specialty", "toCol": "fType, pType"})
    assert "[CAS].[dbo].[Specialty_Conversions]" in c.sql and "[pType]" in c.sql
    assert xw == {"FAMILY MEDICINE": {"89"}, "PAIN MEDICINE": {"103", "138"}}


def test_pasted_list_translation():
    from api_server import parse_list_translation_text, compare_field_values as cmp
    xw = parse_list_translation_text("Pain Medicine = 103\nOtolaryngology -> 74, 328\njunk line\n")
    assert xw == {"PAIN MEDICINE": {"103"}, "OTOLARYNGOLOGY": {"74", "328"}}
    tgt = '[{"SpecId":"103","Specialty":"Pain Management"},{"SpecId":"138","Specialty":"Physical Medicine \u0026 Rehabilitation"}]'
    assert cmp("Pain Medicine", tgt, "Specialty1", "value_in_list") == "MISMATCH"
    assert cmp("Pain Medicine", tgt, "Specialty1", "value_in_list", xw) == "MATCHED"
    assert cmp("Pain Medicine (DO)", tgt, "Specialty1", "value_in_list", xw) == "MATCHED"
