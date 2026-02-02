# src/water_validation/checks.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import re
import math

#from llm_client import classify_funding_with_confidence
# from .llm_client import classify_funding_with_confidence


#from models import CheckResult, Severity, Status
from .models import CheckResult, Severity, Status

from difflib import SequenceMatcher
from .prompts import SUBJECT_TO_ALLOWED, SUBJECT_SYNONYMS, ALLOWED_FUNDING_LABELS, build_llm_prompt


#from config import PlanConfig
from .config import PlanConfig

#from models import CheckResult, Severity, Status
from .models import CheckResult, Severity, Status

from .utils import (
    detect_city_cols,
    excel_row_to_df_index,
    normalize_text,
    parse_ratio,
    round_half_up,
)

LOCATION_HELPER_TEXTS = {'רחוב/שכונה/תב"ע', 'רחוב/שכונה/תב״ע'}


# =============================================================================
# Local helpers (kept here to keep checks self-contained)
# =============================================================================
from pathlib import Path

def print_rule_kpi(rule_id: str, results: List[CheckResult], file_path: str | None = None) -> None:
    """
    Prints only:
      <file.xlsx> | <rule_id> | <fail>/<total> FAIL
    """
    file_name = Path(file_path).name if file_path else "-"

    if not results:
        print(f"{file_name} | {rule_id} | 0/0 FAIL")
        return

    row_idxs = [r.row_index for r in results if getattr(r, "row_index", None) is not None]
    if row_idxs:
        total = len(set(row_idxs))
        fails = len(set(
            r.row_index for r in results
            if getattr(r, "status", None) == Status.FAIL and getattr(r, "row_index", None) is not None
        ))
    else:
        total = len(results)
        fails = sum(1 for r in results if getattr(r, "status", None) == Status.FAIL)

    print(f"{file_name} | {rule_id} | {fails}/{total} FAIL")




def fmt_num(x, decimals=3):
    """Format numbers nicely (thousands separators, trim trailing zeros)."""
    try:
        v = float(x)
    except Exception:
        return str(x)
    s = f"{v:,.{decimals}f}"
    return s.rstrip("0").rstrip(".")



def get_cell(plan_df: pd.DataFrame, df_row_idx: int, col_idx: int) -> Any:
    """
    Safe cell getter (by integer positions).
    Returns None if out-of-bounds instead of raising IndexError.
    """
    if df_row_idx < 0 or df_row_idx >= len(plan_df):
        return None
    if col_idx < 0 or col_idx >= plan_df.shape[1]:
        return None
    return plan_df.iat[df_row_idx, col_idx]


def is_non_empty(value: Any) -> bool:
    return pd.notna(value) and str(value).strip() != ""


def fail_no_cities(rule_id: str, rule_name: str, cfg: PlanConfig) -> List[CheckResult]:
    return [
        CheckResult(
            rule_id=rule_id,
            rule_name=rule_name,
            severity=Severity.CRITICAL,
            sheet_name=cfg.sheet_name,
            row_index=None,
            column_name=None,
            key_context="city_cols_detection",
            actual_value=None,
            expected_value="at least 1 city column",
            status=Status.FAIL,
            message="No city columns detected (header row selection or heuristics mismatch).",
        )
    ]


import json

def load_kinun_store(json_path: str) -> dict:
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["utilities"]  # dict: {utility_name: {...}}

def lookup_kinun_value(kinun_store: dict, utility: str, col_name: str) -> float:
    """
    kinun_store: dict like {"הרי נצרת": {"water_full": 201993, ...}, ...}
    col_name: one of: water_full, water_reduced, sewer_full, sewer_reduced
    """
    u = str(utility).strip()
    if u not in kinun_store:
        raise KeyError(f"Utility not found in kinun JSON: {u}")

    val = kinun_store[u].get(col_name)
    if val is None:
        raise KeyError(f"Missing kinun value: utility={u}, col_name={col_name}")

    return float(val)



# =============================================================================
# Checks
# =============================================================================


def check_001_kinun_values_rounded(
    plan_df: pd.DataFrame,
    kinun_store,
    utility: str,
    cfg: PlanConfig,
) -> List[CheckResult]:
    """
    Compare plan kinun values (rounded) vs kinun reference (rounded).
    Plan values are taken from column R (value_col_r_idx) at fixed Excel rows.
    """
    mapping = {
        ("ערך כינון מלא", "מים"): "water_full",
        ("ערך כינון מלא", "ביוב"): "sewer_full",
        ("ערך כינון מופחת", "מים"): "water_reduced",
        ("ערך כינון מופחת", "ביוב"): "sewer_reduced",
    }


    results: List[CheckResult] = []

    for (label, system), excel_row in cfg.kinun_plan_rows_excel.items():
        df_idx = excel_row_to_df_index(excel_row, cfg)

        plan_raw = get_cell(plan_df, df_idx, cfg.value_col_r_idx)
        kinun_col = mapping[(label, system)]
        kinun_raw = lookup_kinun_value(kinun_store, utility, kinun_col)

        plan_round = round_half_up(plan_raw, 0)
        kinun_round = round_half_up(kinun_raw, 0)

        ok = plan_round == kinun_round

        msg_prefix = "התאמה לאחר עיגול" if ok else "אי־התאמה לאחר עיגול"

        results.append(
            CheckResult(
                rule_id=f"R_1_{label}_{system}",
                rule_name="בדיקת ערכי כינון (עיגול לפני השוואה)",
                severity=Severity.CRITICAL,
                sheet_name=cfg.sheet_name,
                row_index=df_idx,
                column_name="R",
                key_context=f"plan_cell=R{excel_row}; kinun_col={kinun_col}",
                actual_value=plan_round,
                expected_value=kinun_round,
                status=Status.PASS_ if ok else Status.FAIL,
                message=(
                    f"{msg_prefix}: תכנית R{excel_row} ‏{fmt_num(plan_round, 0)} "
                    f"(מקור: {fmt_num(plan_raw, 3)}), "
                    f"ערך כינון ‏{fmt_num(kinun_round, 0)} "
                    f"(מקור: {fmt_num(kinun_raw, 3)})"
                ),
            )
        )

    return results


# -----------------------------------------


def check_rule02_03_asset_ratio(plan_df: pd.DataFrame, cfg: PlanConfig) -> List[CheckResult]:
    """
    Asset ratio in column R should parse to a ratio and satisfy 0 < ratio < 1.
    Accepts '95%' or 0.95 or 95 (interpreted as 0.95).
    Rule02–03: merged asset disposal validation.
    """
    results: List[CheckResult] = []

    for system, excel_row in cfg.asset_ratio_rows_excel.items():
        df_idx = excel_row_to_df_index(excel_row, cfg)
        raw = get_cell(plan_df, df_idx, cfg.value_col_r_idx)
        ratio = parse_ratio(raw)

        if ratio is None:
            status = Status.FAIL
            message = (
                f"חריגה – לא ניתן לפענח יחס גריעת נכסים. "
                f"ערך מקורי: {raw!r}"
            )
            actual_value = raw
            expected_value = "0%–100%"
        else:
            ok = 0 < ratio < 1
            status = Status.PASS_ if ok else Status.FAIL
            ratio_pct = ratio * 100

            prefix = "תקין" if ok else "חריגה"
            message = (
                f"{prefix} – גריעת נכסים: {ratio_pct:.1f}% "
                f"(יחס={ratio:.3f}). "
                f"צפוי: בין 0% ל־100%."
            )
            actual_value = f"{ratio_pct:.1f}%"
            expected_value = "0%–100%"

        results.append(
            CheckResult(
                rule_id=f"R_2_3_{system}",
                rule_name="גריעת נכסים + פרטים",
                severity=Severity.CRITICAL,
                sheet_name=cfg.sheet_name,
                row_index=df_idx,
                column_name="R",
                key_context=f"plan_cell=R{excel_row}",
                actual_value=actual_value,
                expected_value=expected_value,
                status=status,
                message=message,
            )
        )

    return results




def check_004_total_program_values(plan_df: pd.DataFrame, cfg: PlanConfig) -> List[CheckResult]:
    """
    Reporting check: total program values in column R (rows 8/9/10 by system).
    FAIL if missing.
    """
    results: List[CheckResult] = []
    for system, excel_row in cfg.total_program_rows_excel.items():
        df_idx = excel_row_to_df_index(excel_row, cfg)
        val = get_cell(plan_df, df_idx, cfg.value_col_r_idx)

        results.append(
            CheckResult(
                rule_id=f"R_4_{system}",
                rule_name='סה"כ נתוני תכנית השקעה',
                severity=Severity.INFO,
                sheet_name=cfg.sheet_name,
                row_index=df_idx,
                column_name="R",
                key_context=f"plan_cell=R{excel_row}",
                actual_value=val,
                expected_value="reported value",
                status=Status.PASS_ if pd.notna(val) else Status.FAIL,
                message=f"Value from R{excel_row} = {val}",
            )
        )
    return results




def check_005_min_required_program(plan_df: pd.DataFrame, cfg: PlanConfig) -> List[CheckResult]:
    """
    Minimum required program values in column R. FAIL if missing.
    """
    results: List[CheckResult] = []

    for system, excel_row in cfg.min_required_program_rows_excel.items():
        df_idx = excel_row_to_df_index(excel_row, cfg)
        val = get_cell(plan_df, df_idx, cfg.value_col_r_idx)

        results.append(
            CheckResult(
                rule_id=f"R_005_{system}",
                rule_name="מינימום נדרש לתכנית השקעה",
                severity=Severity.CRITICAL,
                sheet_name=cfg.sheet_name,
                row_index=df_idx,
                column_name="R",
                key_context=f"plan_cell=R{excel_row}",
                actual_value=val,
                expected_value="reported value",
                status=Status.PASS_ if pd.notna(val) else Status.FAIL,
                message=f"Value from R{excel_row} = {val}",
            )
        )

    return results

def check_006_rehab_upgrade_min_required(plan_df: pd.DataFrame, cfg: PlanConfig) -> List[CheckResult]:
    """
    Minimum required rehab/upgrade values in column S. FAIL if missing.
    """
    results: List[CheckResult] = []

    for system, excel_row in cfg.rehab_upgrade_min_rows_excel.items():
        df_idx = excel_row_to_df_index(excel_row, cfg)
        val = get_cell(plan_df, df_idx, cfg.value_col_s_idx)

        results.append(
            CheckResult(
                rule_id=f"R_006_{system}",
                rule_name="מינימום נדרש לשיקום/שדרוג",
                severity=Severity.INFO,
                sheet_name=cfg.sheet_name,
                row_index=df_idx,
                column_name="S",
                key_context=f"plan_cell=S{excel_row}",
                actual_value=val,
                expected_value="reported value",
                status=Status.PASS_ if pd.notna(val) else Status.FAIL,
                message=f"Value from S{excel_row} = {val}",
            )
        )

    return results


def check_007_total_planned_investments_by_city(plan_df: pd.DataFrame, cfg: PlanConfig) -> List[CheckResult]:
    """
    City columns: check row planned_investments_row_excel has values per city.
    FAIL per city if blank.
    """
    city_cols = detect_city_cols(plan_df)
    if not city_cols:
        return fail_no_cities("R_007", 'סה"כ השקעות מתוכננות לביצוע', cfg)

    excel_row = cfg.planned_investments_row_excel
    df_idx = excel_row_to_df_index(excel_row, cfg)

    results: List[CheckResult] = []
    for city in city_cols:
        val = plan_df.at[df_idx, city] if df_idx in plan_df.index else None
        results.append(
            CheckResult(
                rule_id=f"R_007_{city}",
                rule_name='סה"כ השקעות מתוכננות לביצוע',
                severity=Severity.CRITICAL,
                sheet_name=cfg.sheet_name,
                row_index=df_idx,
                column_name=str(city),
                key_context=f"row={excel_row}; city={city}",
                actual_value=val,
                expected_value="reported value",
                status=Status.PASS_ if pd.notna(val) else Status.FAIL,
                message=f"Value from row {excel_row} for '{city}' = {val}",
            )
        )
    return results


def check_008_funding_total_and_exists_by_city(plan_df: pd.DataFrame, cfg: PlanConfig) -> List[CheckResult]:
    """
    City columns: check row funding_total_row_excel exists/non-empty per city.
    FAIL per city if empty.
    """
    city_cols = detect_city_cols(plan_df)
    if not city_cols:
        return fail_no_cities("R_008", 'מקורות מימון - סה"כ מקורות תקציב', cfg)

    excel_row = cfg.funding_total_row_excel
    df_idx = excel_row_to_df_index(excel_row, cfg)

    results: List[CheckResult] = []
    for city in city_cols:
        val = plan_df.at[df_idx, city] if df_idx in plan_df.index else None
        exists = is_non_empty(val)
        results.append(
            CheckResult(
                rule_id=f"R_008_{city}",
                rule_name='מקורות מימון - סה"כ מקורות תקציב (כולל בדיקת קיום)',
                severity=Severity.CRITICAL,
                sheet_name=cfg.sheet_name,
                row_index=df_idx,
                column_name=str(city),
                key_context=f"row={excel_row}; city={city}",
                actual_value=val,
                expected_value="non-empty",
                status=Status.PASS_ if exists else Status.FAIL,
                message=f"Funding total for '{city}' from row {excel_row} = {val}",
            )
        )
    return results


def check_010_pipes_any_value(plan_df: pd.DataFrame, cfg: PlanConfig) -> List[CheckResult]:
    """
    R_010: For each city, check that at least ONE of:
      - row 56 (water steel / PE)
      - row 57 (water PVC)
      - row 58 (sewer)
    has a value.
    """
    city_cols = detect_city_cols(plan_df)
    if not city_cols:
        return fail_no_cities("R_010", "דיווח אורכי צנרת (לפחות ערך אחד)", cfg)

    row_ws, row_wp = cfg.water_pipe_rows_excel
    row_sw = cfg.sewer_pipe_row_excel

    idx_ws = excel_row_to_df_index(row_ws, cfg)
    idx_wp = excel_row_to_df_index(row_wp, cfg)
    idx_sw = excel_row_to_df_index(row_sw, cfg)

    results: List[CheckResult] = []

    for city in city_cols:
        v_ws = plan_df.at[idx_ws, city] if idx_ws in plan_df.index else None
        v_wp = plan_df.at[idx_wp, city] if idx_wp in plan_df.index else None
        v_sw = plan_df.at[idx_sw, city] if idx_sw in plan_df.index else None

        ok = is_non_empty(v_ws) or is_non_empty(v_wp) or is_non_empty(v_sw)

        results.append(
            CheckResult(
                rule_id=f"R_010_{city}",
                rule_name="דיווח אורכי צנרת (לפחות ערך אחד מתוך 3 שורות)",
                severity=Severity.WARNING,
                sheet_name=cfg.sheet_name,
                row_index=None,
                column_name=str(city),
                key_context=f"rows={row_ws},{row_wp},{row_sw}; city={city}",
                actual_value={"water_row56": v_ws, "water_row57": v_wp, "sewer_row58": v_sw},
                expected_value="at least one non-empty among rows 56/57/58",
                status=Status.PASS_ if ok else Status.FAIL,
                message=f"row56={v_ws}, row57={v_wp}, row58={v_sw}",
            )
        )

    return results


def check_011_pipes_values_by_type(plan_df: pd.DataFrame, cfg: PlanConfig) -> List[CheckResult]:
    """
    R_011: Output pipe lengths by type (3 rows per city):
      - row 56: water steel / PE
      - row 57: water PVC
      - row 58: sewer
    """
    city_cols = detect_city_cols(plan_df)
    if not city_cols:
        return fail_no_cities("R_011", "דיווח אורכי צנרת (פירוט ערכים)", cfg)

    row_ws, row_wp = cfg.water_pipe_rows_excel
    row_sw = cfg.sewer_pipe_row_excel

    idx_ws = excel_row_to_df_index(row_ws, cfg)
    idx_wp = excel_row_to_df_index(row_wp, cfg)
    idx_sw = excel_row_to_df_index(row_sw, cfg)

    results: List[CheckResult] = []

    def emit(city: str, pipe_type: str, excel_row: int, df_idx: int, val: Any, suffix: str) -> None:
        results.append(
            CheckResult(
                rule_id=f"R_011_{suffix}_{city}",
                rule_name=f"דיווח אורכי צנרת - {pipe_type}",
                severity=Severity.INFO,
                sheet_name=cfg.sheet_name,
                row_index=df_idx,
                column_name=str(city),
                key_context=f"row={excel_row}; city={city}; type={pipe_type}",
                actual_value=val,
                expected_value="reported value",
                status=Status.PASS_,  # reporting
                message=f"Value from row {excel_row} for '{city}' ({pipe_type}) = {val}",
            )
        )

    for city in city_cols:
        v1 = plan_df.at[idx_ws, city] if idx_ws in plan_df.index else None
        v2 = plan_df.at[idx_wp, city] if idx_wp in plan_df.index else None
        v3 = plan_df.at[idx_sw, city] if idx_sw in plan_df.index else None

        emit(city, "מים פלדה (PE/פלדה)", row_ws, idx_ws, v1, "WATER_STEEL")
        emit(city, "מים PVC", row_wp, idx_wp, v2, "WATER_PVC")
        emit(city, "ביוב", row_sw, idx_sw, v3, "SEWER")

    return results


def check_012_project_fields_not_empty(
    report_df: pd.DataFrame,
    cfg: PlanConfig,
) -> List[CheckResult]:
    """
    rule12_projectName_Not_NA:
    In sheet "גיליון דיווח" ensure columns:
      - שם פרויקט
      - מיקום פרויקט
      - סיווג פרויקט
    are always filled for real data rows.

    Note: Excel has messy headers ("מיקום פרויקט " with trailing space, "סיווג\nפרויקט").
    We normalize column names to match robustly.
    """

    checked_cols_label = "שם פרויקט / מיקום פרויקט / סיווג פרויקט"

    rule_id = "R_12"
    rule_name = "שם פרויקט / מיקום פרויקט / סיווג פרויקט - חובה (Not NA)"

    def _norm_col(c: str) -> str:
        return re.sub(r"\s+", " ", str(c)).strip()

    # Map normalized -> original
    norm_to_orig = {_norm_col(c): c for c in report_df.columns}

    required_norm = ["שם פרויקט", "מיקום פרויקט", "סיווג פרויקט"]
    id_norm = getattr(cfg, "report_project_id_col_norm", "מס' פרויקט")

    missing_cols = [c for c in required_norm + [id_norm] if c not in norm_to_orig]
    if missing_cols:
        return [
            CheckResult(
                rule_id=rule_id,
                rule_name=rule_name,
                severity=Severity.CRITICAL,
                sheet_name=getattr(cfg, "report_sheet_name", "גיליון דיווח"),
                row_index=None,
                column_name=None,
                key_context="columns_presence",
                actual_value=list(report_df.columns),
                expected_value=f"columns include: {required_norm + [id_norm]}",
                status=Status.FAIL,
                message=f"Missing columns (after normalization): {missing_cols}",
            )
        ]

    id_col = norm_to_orig[id_norm]
    req_cols = [norm_to_orig[c] for c in required_norm]

    # Decide which rows are “real”
    # In your file: non-data rows have '-' in מס' פרויקט
    id_series = report_df[id_col].astype(str).str.strip()
    data_mask = report_df[id_col].notna() & (id_series != "-") & (id_series != "nan") & (id_series != "")

    df_data = report_df.loc[data_mask, [id_col] + req_cols].copy()

    def is_missing(v: Any) -> bool:
        if pd.isna(v):
            return True
        if isinstance(v, str) and v.strip() == "":
            return True
        return False

    miss = df_data[req_cols].map(is_missing)
    failed_mask = miss.any(axis=1)

    # Convert failures into CheckResult rows (one per failing row)
    results: List[CheckResult] = []

    # Excel row number: header_row=6 => header is Excel row 7.
    # DataFrame index 0 corresponds to Excel row 8.
    header_row = getattr(cfg, "report_header_row", 6)
    excel_row_offset = (header_row + 2)  # +1 for 1-based Excel, +1 because first data row is after header


    from openpyxl.utils import get_column_letter

    # Map original df columns -> Excel column letters (A,B,C,...)
    col_to_excel_letter = {
        col: get_column_letter(i + 1)
        for i, col in enumerate(list(report_df.columns))
    }

    for df_idx, row in df_data.loc[failed_mask].iterrows():
        missing_fields = [ _norm_col(col) for col, flag in miss.loc[df_idx].items() if flag ]
        excel_row = int(df_idx + excel_row_offset)
        excel_cells = []
        for col, flag in miss.loc[df_idx].items():
            if flag:
                # col here is original column name (like "מיקום פרויקט ")
                letter = col_to_excel_letter.get(col)
                if letter:
                    excel_cells.append(f"{getattr(cfg, 'report_sheet_name', 'גיליון דיווח')}!{letter}{excel_row}")
        
        results.append(
            CheckResult(
                rule_id=rule_id,
                rule_name=rule_name,
                severity=Severity.CRITICAL,
                sheet_name=getattr(cfg, "report_sheet_name", "גיליון דיווח"),
                row_index=df_idx,
                column_name=" / ".join(required_norm),
                key_context=f"{id_norm}={row[id_col]} | excel_row={excel_row} | checked_cols={checked_cols_label}",
                actual_value={_norm_col(c): row[c] for c in req_cols},
                expected_value="non-empty values",
                status=Status.FAIL,
                message=f"Missing fields: {missing_fields} (Excel row {excel_row})",
                excel_cells=excel_cells,   # ✅ NEW
            )
        )

    # If no failures, emit a single PASS result (so it shows nicely in summary)
    if not results:
        results.append(
            CheckResult(
                rule_id=rule_id,
                rule_name=rule_name,
                severity=Severity.INFO,
                sheet_name=getattr(cfg, "report_sheet_name", "גיליון דיווח"),
                row_index=None,
                column_name=" / ".join(required_norm),
                key_context=f"rows_checked={len(df_data)} | checked_cols={checked_cols_label}",
                actual_value=len(df_data),
                expected_value="all required fields filled",
                status=Status.PASS_,
                message="All required project fields are filled.",
            )
        )

    return results

# =============================================================================
# Rule 14: Funding Classification Validation (LLM/NLP)
# =============================================================================

# Fuzzy threshold constant
R14_FUZZY_THRESHOLD = 0.50  # Confidence >= 50% -> accept fuzzy decision


def canonicalize_label(s: str) -> str:
    """
    Canonicalize a funding label to one of the 5 allowed canonical forms.

    Canonical labels:
    - "שיקום/שדרוג"
    - "פיתוח"
    - "תשתית ביוב אזורית"
    - "קידוחים"
    - "תחזוקה / שוטף"
    """
    if not s:
        return ""

    # Normalize whitespace (including NBSP)
    s = s.replace("\u00A0", " ")
    s = re.sub(r"\s+", " ", s).strip()

    # --- תשתית ביוב אזורית variations (singular canonical) ---
    if "תשתיות ביוב אזוריות" in s:
        s = s.replace("תשתיות ביוב אזוריות", "תשתית ביוב אזורית")
    if "תשתיות ביוב אזורית" in s:
        s = s.replace("תשתיות ביוב אזורית", "תשתית ביוב אזורית")
    if "תשתית ביוב אזוריות" in s:
        s = s.replace("תשתית ביוב אזוריות", "תשתית ביוב אזורית")
    if "תשתית ביוב אזורי" in s and "תשתית ביוב אזורית" not in s:
        s = s.replace("תשתית ביוב אזורי", "תשתית ביוב אזורית")

    # --- קידוחים variations ---
    # Only replace standalone "קידוח" when it's likely to be the label
    if s == "קידוח":
        s = "קידוחים"

    # --- שיקום/שדרוג variations ---
    s = s.replace("שיקום ושדרוג", "שיקום/שדרוג")
    s = s.replace("שיקום ושידרוג", "שיקום/שדרוג")
    s = s.replace("שיקום / שדרוג", "שיקום/שדרוג")
    s = s.replace("שיקום/ שדרוג", "שיקום/שדרוג")
    s = s.replace("שיקום /שדרוג", "שיקום/שדרוג")

    # --- תחזוקה / שוטף variations (canonical has spaces around slash) ---
    # Match various spacing patterns around the slash
    s = re.sub(r"תחזוקה\s*/\s*שוטף", "תחזוקה / שוטף", s)
    s = s.replace("תחזוקה ושוטף", "תחזוקה / שוטף")

    return s


def _token_set(s: str) -> set[str]:
    """Split string into word tokens."""
    return set(s.split())


def _best_subject_match(project_name: str) -> tuple[str, float]:
    """
    Find the best matching subject from SUBJECT_TO_ALLOWED using fuzzy matching.

    Returns: (best_subject, score) where score is in [0..1]
    """
    pn = normalize_text(project_name)
    pt = _token_set(pn)

    best_subj = ""
    best_score = -1.0

    for subj in SUBJECT_TO_ALLOWED.keys():
        sn = normalize_text(subj)
        st = _token_set(sn)

        # Jaccard similarity on word tokens
        jacc = (len(pt & st) / len(pt | st)) if (pt or st) else 0.0
        # Sequence similarity
        seq = SequenceMatcher(None, pn, sn).ratio()
        # Combined score
        score = 0.55 * seq + 0.45 * jacc

        if score > best_score:
            best_score = score
            best_subj = subj

    return best_subj, float(best_score)


def check_014_llm_project_funding_classification(
    report_df: pd.DataFrame,
    cfg: PlanConfig,
) -> List[CheckResult]:
    """
    R14 - Funding Classification Validation (LLM/NLP)

    Validates that the reported funding classification (סיווג פרויקט) matches
    the predicted classification based on project name analysis.

    Pipeline order:
    1. Keyword stage (deterministic, conf=1.0)
    2. Fuzzy stage (synonym matching + fuzzy subject matching)
    3. LLM stage (fallback when keyword + fuzzy fail)
    """
    import os

    # TEMP DEBUG: Show config values at function entry
    print(f"[R_14 DEBUG] check_014 called with cfg.llm_enabled={getattr(cfg, 'llm_enabled', 'NOT SET')}, cfg.llm_provider={getattr(cfg, 'llm_provider', 'NOT SET')}")

    rule_id = "R_14"
    rule_name = "R14 - אימות מקור מימון לפי שם פרויקט (LLM/NLP)"
    sheet = getattr(cfg, "report_sheet_name", "גיליון דיווח")

    # ---------------------------
    # Helper functions
    # ---------------------------
    def _norm_col(x: object) -> str:
        return re.sub(r"\s+", " ", str(x)).strip()

    def _is_empty(v: object) -> bool:
        if v is None:
            return True
        try:
            if pd.isna(v):
                return True
        except Exception:
            pass
        if isinstance(v, str) and v.strip() == "":
            return True
        s = str(v).strip()
        return s == "" or s.lower() in {"nan", "none"}

    def _confidence_bucket(c: float) -> str:
        return "ביטחון גבוה" if c >= R14_FUZZY_THRESHOLD else "ביטחון נמוך"

    def _final_line(method: str, label: str, conf: float) -> str:
        """Build the method detail line (no subject)."""
        if method == "keyword":
            return f"סיווג סופי על ידי keyword: {label}"
        if method == "fuzzy":
            return f"סיווג סופי על ידי fuzzy ({_confidence_bucket(conf)}): {label} (conf={conf:.2f})"
        if method == "llm":
            return f"סיווג סופי על ידי LLM: {label} (conf={conf:.2f})"
        if method == "prior":
            return f"סיווג סופי על ידי prior (ביטחון נמוך): {label} (conf={conf:.2f})"
        # no-decision case
        return f"סיווג סופי על ידי LLM: לא זמין (conf=0.0)"

    def _predict_by_keyword(pn: str) -> tuple[str | None, float]:
        """
        Keyword-based prediction (Stage A).
        Returns: (label or None, confidence)

        IMPORTANT: Only return high-precision matches here.
        Generic terms like "חדש", "שדרוג" are handled in fuzzy stage as tie-breakers.
        """
        # === קידוחים/באר - highest priority, unambiguous ===
        if ("באר" in pn) or ("קידוח" in pn) or ("קידוחים" in pn) or ("רדיוס מגן" in pn):
            return "קידוחים", 1.0

        # === מט"ש / טיהור שפכים -> תשתית ביוב אזורית ===
        if ('מט"ש' in pn) or ("מטש" in pn) or ("מתקן טיהור" in pn) or ("טיהור שפכים" in pn):
            return "תשתית ביוב אזורית", 1.0

        # === מאסף אזורי / ראשי / חוצה -> תשתית ביוב אזורית ===
        if "מאסף" in pn and ("אזורי" in pn or "ראשי" in pn or "חוצה" in pn):
            return "תשתית ביוב אזורית", 1.0

        # === קו הולכת שפכים/ביוב אזורי -> תשתית ביוב אזורית ===
        if ("הולכת שפכים" in pn) or ("הולכת ביוב" in pn and "אזורי" in pn):
            return "תשתית ביוב אזורית", 1.0

        # === תחזוקה/שוטף - high precision maintenance terms ===
        maintenance_terms = (
            "צילום" in pn or "שטיפה" in pn or
            "סקר " in pn or pn.startswith("סקר") or  # סקר with space or at start (avoid partial)
            "מדידות" in pn or
            'קר"מ' in pn or "קרמ" in pn or "קריאה מרחוק" in pn or
            "GIS" in pn or "gis" in pn.lower() or
            "ניתור" in pn or "ניטור" in pn or
            "מפוחים" in pn or "מפוח" in pn or
            "מדי מים" in pn or "מוני מים" in pn or "החלפת מדים" in pn or
            "סייבר" in pn or "cyber" in pn.lower() or
            "תכנית אב" in pn or "תכניות אב" in pn or
            "הגנה קתודית" in pn or
            "ציוד חירום" in pn or "מחסן חירום" in pn or
            "לכידת אבנית" in pn or
            "הפרדת ביוב" in pn or
            "סריקות הנדסיות" in pn
        )
        if maintenance_terms:
            return "תחזוקה / שוטף", 1.0

        # === פיתוח - strong development zone indicators ===
        # These are almost always new development (פיתוח)
        pituach_strong = (
            ('אז"ת' in pn) or ("אזת" in pn) or
            ("אזור תעשי" in pn) or ("א.ת" in pn) or ("א.ת." in pn) or
            ("היי-טק" in pn) or ("הייטק" in pn) or
            ("מתחם חדש" in pn) or ("שכונה חדשה" in pn) or
            ("ותמ\"ל" in pn) or ('ותמ"ל' in pn) or ("ותמל" in pn) or
            ("פינוי בינוי" in pn) or ("תמא 38" in pn) or ("תמ\"א 38" in pn) or
            # הקמת patterns - "הקמת" + infrastructure = new development
            (pn.startswith("הקמת ") or "הקמת מערכ" in pn or "הקמת תחנ" in pn or
             "הקמת קו" in pn or "הקמת בריכ" in pn or "הקמת מאגר" in pn)
        )
        if pituach_strong:
            return "פיתוח", 0.90

        # === שיקום/שדרוג - strong rehabilitation indicators ===
        # Only return when very clear (action + infra combo)
        shikum_strong = (
            ("שדרוג מאסף" in pn) or ("שדרוג מאספים" in pn) or
            ("שיקום מאסף" in pn) or ("שיקום מאספים" in pn) or
            ("החלפת משאב" in pn) or ("החלפת משאבות" in pn) or
            ("החלפת לוח חשמל" in pn) or
            ("שיקום בריכ" in pn) or ("שיקום מגדל" in pn) or
            ("איטום בריכ" in pn) or ("ציפוי בריכ" in pn) or
            ("שיקום תחנ" in pn) or ("שדרוג תחנ" in pn) or
            ("שיקום מערכ" in pn) or ("שדרוג מערכ" in pn) or
            ("החלפת צנרת" in pn) or ("שיקום צנרת" in pn) or
            ("שיקום קו" in pn) or ("שדרוג קו" in pn)
        )
        if shikum_strong:
            return "שיקום/שדרוג", 0.90

        # === DO NOT return generic שיקום/שדרוג or פיתוח here ===
        # Those are handled by fuzzy stage with subject-aware tie-breaking
        return None, 0.0

    def _subject_from_synonyms_normalized(pn_normalized: str) -> str | None:
        """
        Deterministic synonym matching using normalized text.
        Both the project name and synonym keys are compared in normalized form.
        """
        for key, subject in SUBJECT_SYNONYMS.items():
            key_norm = normalize_text(key)
            if key_norm and (key_norm in pn_normalized):
                return subject
        return None

    def _apply_tiebreaker(pn: str, allowed_set: set[str]) -> str | None:
        """
        Apply keyword-based tie-breaker when allowed_set has exactly 2 options.
        Returns label if tie-breaker succeeds, None otherwise.
        """
        if allowed_set != {"שיקום/שדרוג", "פיתוח"}:
            return None  # Only apply tie-breaker for this specific pair

        # שיקום/שדרוג indicators (strong)
        shikum_terms = (
            "החלפה" in pn or "החלפת" in pn or
            "שיקום" in pn or "שדרוג" in pn or
            "חידוש" in pn or "חידושי" in pn or
            "שיפוץ" in pn or "תיקון" in pn or
            "איטום" in pn or "ציפוי" in pn or
            "שיפור" in pn or "חיזוק" in pn or
            "ישן" in pn or "קיימ" in pn  # "existing" indicators
        )

        # פיתוח indicators (strong)
        pituach_terms = (
            "הקמה" in pn or "הקמת" in pn or
            "חדש" in pn or "חדשה" in pn or "חדשים" in pn or
            "תוספת" in pn or "תוספות" in pn or
            "הרחבה" in pn or "הרחבת" in pn or
            "השלמת" in pn or "השלמה" in pn or  # Completing = new work
            "שלב ב" in pn or "שלב ג" in pn or "שלב 2" in pn or "שלב 3" in pn or
            "ותמל" in pn or 'ותמ"ל' in pn or "ותמ\"ל" in pn or
            "שכונה חדשה" in pn or "מתחם חדש" in pn or
            "פינוי בינוי" in pn or "תמא 38" in pn or
            "מתחם א" in pn or "מתחם ב" in pn  # Development phases
        )

        if shikum_terms and not pituach_terms:
            return "שיקום/שדרוג"
        if pituach_terms and not shikum_terms:
            return "פיתוח"
        # Both or neither - can't decide
        return None

    def _predict_by_fuzzy(pn: str, project_name: str) -> tuple[str | None, float, set[str] | None]:
        """
        Fuzzy/synonym-based prediction (Stage B).
        Returns: (label or None, confidence, candidate_allowed_set for LLM fallback)

        Logic:
        1. Try synonym matching to find subject
        2. Look up allowed_set for subject
        3. If single label -> return it
        4. If 2 labels (שיקום/שדרוג, פיתוח) -> apply tie-breaker
        5. If tie-breaker fails -> return allowed_set for LLM
        """
        pn_normalized = normalize_text(project_name)

        # ========================================
        # Step 1: Try synonym substring matching
        # ========================================
        subj = _subject_from_synonyms_normalized(pn_normalized)
        if subj:
            # Direct label match (e.g., קידוחים)
            if subj in ALLOWED_FUNDING_LABELS:
                return canonicalize_label(subj), 0.90, None

            # Look up allowed set for this subject
            allowed_set = SUBJECT_TO_ALLOWED.get(subj, set())

            if allowed_set and len(allowed_set) == 1:
                # Single option - deterministic
                return canonicalize_label(next(iter(allowed_set))), 0.90, None

            if allowed_set and len(allowed_set) == 2:
                # Try tie-breaker
                result = _apply_tiebreaker(pn, allowed_set)
                if result:
                    return result, 0.85, None
                # Tie-breaker failed - pass to LLM with narrowed set
                return None, 0.50, allowed_set

        # ========================================
        # Step 2: No synonym match - try fuzzy subject matching
        # ========================================
        subj, score = _best_subject_match(project_name)
        if not subj:
            # No subject found at all - try to classify by keywords alone
            # Default to קווים קצרים which allows {שיקום/שדרוג, פיתוח}
            default_allowed = {"שיקום/שדרוג", "פיתוח"}
            result = _apply_tiebreaker(pn, default_allowed)
            if result:
                return result, 0.60, None
            return None, 0.0, default_allowed

        allowed_set = SUBJECT_TO_ALLOWED.get(subj, set())
        if not allowed_set:
            return None, 0.0, None

        # ========================================
        # Step 3: Apply fuzzy threshold
        # ========================================
        if score >= R14_FUZZY_THRESHOLD:
            if len(allowed_set) == 1:
                return canonicalize_label(next(iter(allowed_set))), score, None

            # Multiple options - try tie-breaker
            result = _apply_tiebreaker(pn, allowed_set)
            if result:
                return result, score, None
            # Tie-breaker failed - pass to LLM
            return None, score, allowed_set

        # Below threshold - still try tie-breaker with lower confidence
        if len(allowed_set) == 2:
            result = _apply_tiebreaker(pn, allowed_set)
            if result:
                return result, 0.55, None
            return None, score, allowed_set

        return None, score, allowed_set

    def _predict_by_llm(project_name: str, allowed_set: set[str] | None) -> tuple[str | None, float]:
        """
        LLM-based prediction (Stage C).
        Returns: (label or None, confidence)

        IMPORTANT: Only call LLM when allowed_set == {שיקום/שדרוג, פיתוח}
        For other cases, deterministic classification should have succeeded.
        """
        # Only call LLM for the specific 2-way ambiguity case
        target_set = {"שיקום/שדרוג", "פיתוח"}
        if allowed_set != target_set:
            print(f"[R_14 DEBUG] LLM SKIPPED: allowed_set={allowed_set} != target {target_set}")
            return None, 0.0

        llm_enabled = bool(getattr(cfg, "llm_enabled", False))
        provider = getattr(cfg, "llm_provider", "gemini")
        provider_l = (provider or "").strip().lower()

        if provider_l == "gemini":
            has_key = bool(os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY"))
            default_model = "gemini-1.5-flash"
        else:
            has_key = bool(os.getenv("OPENAI_API_KEY"))
            default_model = "gpt-4o-mini"

        # TEMP DEBUG: Show why LLM is skipped or called
        print(f"[R_14 DEBUG] _predict_by_llm called: project={project_name[:30]!r}, llm_enabled={llm_enabled}, has_key={has_key}, provider={provider_l}")

        if not (llm_enabled and has_key):
            print(f"[R_14 DEBUG] LLM SKIPPED: llm_enabled={llm_enabled}, has_key={has_key}")
            return None, 0.0

        try:
            from .llm_client import classify_funding_with_confidence, LLMQuotaError

            model = getattr(cfg, "llm_model", default_model)

            # LLM only chooses between שיקום/שדרוג and פיתוח
            prompt = build_llm_prompt(project_name, allowed_set=target_set)
            print(f"[R_14 DEBUG] Calling LLM with model={model}, allowed_set={target_set}")
            llm_label, llm_conf = classify_funding_with_confidence(
                prompt, provider=provider_l, model=model
            )
            print(f"[R_14 DEBUG] LLM returned: label={llm_label!r}, conf={llm_conf}")

            return canonicalize_label(llm_label), float(llm_conf)

        except LLMQuotaError:
            # Quota exceeded - graceful fallback
            return None, 0.0
        except Exception as e:
            print(f"[R_14] LLM error (provider={provider_l}): {e!r}")
            return None, 0.0

    # ---------------------------
    # Column resolution
    # ---------------------------
    col_map = {_norm_col(c): c for c in report_df.columns}
    project_col = col_map.get("שם פרויקט")
    class_col = col_map.get("סיווג פרויקט")

    missing = []
    if project_col is None:
        missing.append("שם פרויקט")
    if class_col is None:
        missing.append("סיווג פרויקט")

    if missing:
        return [
            CheckResult(
                rule_id=rule_id,
                rule_name=rule_name,
                severity=Severity.CRITICAL,
                sheet_name=sheet,
                row_index=None,
                column_name=None,
                key_context="columns_presence",
                actual_value=[_norm_col(c) for c in report_df.columns],
                expected_value=f"must include {sorted(['שם פרויקט', 'סיווג פרויקט'])}",
                status=Status.FAIL,
                message=f"חסרות עמודות (אחרי נירמול כותרות): {missing}",
            )
        ]

    # Excel cell mapping for row/column tracking
    from openpyxl.utils import get_column_letter as _get_col_letter
    _col_to_letter = {col: _get_col_letter(i + 1) for i, col in enumerate(report_df.columns)}
    _header_row = getattr(cfg, "report_header_row", 6)
    _excel_row_offset = _header_row + 2  # same convention as R12

    def _r14_cell_ref(df_i: int, col_name: str) -> Optional[str]:
        letter = _col_to_letter.get(col_name)
        if not letter:
            return None
        return f"{sheet}!{letter}{int(df_i + _excel_row_offset)}"

    # Row filtering setup
    id_norm = getattr(cfg, "report_project_id_col_norm", "מס' פרויקט")
    id_col = col_map.get(id_norm)

    def _is_real_row(idx: int) -> bool:
        if not id_col:
            return True
        v = report_df.at[idx, id_col]
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return False
        s = str(v).strip()
        if s in {"-", "", "nan", "None", "none"}:
            return False
        return True

    # Config flag for שיקום/שדרוג special treatment
    shikum_not_investment = bool(getattr(cfg, "r14_shikum_not_investment", False))

    # ---------------------------
    # Stage B3: Prior fallback from reported distribution
    # ---------------------------
    # Templates for bucketing generic project names (ordered by priority)
    PRIOR_TEMPLATES = [
        # Core infra patterns
        ("קווי מים", "קווי מים"),
        ("קו מים", "קו מים"),
        ("קווי ביוב", "קווי ביוב"),
        ("קו ביוב", "קו ביוב"),
        ("מערכת מים", "מערכת מים"),
        ("מערכת ביוב", "מערכת ביוב"),
        ("מאסף ביוב", "מאסף ביוב"),
        ("מאסף", "מאסף"),
        ("קו אספקה", "קו אספקה"),
        ("אספקת מים", "אספקת מים"),
        # Facilities
        ("תחנת שאיבה", "תחנת שאיבה"),
        ("תחנת בוסטרים", "בוסטרים"),
        ("בוסטר", "בוסטרים"),
        ("תחנת שאיבת מים", "תחנת שאיבת מים"),
        ("תחנת שאיבה", "תחנת שאיבה"),
        ("מאגר", "מאגר"),
        ("מערכת בקרה", "בקרה"),
        ("פיקוד ובקרה", "בקרה"),
        ("מיגון", "בקרה"),
        ("מגוף", "מגוף"),
        # Development zones (tend toward פיתוח)
        ('אז"ת', "אזור תעשייה"),
        ("אזור תעשייה", "אזור תעשייה"),
        ("א.ת", "אזור תעשייה"),
        ("היי-טק", "היי-טק"),
        ("הייטק", "היי-טק"),
        ("מתחם", "מתחם"),
        # Generic location patterns (most common, order matters - specific first)
        ("בשכונת", "שכונה"),  # More specific first
        ("בשכונה", "שכונה"),
        ("שכונת", "שכונה"),
        ("שכונה", "שכונה"),
        ("באזורים שונים", "אזורים"),
        ("במוקדים שונים", "אזורים"),
        ("אזורים מפוזרים", "אזורים"),
        ("אזורים שונים", "אזורים"),
        ("באזור", "אזורים"),
        # Road/highway patterns
        ("בכביש", "כביש"),
        ("כביש", "כביש"),
        ("ציר", "ציר"),
        ("ברחוב", "רחוב"),
        # Municipal patterns
        ("גרעין הישוב", "גרעין"),
        ("מרכז הישוב", "מרכז"),
        ("במרכז הישוב", "מרכז"),
        ("הסעת המונים", "הסעת המונים"),
        # Direction patterns
        ("צפוני", "כיוון"),
        ("דרומי", "כיוון"),
        ("מזרחי", "כיוון"),
        ("מערבי", "כיוון"),
        ("לכיוון", "כיוון"),
    ]

    PRIOR_MIN_SUPPORT = 5   # Minimum rows to learn prior (lowered from 8)
    PRIOR_MIN_SKEW = 0.67   # Minimum ratio for strong prior (lowered from 0.75)

    def _match_template(pn_norm: str) -> str | None:
        """Match project name to first matching template."""
        for pattern, template_name in PRIOR_TEMPLATES:
            if pattern in pn_norm:
                return template_name
        return None

    def _has_infra_or_action_token(pn_norm: str) -> bool:
        """
        Check if name has infrastructure or action tokens (not just location).
        This prevents classifying names like "שכונה צפונית" as location-only
        when they clearly have infra/action terms.
        """
        # Infrastructure tokens
        infra_tokens = [
            "קו", "קווי", "מערכת", "מים", "ביוב", "מאסף", "מאספים",
            "תחנה", "מאגר", "בריכ", "משאב", "סניקה", "בוסטר", "בוסטרים",
            'אז"ת', "אזור תעשי", "היי-טק", "הייטק", "מתחם",
            "מגוף", "מגופים", "מגוף מכני",
            'תש"ב', "תשב",  # תחנת שאיבה abbreviation
            "ותמ\"ל", 'ותמ"ל', "ותמל",  # Development projects
            "צנרת", "רשת",
        ]
        # Action tokens (שיקום/שדרוג or פיתוח indicators)
        action_tokens = [
            "שדרוג", "שיקום", "החלפ", "חידוש", "שיפוץ", "תיקון",
            "איטום", "ציפוי",  # Rehabilitation
            "הקמ", "חדש", "תוספ", "הרחב", "בנייה", "בניה",  # Development
        ]
        all_tokens = infra_tokens + action_tokens
        return any(t in pn_norm for t in all_tokens)

    # Compute priors from reported values (once per run)
    template_counts: dict[str, dict[str, int]] = {}  # template -> {label: count}
    target_labels = {"שיקום/שדרוג", "פיתוח"}

    for idx, row in report_df.iterrows():
        if not _is_real_row(idx):
            continue
        pn_raw = str(row.get(project_col, "")).strip()
        reported_raw = str(row.get(class_col, "")).strip()
        if not pn_raw or pn_raw.lower() == "nan":
            continue
        reported_label = canonicalize_label(reported_raw)
        if reported_label not in target_labels:
            continue
        pn_norm = normalize_text(pn_raw)
        template = _match_template(pn_norm)
        if template:
            if template not in template_counts:
                template_counts[template] = {"שיקום/שדרוג": 0, "פיתוח": 0}
            template_counts[template][reported_label] += 1

    def _get_prior(pn_norm: str) -> tuple[str | None, float]:
        """
        Get prior prediction based on template distribution.
        Returns (label, conf) or (None, 0).

        Uses tiered confidence:
        - Strong skew (>=67%) + good support (>=5): conf=0.60
        - Moderate skew (>=55%) + some support (>=3): conf=0.50
        - Weak majority (>50%) + minimal support (>=2): conf=0.45
        """
        template = _match_template(pn_norm)
        if not template or template not in template_counts:
            return None, 0.0
        counts = template_counts[template]
        total = counts["שיקום/שדרוג"] + counts["פיתוח"]
        if total < 2:  # Need at least 2 examples
            return None, 0.0

        winner = "שיקום/שדרוג" if counts["שיקום/שדרוג"] >= counts["פיתוח"] else "פיתוח"
        ratio = counts[winner] / total

        # Tiered confidence based on support and skew
        if total >= PRIOR_MIN_SUPPORT and ratio >= PRIOR_MIN_SKEW:
            return winner, 0.60  # Strong prior
        if total >= 3 and ratio >= 0.55:
            return winner, 0.50  # Moderate prior
        if total >= 2 and ratio > 0.50:
            return winner, 0.45  # Weak prior
        # Even split - can't decide
        return None, 0.0

    # ---------------------------
    # Main loop
    # ---------------------------
    results: List[CheckResult] = []

    for idx, row in report_df.iterrows():
        if not _is_real_row(idx):
            continue

        project_name = str(row.get(project_col, "")).strip()
        if not project_name or project_name.lower() == "nan":
            continue

        reported_raw = str(row.get(class_col, "")).strip()

        # --- Check 1: Missing reported value ---
        if _is_empty(reported_raw):
            msg = (
                "נכשל - ערך חסר בעמודת סיווג פרויקט\n"
                "סיווג סופי על ידי keyword: (אין)"
            )
            results.append(
                CheckResult(
                    rule_id=rule_id,
                    rule_name=rule_name,
                    severity=Severity.WARNING,
                    sheet_name=sheet,
                    row_index=idx,
                    column_name="סיווג פרויקט",
                    status=Status.FAIL,
                    message=msg,
                    actual_value="(ריק)",
                    expected_value=" | ".join(sorted(ALLOWED_FUNDING_LABELS)),
                    key_context=f"project_name={project_name}",
                )
            )
            continue

        reported = canonicalize_label(reported_raw)

        # --- Check 2: Illegal reported value ---
        if reported and (reported not in ALLOWED_FUNDING_LABELS):
            msg = (
                f"נכשל - ערך לא חוקי: '{reported_raw}'\n"
                "סיווג סופי על ידי keyword: (אין)"
            )
            results.append(
                CheckResult(
                    rule_id=rule_id,
                    rule_name=rule_name,
                    severity=Severity.WARNING,
                    sheet_name=sheet,
                    row_index=idx,
                    column_name="סיווג פרויקט",
                    status=Status.FAIL,
                    message=msg,
                    actual_value=reported_raw,
                    expected_value=" | ".join(sorted(ALLOWED_FUNDING_LABELS)),
                    key_context=f"project_name={project_name}",
                )
            )
            continue

        pn = normalize_text(project_name)

        # --- Pipeline: Predict label ---
        predicted = None
        method = None
        confidence = 0.0
        candidate_allowed_set = None
        llm_was_attempted = False  # Track if LLM was actually called

        # Stage A: Keyword
        kw_label, kw_conf = _predict_by_keyword(pn)
        # TEMP DEBUG
        print(f"[R_14 DEBUG] Row {idx}: project={project_name[:40]!r}")
        print(f"[R_14 DEBUG]   Keyword stage: label={kw_label!r}, conf={kw_conf}")
        if kw_label is not None:
            predicted = canonicalize_label(kw_label)
            method = "keyword"
            confidence = kw_conf

        # Stage B: Fuzzy (only if keyword didn't decide)
        if predicted is None:
            fuzzy_label, fuzzy_conf, fuzzy_allowed = _predict_by_fuzzy(pn, project_name)
            # TEMP DEBUG
            print(f"[R_14 DEBUG]   Fuzzy stage: label={fuzzy_label!r}, conf={fuzzy_conf}, allowed={fuzzy_allowed}")
            if fuzzy_label is not None:
                predicted = canonicalize_label(fuzzy_label)
                method = "fuzzy"
                confidence = fuzzy_conf
            else:
                candidate_allowed_set = fuzzy_allowed

        # Stage C: LLM (only if keyword + fuzzy failed)
        if predicted is None:
            print(f"[R_14 DEBUG]   -> Entering LLM stage (keyword+fuzzy failed)")
            # Check if LLM will actually be called (allowed_set matches and keys present)
            target_set = {"שיקום/שדרוג", "פיתוח"}
            if candidate_allowed_set == target_set:
                llm_was_attempted = True
            llm_label, llm_conf = _predict_by_llm(project_name, candidate_allowed_set)
            if llm_label is not None:
                predicted = canonicalize_label(llm_label)
                method = "llm"
                confidence = llm_conf
        else:
            print(f"[R_14 DEBUG]   -> LLM SKIPPED (predicted already set by {method})")

        # Stage D: Prior fallback
        # Run when:
        # 1. Still no prediction AND
        # 2. Either allowed_set is the 2-way ambiguity OR name matches a strong template
        if predicted is None:
            use_prior = False
            # Case 1: Explicit 2-way ambiguity from fuzzy stage
            if candidate_allowed_set == {"שיקום/שדרוג", "פיתוח"}:
                use_prior = True
            # Case 2: Subject matching failed but name matches a strong infra template
            # (templates like מערכת/קווי/מאסף + מים/ביוב imply 2-way ambiguity)
            elif candidate_allowed_set is None or len(candidate_allowed_set or set()) == 0:
                template = _match_template(pn)
                if template is not None:
                    use_prior = True
                    print(f"[R_14 DEBUG]   -> Template matched ({template}), enabling prior fallback")

            if use_prior:
                prior_label, prior_conf = _get_prior(pn)
                if prior_label is not None:
                    print(f"[R_14 DEBUG]   -> Prior fallback: label={prior_label!r}, conf={prior_conf}")
                    predicted = prior_label
                    method = "prior"
                    confidence = prior_conf

        # Stage E: Global fallback - use overall file distribution for 2-way ambiguity
        # Only when we have clear infra pattern but no template-specific prior
        if predicted is None and candidate_allowed_set == {"שיקום/שדרוג", "פיתוח"}:
            # Check if name has clear infra pattern
            has_clear_infra = any(t in pn for t in ["מערכת", "קווי", "קו ", "מאסף", "תחנ", "מאגר"])
            if has_clear_infra:
                # Use global distribution from template_counts
                global_shikum = sum(c.get("שיקום/שדרוג", 0) for c in template_counts.values())
                global_pituach = sum(c.get("פיתוח", 0) for c in template_counts.values())
                total = global_shikum + global_pituach
                if total >= 10:  # Need reasonable sample
                    dominant = "שיקום/שדרוג" if global_shikum >= global_pituach else "פיתוח"
                    ratio = max(global_shikum, global_pituach) / total
                    if ratio >= 0.55:  # At least 55% skew
                        print(f"[R_14 DEBUG]   -> Global fallback: {dominant} ({ratio:.0%} of {total})")
                        predicted = dominant
                        method = "prior"
                        confidence = 0.40  # Very low confidence

        # --- No decision (keyword/fuzzy/LLM/prior all failed or unavailable) ---
        # Show the row with INFO status instead of skipping
        if predicted is None:
            # Check if this is a location-only name (no infra/action tokens)
            if not _has_infra_or_action_token(pn):
                print(f"[R_14 DEBUG]   -> No prediction: location-only name")
                msg = (
                    "לא ניתן לסווג - שם פרויקט כללי/מיקום בלבד (חסר סוג תשתית/פעולה)\n"
                    "סיווג סופי: לא זמין"
                )
            else:
                print(f"[R_14 DEBUG]   -> No prediction available, showing as INFO")
                msg = (
                    "לא ניתן לסווג - keyword/fuzzy/LLM לא החזירו תוצאה\n"
                    "סיווג סופי: לא זמין"
                )
            # Determine method for "רמת בדיקה" column
            no_decision_method = "fail_llm" if llm_was_attempted else "no_decision"
            results.append(
                CheckResult(
                    rule_id=rule_id,
                    rule_name=rule_name,
                    severity=Severity.INFO,
                    sheet_name=sheet,
                    row_index=idx,
                    column_name="סיווג פרויקט",
                    status=Status.INFO,
                    message=msg,
                    actual_value=reported_raw,
                    expected_value="(לא ניתן לחזות)",
                    key_context=f"project_name={project_name}",
                    confidence=0.0,
                    method=no_decision_method,
                )
            )
            continue

        # Ensure canonical forms
        predicted = canonicalize_label(predicted)
        reported = canonicalize_label(reported)

        # --- Hard rule: תחזוקה / שוטף is NOT investment ---
        if reported == "תחזוקה / שוטף":
            msg = (
                "נכשל - לפי ערך בפועל: אינו שייך לתוכנית השקעה\n"
                + _final_line(method, predicted, confidence)
            )
            results.append(
                CheckResult(
                    rule_id=rule_id,
                    rule_name=rule_name,
                    severity=Severity.WARNING,
                    sheet_name=sheet,
                    row_index=idx,
                    column_name="סיווג פרויקט",
                    status=Status.FAIL,
                    message=msg,
                    actual_value=reported_raw,
                    expected_value="(לא תחזוקה / שוטף)",
                    key_context=f"project_name={project_name}",
                    confidence=confidence,
                    method=method,
                )
            )
            continue

        if predicted == "תחזוקה / שוטף":
            msg = (
                "נכשל - לפי ערך צפוי: אינו שייך לתוכנית השקעה\n"
                + _final_line(method, predicted, confidence)
            )
            results.append(
                CheckResult(
                    rule_id=rule_id,
                    rule_name=rule_name,
                    severity=Severity.WARNING,
                    sheet_name=sheet,
                    row_index=idx,
                    column_name="סיווג פרויקט",
                    status=Status.FAIL,
                    message=msg,
                    actual_value=reported_raw,
                    expected_value="(לא תחזוקה / שוטף)",
                    key_context=f"project_name={project_name}",
                    confidence=confidence,
                    method=method,
                )
            )
            continue

        # --- Optional: שיקום/שדרוג NOT investment mode ---
        if shikum_not_investment and reported == "שיקום/שדרוג" and predicted == "שיקום/שדרוג":
            msg = (
                "נכשל - לפי דרישת לקוח: שיקום/שדרוג אינו שייך לתוכנית השקעה\n"
                + _final_line(method, predicted, confidence)
            )
            results.append(
                CheckResult(
                    rule_id=rule_id,
                    rule_name=rule_name,
                    severity=Severity.WARNING,
                    sheet_name=sheet,
                    row_index=idx,
                    column_name="סיווג פרויקט",
                    status=Status.FAIL,
                    message=msg,
                    actual_value=reported_raw,
                    expected_value="(לא שיקום/שדרוג)",
                    key_context=f"project_name={project_name}",
                    confidence=confidence,
                    method=method,
                )
            )
            continue

        # --- Compare reported vs predicted ---
        LOW_CONF_THRESHOLD = 0.70  # Below this = low confidence (gray/INFO)

        # Special note for prior-based predictions
        prior_note = ""
        if method == "prior":
            prior_note = "\n(סיווג בוצע לפי התפלגות ערכים מדווחים בתאגיד עבור תבנית שם דומה. ייתכן שהשם כללי מדי.)"

        if reported == predicted:
            # Check if low confidence match
            if confidence < LOW_CONF_THRESHOLD and method != "keyword":
                # Low confidence PASS - show as PASS with INFO severity (gray)
                msg = f"עבר (ביטחון נמוך): התאמה\n{_final_line(method, predicted, confidence)}{prior_note}"
                results.append(
                    CheckResult(
                        rule_id=rule_id,
                        rule_name=rule_name,
                        severity=Severity.INFO,
                        sheet_name=sheet,
                        row_index=idx,
                        column_name="סיווג פרויקט",
                        status=Status.PASS_,  # Show עבר but with gray styling
                        message=msg,
                        actual_value=reported_raw,
                        expected_value=predicted,
                        key_context=f"project_name={project_name}",
                        confidence=confidence,
                        method=method,
                    )
                )
            else:
                # High confidence PASS (prior always has low conf, so won't reach here)
                msg = f"עבר: התאמה\n{_final_line(method, predicted, confidence)}{prior_note}"
                results.append(
                    CheckResult(
                        rule_id=rule_id,
                        rule_name=rule_name,
                        severity=Severity.INFO,
                        sheet_name=sheet,
                        row_index=idx,
                        column_name="סיווג פרויקט",
                        status=Status.PASS_,
                        message=msg,
                        actual_value=reported_raw,
                        expected_value=predicted,
                        key_context=f"project_name={project_name}",
                        confidence=confidence,
                        method=method,
                    )
                )
        else:
            # FAIL - mismatch
            if confidence < LOW_CONF_THRESHOLD and method != "keyword":
                # Low confidence FAIL - show as FAIL with INFO severity (gray)
                msg = (
                    "אי-התאמה (ביטחון נמוך)\n"
                    + _final_line(method, predicted, confidence)
                    + prior_note
                )
                results.append(
                    CheckResult(
                        rule_id=rule_id,
                        rule_name=rule_name,
                        severity=Severity.INFO,
                        sheet_name=sheet,
                        row_index=idx,
                        column_name="סיווג פרויקט",
                        status=Status.FAIL,  # Show נכשל but with gray styling
                        message=msg,
                        actual_value=reported_raw,
                        expected_value=predicted,
                        key_context=f"project_name={project_name}",
                        confidence=confidence,
                        method=method,
                    )
                )
            else:
                # High confidence FAIL (prior always has low conf, so won't reach here)
                msg = (
                    "נכשל - סיווג לא תואם\n"
                    + _final_line(method, predicted, confidence)
                    + prior_note
                )
                results.append(
                    CheckResult(
                        rule_id=rule_id,
                        rule_name=rule_name,
                        severity=Severity.WARNING,
                        sheet_name=sheet,
                        row_index=idx,
                        column_name="סיווג פרויקט",
                        status=Status.FAIL,
                        message=msg,
                        actual_value=reported_raw,
                        expected_value=predicted,
                        key_context=f"project_name={project_name}",
                        confidence=confidence,
                        method=method,
                    )
                )

    # Post-process: add excel_cells to all R14 results for row/column tracking
    for r in results:
        if r.row_index is not None and r.excel_cells is None:
            ref = _r14_cell_ref(r.row_index, class_col)
            if ref:
                r.excel_cells = [ref]

    return results



INVALID_PROJECT_TEXTS = {
    "רחוב",
    "בין הבתים",
    "שטח פתוח",
    "רחוב שכונת",
    "רחוב שכונה",   # practical variant
}

def check_015_invalid_project_names(
    report_df: pd.DataFrame,
    cfg: PlanConfig,
) -> List[CheckResult]:
    """
    R15 - "שמות פרויקטים לא תקינים"

    Input:
      - report_df: DataFrame של "גיליון דיווח"
      - cfg: PlanConfig (כולל report_project_id_col_norm, report_header_row וכו')

    What it checks:
      - בעמודות "שם פרויקט" ו-"מיקום פרויקט" אין ערכים כלליים מדי כמו:
        "רחוב", "בין הבתים", "שטח פתוח", "רחוב שכונת"
      - בעמודה "מיקום פרויקט" יש תא-עזר שמופיע תחת הכותרת בגלל header דו-שורתי:
        "רחוב/שכונה/תב\"ע" -> את זה חייבים לדלג (זה לא נתון אמיתי)

    Output:
      - List[CheckResult] (רק FAILים; בלי PASS כדי לא לנפח Output)
    """



    def _norm_col(col_name: object) -> str:
        """
        Normalize Excel column headers for robust matching.
        Handles trailing spaces, newlines, NBSP, repeated whitespace,
        and ignores helper header text like 'רחוב/שכונה/תב"ע'.
        Also removes pandas duplicate suffix like ".1".
        """
        s = "" if col_name is None else str(col_name)

        # normalize whitespace-ish characters
        s = s.replace("\u00A0", " ")   # NBSP -> space
        s = s.replace("\n", " ").replace("\r", " ").replace("\t", " ")

        # ignore helper header fragment that appears due to merged cells
        s = s.replace('רחוב/שכונה/תב"ע', "")
        s = s.replace("רחוב/שכונה/תב״ע", "")

        # collapse whitespace
        s = " ".join(s.split()).strip()

        # remove pandas duplicate suffix: ".1", ".2", ...
        s = re.sub(r"\.\d+$", "", s).strip()
        return s


    RULE_ID = "R_15"
    RULE_NAME = "שמות פרויקטים לא תקינים"
    SHEET = getattr(cfg, "report_sheet_name", "גיליון דיווח")
    EMIT_PASS_ROWS = True  # debug mode: include PASS rows too

    # ---- Patterns ----
    INVALID_PROJECT_TEXTS = {
        normalize_text("רחוב"),
        normalize_text("בין הבתים"),
        normalize_text("שטח פתוח"),
        normalize_text("רחוב שכונת"),
        normalize_text("רחוב שכונה"),
        normalize_text('רחוב שכונת"'),
    }

    # אם תרצה הרחבה בהמשך (אופציונלי)
    INVALID_PROJECT_REGEXES = [
        re.compile(r"^רחוב\s*$"),
        re.compile(r"^בין\s+הבתים\s*$"),
        re.compile(r"^שטח\s+פתוח\s*$"),
        re.compile(r"^רחוב\s+שכונ(ה|ת)\s*$"),
    ]

    LOCATION_HELPER_TEXTS = {
        'רחוב/שכונה/תב"ע',
        'רחוב/שכונה/תב״ע',
        'רחוב/שכונה/תב”ע',
    }

    results: List[CheckResult] = []

    # ---- Resolve columns robustly (handles Unnamed + spaces + 2-row headers) ----
    norm_to_orig = {_norm_col(c): c for c in report_df.columns}

    def _tokens(s: str) -> set[str]:
        return {t for t in _norm_col(s).split(" ") if t}

    def _resolve_required(required: str) -> str | None:
        req_norm = _norm_col(required)
        req_tokens = _tokens(required)

        # 1) exact normalized match
        if req_norm in norm_to_orig:
            return norm_to_orig[req_norm]

        # 2) token containment (best for headers with suffixes like "שם פרויקט Unnamed...")
        candidates = []
        for norm_name, orig_name in norm_to_orig.items():
            cand_tokens = _tokens(norm_name)
            if req_tokens.issubset(cand_tokens):
                candidates.append((len(cand_tokens), len(norm_name), orig_name))

        if candidates:
            candidates.sort()
            return candidates[0][2]

        # 3) substring fallback
        for norm_name, orig_name in norm_to_orig.items():
            if req_norm and (req_norm in norm_name or norm_name in req_norm):
                return orig_name

        return None

    required_logical = ["שם פרויקט", "מיקום פרויקט"]
    resolved_map: dict[str, str] = {}
    missing_cols: list[str] = []

    for logical in required_logical:
        resolved = _resolve_required(logical)
        if resolved is None:
            missing_cols.append(logical)
        else:
            resolved_map[logical] = resolved

    if missing_cols:
        results.append(
            CheckResult(
                rule_id=RULE_ID,
                rule_name=RULE_NAME,
                severity=Severity.WARNING,
                sheet_name=SHEET,
                status=Status.FAIL,
                column_name=" / ".join(missing_cols),
                key_context="columns_presence",
                message=f"חסרות עמודות נדרשות לבדיקה: {', '.join(missing_cols)}",
            )
        )
        return results

    # ---- Data rows mask (same idea as Rule 12) ----
    id_norm = getattr(cfg, "report_project_id_col_norm", "מס' פרויקט")
    id_col = _resolve_required(id_norm)
    if id_col is None:
        # אם אין מס' פרויקט, נריץ “על הכל” אבל עדיין נדלג על שורות ריקות
        data_df = report_df.copy()
    else:
        id_series = report_df[id_col].astype(str).str.strip()
        data_mask = (
            report_df[id_col].notna()
            & (id_series != "-")
            & (id_series.str.lower() != "nan")
            & (id_series != "")
        )
        data_df = report_df.loc[data_mask].copy()

    #print("R15 DEBUG: rows total =", len(report_df), "rows after mask =", len(data_df))
    #print("R15 DEBUG: resolved cols =", resolved_map)

    # ---- Iterate ----
    for idx, row in data_df.iterrows():
        for logical_col, actual_df_col in resolved_map.items():
            raw_val = row.get(actual_df_col, None)
            if raw_val is None:
                continue

            val_str = str(raw_val).strip()
            if val_str == "" or val_str.lower() in {"nan", "none"}:
                continue  # empties handled by Rule 12

            # Skip the helper cell under the 2-row header in "מיקום פרויקט"
            if logical_col == "מיקום פרויקט" and val_str in LOCATION_HELPER_TEXTS:
                continue

            norm = normalize_text(val_str)

            is_invalid = (norm in INVALID_PROJECT_TEXTS) or any(rx.match(norm) for rx in INVALID_PROJECT_REGEXES)
            """
            if not is_invalid:
                continue  # only failures

            results.append(
                CheckResult(
                    rule_id=RULE_ID,
                    rule_name=RULE_NAME,
                    severity=Severity.WARNING,
                    sheet_name=SHEET,
                    status=Status.FAIL,
                    row_index=int(idx),
                    # IMPORTANT: show logical column name only (no Unnamed...)
                    column_name=logical_col,
                    actual_value=val_str,
                    expected_value='תיאור פרויקט מפורט (לא "רחוב"/"בין הבתים"/"שטח פתוח"/"רחוב שכונה")',
                    confidence=1.0,
                    method="Pattern",
                    message='ערך כללי מדי / לא תקין. יש להזין תיאור פרויקט מלא (לא "רחוב"/"בין הבתים"/"שטח פתוח"/"רחוב שכונה").',
                )
            )
            """

            is_invalid = (norm in INVALID_PROJECT_TEXTS) or any(
                rx.match(norm) for rx in INVALID_PROJECT_REGEXES
            )

            if is_invalid:
                results.append(
                    CheckResult(
                        rule_id=RULE_ID,
                        rule_name=RULE_NAME,
                        severity=Severity.WARNING,
                        sheet_name=SHEET,
                        status=Status.FAIL,
                        row_index=int(idx),
                        column_name=logical_col,  # show only logical col name
                        actual_value=val_str,
                        expected_value='תיאור פרויקט מפורט (לא "רחוב"/"בין הבתים"/"שטח פתוח"/"רחוב שכונה")',
                        confidence=1.0,
                        method="Pattern",
                        message='ערך כללי מדי / לא תקין. יש להזין תיאור פרויקט מלא (לא "רחוב"/"בין הבתים"/"שטח פתוח"/"רחוב שכונה").',
                    )
                )
            else:
                if EMIT_PASS_ROWS:
                    results.append(
                        CheckResult(
                            rule_id=RULE_ID,
                            rule_name=RULE_NAME,
                            severity=Severity.INFO,
                            sheet_name=SHEET,
                            status=Status.PASS_,
                            row_index=int(idx),
                            column_name=logical_col,
                            actual_value=val_str,
                            expected_value="",
                            confidence=1.0,
                            method="Pattern",
                            message="תקין",
                        )
                    )


    return results

def check_018_facility_rehab_upgrade(
    report_df: pd.DataFrame,
    cfg: PlanConfig,
) -> List[CheckResult]:
    """
    R_18 — בדיקת שיקום ושדרוג מתקנים

    Levels (each emitted per real row, PASS + FAIL):
      1) שנת הקמה: integer, 1960..current_year
      2) נפח/ספיקה: not empty
      3) פירוט העבודות (merged across 5 cols): at least one 'X' exists in Q–U
    """
    from datetime import datetime
    from openpyxl.utils import get_column_letter

    RULE_BASE = "R_18"
    RULE_NAME = "בדיקת שיקום ושדרוג מתקנים"
    SHEET = getattr(cfg, "report_sheet_name", "גיליון דיווח")

    MIN_YEAR = 1960
    CUR_YEAR = datetime.now().year

    # -----------------------------
    # Column resolution (robust)
    # -----------------------------
    cols = list(report_df.columns)

    def _norm_col(c: object) -> str:
        s = str(c) if c is not None else ""
        s = re.sub(r"\s*Unnamed:.*", "", s)
        s = re.sub(r"_level_\d+", "", s)
        s = s.replace("\n", " ")
        s = re.sub(r"\s+", " ", s).strip()
        return s

    norm_to_orig: dict[str, str] = {}
    for c in cols:
        n = _norm_col(c)
        if n and n not in norm_to_orig:
            norm_to_orig[n] = c

    year_key = "שנת הקמה"
    flow_key = "נפח/ספיקה"
    details_prefix = "פירוט העבודות"

    # exact OR prefix-match (handles units like 'נפח/ספיקה [מ"ק]/[מק"ש]')
    year_col = norm_to_orig.get(year_key) or next(
        (orig for norm, orig in norm_to_orig.items() if norm.startswith(year_key)),
        None
    )

    flow_col = norm_to_orig.get(flow_key) or next(
        (orig for norm, orig in norm_to_orig.items() if norm.startswith(flow_key)),
        None
    )



    id_norm = getattr(cfg, "report_project_id_col_norm", "מס' פרויקט")
    id_col = norm_to_orig.get(id_norm)


    details_cols = [c for c in cols if _norm_col(c).startswith(details_prefix)]
    print("R18 resolved:", {"id": id_col, "year": year_col, "flow": flow_col, "details": len(details_cols)})

    missing = []
    if year_col is None:
        missing.append(year_key)
    if flow_col is None:
        missing.append(flow_key)
    if id_col is None:
        missing.append(id_norm)
    if not details_cols:
        missing.append(details_prefix)

    if missing:
        return [
            CheckResult(
                rule_id=f"{RULE_BASE}_מבנה עמודות",
                rule_name=RULE_NAME,
                severity=Severity.CRITICAL,
                sheet_name=SHEET,
                status=Status.FAIL,
                message=f"Missing required columns (after normalization): {missing}",
                actual_value=list(report_df.columns),
                expected_value=[year_key, flow_key, f"{details_prefix}* (5 cols)"],
                row_index=None,
                column_name=None,
                key_context="columns_presence",
                excel_cells=None,
            )
        ]

    # -----------------------------
    # Excel cell mapping for highlighting
    # -----------------------------
    col_to_excel_letter = {col: get_column_letter(i + 1) for i, col in enumerate(cols)}

    header_row = getattr(cfg, "report_header_row", 6)
    excel_row_offset = header_row + 2  # same convention as R12

    def _cell_ref(df_i: int, col_name: str) -> Optional[str]:
        letter = col_to_excel_letter.get(col_name)
        if not letter:
            return None
        excel_row = int(df_i + excel_row_offset)
        return f"{SHEET}!{letter}{excel_row}"

    def _is_empty(v: object) -> bool:
        if v is None:
            return True
        if pd.isna(v):
            return True
        if isinstance(v, str) and v.strip() == "":
            return True
        s = str(v).strip()
        return s == "" or s.lower() in {"nan", "none"}

    def _is_real_row(df_i: int) -> bool:
        v = report_df.at[df_i, id_col]
        if _is_empty(v):
            return False
        s = str(v).strip()
        if s in {"-", ""}:
            return False
        if s.lower() == "nan":
            return False
        return True


    results: List[CheckResult] = []

    seen_project_ids: set[str] = set()

    for i in range(len(report_df)):
        if not _is_real_row(i):
            continue

        pid = str(report_df.at[i, id_col]).strip()

        # validate each unique project only once
        if pid in seen_project_ids:
            continue
        seen_project_ids.add(pid)
        excel_row = int(i + excel_row_offset)
        # =========================================================
        # Level 1 — שנת הקמה
        # =========================================================
        raw_year = report_df.at[i, year_col]
        year_ok = True
        year_int: Optional[int] = None

        if _is_empty(raw_year):
            year_ok = False
        else:
            try:
                year_int = int(float(str(raw_year).strip()))
                if year_int < MIN_YEAR or year_int > CUR_YEAR:
                    year_ok = False
            except Exception:
                year_ok = False

        year_status = Status.PASS_ if year_ok else Status.FAIL
        year_sev = Severity.INFO if year_ok else Severity.WARNING
        year_cells = None
        if not year_ok:
            ref = _cell_ref(i, year_col)
            year_cells = [ref] if ref else None

        results.append(
            CheckResult(
                rule_id=f"{RULE_BASE}_שנת הקמה",
                rule_name=RULE_NAME,
                severity=year_sev,
                sheet_name=SHEET,
                status=year_status,
                row_index=int(i),
                column_name=year_key,
                key_context=f"{id_norm}={pid} | excel_row={excel_row} | level=שנת הקמה",
                actual_value=raw_year,
                expected_value=f'בין השנים "{MIN_YEAR}-{CUR_YEAR}"',
                confidence=1.0,
                method="Range",
                excel_cells=year_cells,
                message=(
                    f"עבר: שנת הקמה={year_int}"
                    if year_ok
                    else f"נכשל: שנת הקמה חייב להיות מספר שלם בין {MIN_YEAR} ל-{CUR_YEAR}. ערך בפועל: {raw_year!r}"
                ),
            )
        )

        # =========================================================
        # Level 2 — נפח/ספיקה
        # =========================================================
        raw_flow = report_df.at[i, flow_col]
        flow_ok = not _is_empty(raw_flow)

        flow_status = Status.PASS_ if flow_ok else Status.FAIL
        flow_sev = Severity.INFO if flow_ok else Severity.WARNING
        flow_cells = None
        if not flow_ok:
            ref = _cell_ref(i, flow_col)
            flow_cells = [ref] if ref else None

        results.append(
            CheckResult(
                rule_id=f"{RULE_BASE}_נפח/ספיקה",
                rule_name=RULE_NAME,
                severity=flow_sev,
                sheet_name=SHEET,
                status=flow_status,
                row_index=int(i),
                column_name=flow_key,
                key_context=f"{id_norm}={pid} | excel_row={excel_row} | level=נפח/ספיקה",
                actual_value=raw_flow,
                expected_value="מכיל ערך",
                confidence=1.0,
                method="NotEmpty",
                excel_cells=flow_cells,
                message=(
                    f"עבר: נפח/ספיקה={raw_flow!r}"
                    if flow_ok
                    else 'נכשל: "נפח/ספיקה" לא יכול להיות ריק.'
                ),
            )
        )

        # =========================================================
        # Level 3 — פירוט העבודות (Q–U): at least one X
        # =========================================================
        raw_details = [report_df.at[i, dc] for dc in details_cols]

        def _details_label(col_name: str) -> str:
            # from "פירוט העבודות א\"מ" -> 'א"מ'
            n = _norm_col(col_name)
            n = n.replace(details_prefix, "").strip()
            return n or col_name

        x_labels = []
        non_empty_labels = []

        for dc, v in zip(details_cols, raw_details):
            label = _details_label(dc)
            if not _is_empty(v):
                non_empty_labels.append(label)
            if (not _is_empty(v)) and str(v).strip().upper() == "X":
                x_labels.append(label)



        details_ok = len(x_labels) > 0

        details_status = Status.PASS_ if details_ok else Status.FAIL
        details_sev = Severity.INFO if details_ok else Severity.WARNING
        details_cells = None
        if not details_ok:
            refs = []
            for dc in details_cols:
                ref = _cell_ref(i, dc)
                if ref:
                    refs.append(ref)
            details_cells = refs or None

        results.append(
            CheckResult(
                rule_id=f"{RULE_BASE}_פירוט העבודות",
                rule_name=RULE_NAME,
                severity=details_sev,
                sheet_name=SHEET,
                status=details_status,
                row_index=int(i),
                column_name=details_prefix,
                key_context=f"{id_norm}={pid} | excel_row={excel_row} | level=פירוט העבודות | cols={len(details_cols)}",
                actual_value=(
                    f"X ב: {', '.join(x_labels)}"
                    if x_labels
                    else ("ערכים ריקים" if len(non_empty_labels) == 0 else f"אין X; ערכים קיימים ב: {', '.join(non_empty_labels)}")
                ),
                expected_value="לפחות X אחד",
                confidence=1.0,
                method="AnyX",
                excel_cells=details_cells,
                message=(
                    "עבר: נמצא לפחות 'X' אחד בפירוט העבודות."
                    if details_ok
                    else "נכשל: חייב להיות לפחות 'X' אחד באחד מעמודות פירוט העבודות (Q–U)."
                ),
            )
        )

    return results

def check_019_total_planned_cost_per_project(
    report_df: pd.DataFrame,
    cfg: PlanConfig,
) -> List[CheckResult]:
    """
    R_19 — סה"כ עלות מתוכננת לפרויקט

    Per unique 'מס' פרויקט' row:
      אומדן פרויקט >= (Excel columns X + AA + AD) in the same row.

    Notes:
      - X/AA/AD are taken by Excel letter positions (stable layout).
      - Emits PASS + FAIL per project id.
    """
    from openpyxl.utils import get_column_letter, column_index_from_string

    RULE_ID = "R_19"
    RULE_NAME = 'סה"כ עלות מתוכננת לפרויקט'
    SHEET = getattr(cfg, "report_sheet_name", "גיליון דיווח")

    cols = list(report_df.columns)

    def _is_empty(v: object) -> bool:
        if v is None:
            return True
        if pd.isna(v):
            return True
        if isinstance(v, str) and v.strip() == "":
            return True
        s = str(v).strip()
        return s == "" or s.lower() in {"nan", "none"}

    def _to_number(v: object) -> Optional[float]:
        if _is_empty(v):
            return None
        try:
            s = str(v).strip()
            s = s.replace(",", "")
            return float(s)
        except Exception:
            return None

    def _fmt_int(v: Optional[float]) -> str:
        if v is None:
            return ""
        try:
            return str(int(round(float(v))))
        except Exception:
            return ""


    # Excel -> df column by letter (A=1)
    def _col_by_excel_letter(letter: str) -> Optional[str]:
        idx_1b = column_index_from_string(letter)  # A=1
        idx_0b = idx_1b - 1
        if idx_0b < 0 or idx_0b >= len(cols):
            return None
        return cols[idx_0b]

    # Resolve required columns
    id_norm = getattr(cfg, "report_project_id_col_norm", "מס' פרויקט")
    estimate_key = "אומדן פרויקט"

    # robust header normalization (like R18/R15)
    def _norm_col(c: object) -> str:
        s = str(c) if c is not None else ""
        s = re.sub(r"\s*Unnamed:.*", "", s)      # remove Unnamed artifacts
        s = re.sub(r"_level_\d+", "", s)         # remove MultiIndex level suffix
        s = s.replace("\u00A0", " ")             # NBSP
        s = s.replace("\n", " ").replace("\r", " ").replace("\t", " ")
        s = re.sub(r"\s+", " ", s).strip()
        return s

    norm_to_orig: dict[str, str] = {}
    for c in cols:
        n = _norm_col(c)
        if n and n not in norm_to_orig:
            norm_to_orig[n] = c

    def _tokens(s: str) -> set[str]:
        return {t for t in _norm_col(s).split(" ") if t}

    def _resolve_required(key: str) -> Optional[str]:
        key_norm = _norm_col(key)
        key_tokens = _tokens(key)

        # 1) exact match
        if key_norm in norm_to_orig:
            return norm_to_orig[key_norm]

        # 2) token containment (best for "אומדן פרויקט Unnamed: ...")
        candidates = []
        for norm_name, orig_name in norm_to_orig.items():
            cand_tokens = _tokens(norm_name)
            if key_tokens.issubset(cand_tokens):
                candidates.append((len(cand_tokens), len(norm_name), orig_name))

        if candidates:
            candidates.sort()
            return candidates[0][2]

        # 3) prefix / contains fallback
        for norm_name, orig_name in norm_to_orig.items():
            if norm_name.startswith(key_norm) or (key_norm in norm_name):
                return orig_name

        return None

    id_col = _resolve_required(id_norm)

    # Try by header name first, BUT fallback to Excel column AE (matches "AE >= X+AA+AD")
    estimate_col = _resolve_required(estimate_key) or _col_by_excel_letter("AE")



    x_col = _col_by_excel_letter("X")
    aa_col = _col_by_excel_letter("AA")
    ad_col = _col_by_excel_letter("AD")
    print("R19 resolved:", {"id": id_col, "estimate": estimate_col, "X": x_col, "AA": aa_col, "AD": ad_col})

    missing = []
    if id_col is None:
        missing.append(id_norm)
    if estimate_col is None:
        missing.append(f"{estimate_key} (or AE)")
    if x_col is None:
        missing.append("X")
    if aa_col is None:
        missing.append("AA")
    if ad_col is None:
        missing.append("AD")

    if missing:
        return [
            CheckResult(
                rule_id=f"{RULE_ID}_מבנה עמודות",
                rule_name=RULE_NAME,
                severity=Severity.CRITICAL,
                sheet_name=SHEET,
                status=Status.FAIL,
                row_index=None,
                column_name=None,
                key_context="columns_presence",
                actual_value=list(report_df.columns),
                expected_value=[id_norm, estimate_key, "X", "AA", "AD"],
                message=f"Missing required columns: {missing}",
            )
        ]

    # Excel highlighting mapping
    col_to_excel_letter = {col: get_column_letter(i + 1) for i, col in enumerate(cols)}
    header_row = getattr(cfg, "report_header_row", 6)
    excel_row_offset = header_row + 2  # same as R12/R18

    def _cell_ref(df_i: int, col_name: str) -> Optional[str]:
        letter = col_to_excel_letter.get(col_name)
        if not letter:
            return None
        excel_row = int(df_i + excel_row_offset)
        return f"{SHEET}!{letter}{excel_row}"

    def _is_real_row(df_i: int) -> bool:
        v = report_df.at[df_i, id_col]
        if _is_empty(v):
            return False
        s = str(v).strip()
        if s in {"-", ""}:
            return False
        if s.lower() == "nan":
            return False
        return True

    results: List[CheckResult] = []
    seen_project_ids: set[str] = set()

    for i in range(len(report_df)):
        if not _is_real_row(i):
            continue

        pid = str(report_df.at[i, id_col]).strip()
        if pid in seen_project_ids:
            continue
        seen_project_ids.add(pid)

        excel_row = int(i + excel_row_offset)

        raw_est = report_df.at[i, estimate_col]
        raw_x = report_df.at[i, x_col]
        raw_aa = report_df.at[i, aa_col]
        raw_ad = report_df.at[i, ad_col]

        est = _to_number(raw_est)
        vx = _to_number(raw_x) or 0.0
        vaa = _to_number(raw_aa) or 0.0
        vad = _to_number(raw_ad) or 0.0
        ssum = vx + vaa + vad

        # Pretty strings for export
        expected_str = f'אומדן פרויקט >= סה"כ עלויות צפויות {int(round(ssum))}'

        if est is None:
            actual_str = "אומדן פרויקט ריק"
            ok = False
            fail_msg = "נכשל: אומדן פרויקט ללא ערך"
        else:
            actual_str = f"אומדן פרויקט : {int(round(est))}"
            ok = est >= ssum
            fail_msg = f"נכשל: אומדן פרויקט חייב להיות >= סה\"כ עלויות צפויות {int(round(ssum))}"




        status = Status.PASS_ if ok else Status.FAIL
        sev = Severity.INFO if ok else Severity.WARNING

        excel_cells = None
        if not ok:
            refs = []
            for c in [estimate_col, x_col, aa_col, ad_col]:
                r = _cell_ref(i, c)
                if r:
                    refs.append(r)
            excel_cells = refs or None

        results.append(
            CheckResult(
                rule_id=RULE_ID,
                rule_name=RULE_NAME,
                severity=sev,
                sheet_name=SHEET,
                status=status,
                row_index=int(i),
                column_name=estimate_key,
                key_context=f"{id_norm}={pid} | excel_row={excel_row}",
                actual_value=actual_str,
                expected_value=expected_str,
                confidence=1.0,
                method="Compare",
                excel_cells=excel_cells,
                message=(
                    f"עבר: אומדן={int(round(est))} >= סכום={int(round(ssum))}"
                    if ok
                    else fail_msg
                ),

            )
        )

    return results


def check_020_project_status_planning_report(
    report_df: pd.DataFrame,
    cfg: PlanConfig,
) -> List[CheckResult]:
    """
    R_20 — סטטוס פרויקט (בדווח תכנון)

    Per unique 'מס' פרויקט' row:
      Excel column AH (סטטוס פרויקט) must be non-empty.
    """
    from openpyxl.utils import get_column_letter, column_index_from_string

    RULE_ID = "R_20"
    RULE_NAME = "סטטוס פרויקט (בדווח תכנון)"
    SHEET = getattr(cfg, "report_sheet_name", "גיליון דיווח")

    cols = list(report_df.columns)

    def _is_empty(v: object) -> bool:
        # If pandas returns a Series (e.g., duplicate columns), treat empty if ALL elements empty
        if isinstance(v, pd.Series):
            if v.empty:
                return True
            return all(_is_empty(x) for x in v.tolist())

        # If it's a list/tuple/ndarray (rare but possible), same logic
        if isinstance(v, (list, tuple)):
            if len(v) == 0:
                return True
            return all(_is_empty(x) for x in v)

        if v is None:
            return True

        # pd.isna on scalars only
        try:
            if pd.isna(v):
                return True
        except Exception:
            pass

        if isinstance(v, str) and v.strip() == "":
            return True

        s = str(v).strip()
        return s == "" or s.lower() in {"nan", "none"}


    # Excel -> df column by letter (A=1)
    def _col_by_excel_letter(letter: str) -> Optional[str]:
        idx_1b = column_index_from_string(letter)  # A=1
        idx_0b = idx_1b - 1
        if idx_0b < 0 or idx_0b >= len(cols):
            return None
        return cols[idx_0b]

    # Robust header normalization (same spirit as R_19)
    def _norm_col(c: object) -> str:
        s = str(c) if c is not None else ""
        s = s.replace("\n", " ")
        s = re.sub(r"\s+", " ", s).strip()
        return s

    norm_to_orig = {_norm_col(c): c for c in cols}

    def _resolve_by_prefix(key: str) -> Optional[str]:
        if key in norm_to_orig:
            return norm_to_orig[key]
        for norm, orig in norm_to_orig.items():
            if norm.startswith(key):
                return orig
        for norm, orig in norm_to_orig.items():
            if key in norm:
                return orig
        return None

    # Required columns
    id_norm = getattr(cfg, "report_project_id_col_norm", "מס' פרויקט")
    id_col = _resolve_by_prefix(id_norm)

    status_col = _col_by_excel_letter("AF")  # <-- Excel letter AH

    # OPTIONAL debug print (like R18/R19)
    print("R20 resolved:", {"id": id_col, "AF": status_col})

    missing = []
    if id_col is None:
        missing.append(id_norm)
    if status_col is None:
        missing.append("AF")

    if missing:
        return [
            CheckResult(
                rule_id=f"{RULE_ID}_מבנה עמודות",
                rule_name=RULE_NAME,
                severity=Severity.CRITICAL,
                sheet_name=SHEET,
                status=Status.FAIL,
                row_index=None,
                column_name=None,
                key_context="columns_presence",
                actual_value=list(report_df.columns),
                expected_value=[id_norm, "AH"],
                message=f"Missing required columns: {missing}",
            )
        ]

    # Excel highlighting mapping
    col_to_excel_letter = {col: get_column_letter(i + 1) for i, col in enumerate(cols)}
    header_row = getattr(cfg, "report_header_row", 6)
    excel_row_offset = header_row + 2  # same as R12/R18/R19

    def _cell_ref(df_i: int, col_name: str) -> Optional[str]:
        letter = col_to_excel_letter.get(col_name)
        if not letter:
            return None
        excel_row = int(df_i + excel_row_offset)
        return f"{SHEET}!{letter}{excel_row}"

    def _is_real_row(df_i: int) -> bool:
        v = report_df.at[df_i, id_col]
        if _is_empty(v):
            return False
        s = str(v).strip()
        if s in {"-", ""}:
            return False
        if s.lower() == "nan":
            return False
        return True

    results: List[CheckResult] = []
    seen_project_ids: set[str] = set()

    for i in range(len(report_df)):
        if not _is_real_row(i):
            continue

        pid = str(report_df.at[i, id_col]).strip()
        if pid in seen_project_ids:
            continue
        seen_project_ids.add(pid)

        excel_row = int(i + excel_row_offset)

        raw_status = report_df.at[i, status_col]
        ok = not _is_empty(raw_status)

        status = Status.PASS_ if ok else Status.FAIL
        sev = Severity.INFO if ok else Severity.CRITICAL

        if ok:
            actual_str = f"סטטוס : {str(raw_status).strip()}"
            msg = "עבר: סטטוס קיים"
            excel_cells = None
        else:
            actual_str = "סטטוס ריק"
            msg = "נכשל: סטטוס פרויקט ללא ערך"
            ref = _cell_ref(i, status_col)
            excel_cells = [ref] if ref else None

        results.append(
            CheckResult(
                rule_id=RULE_ID,
                rule_name=RULE_NAME,
                severity=sev,
                sheet_name=SHEET,
                status=status,
                row_index=int(i),
                column_name="סטטוס פרויקט",
                key_context=f"{id_norm}={pid} | excel_row={excel_row}",
                actual_value=actual_str,
                expected_value="מכיל ערך",
                confidence=1.0,
                method="NotEmpty",
                excel_cells=excel_cells,
                message=msg,
            )
        )

    return results


def check_024_short_pipe_projects_ratio(
    report_df: pd.DataFrame,
    cfg: PlanConfig,
) -> List[CheckResult]:
    """
    R_24 — ratio of expected costs (AE) for projects whose minimum pipe length in column M is < 100.

    - Identify real project rows by project-id column (default "מס' פרויקט")
    - For each row: extract all numbers in M (supports ":" separated), take MIN.
      If MIN < 100 => row is in "small pipe" group.
    - Sum AE ONLY for rows with numeric AE (skip rows where AE is empty/unparseable)
      - If ALL rows have AE empty => CRITICAL "no info"
      - If AE has content but NONE numeric => CRITICAL "no numeric"
    - ratio = sum_small / sum_total; if ratio > 5% => FAIL (CRITICAL)
    """
    import re
    import math
    from openpyxl.utils import column_index_from_string

    RULE_ID = "R_24"
    RULE_NAME = "אחוז פרויקטים עם אורך צנרת קטן מ-100 מטרים"
    SHEET = getattr(cfg, "report_sheet_name", "גיליון דיווח")

    THRESH_M = 100.0
    THRESH_RATIO = 0.05  # 5%

    cols = list(report_df.columns)

    def _finite_or_none(x: Optional[float]) -> Optional[float]:
        try:
            if x is None:
                return None
            x = float(x)
            return x if math.isfinite(x) else None
        except Exception:
            return None

    def _safe_int(x: Optional[float]) -> int:
        x2 = _finite_or_none(x)
        return int(round(x2)) if x2 is not None else 0

    # Excel -> df column by letter (A=1)
    def _col_by_excel_letter(letter: str) -> Optional[str]:
        idx_1b = column_index_from_string(letter)
        idx_0b = idx_1b - 1
        if idx_0b < 0 or idx_0b >= len(cols):
            return None
        return cols[idx_0b]

    def _is_empty_scalar(v: object) -> bool:
        if v is None:
            return True
        if isinstance(v, (list, tuple, dict, set)):
            return len(v) == 0
        if hasattr(v, "shape") and not isinstance(v, (str, bytes)):
            return False
        try:
            if pd.isna(v):
                return True
        except Exception:
            pass
        if isinstance(v, str) and v.strip() == "":
            return True
        s = str(v).strip()
        return s == "" or s.lower() in {"nan", "none"}

    def _to_number(v: object) -> Optional[float]:
        if _is_empty_scalar(v):
            return None
        try:
            s = str(v).strip().replace(",", "")
            return float(s)
        except Exception:
            return None

    def _extract_numbers_min(v: object) -> Optional[float]:
        """
        Return the minimum numeric value found in the cell.
        Supports colon-separated patterns like "800:500:200".
        Falls back to regex for messy text.
        """
        if _is_empty_scalar(v):
            return None

        s = str(v).strip()

        # Fast path: ":" / commas / whitespace separated tokens
        parts = re.split(r"[:，,;\s]+", s)
        vals: list[float] = []
        for p in parts:
            p = p.strip()
            if not p:
                continue
            if re.fullmatch(r"-?\d+(?:\.\d+)?", p):
                try:
                    vals.append(float(p))
                except Exception:
                    pass
        if vals:
            return min(vals)

        # Fallback: find numbers anywhere
        nums = re.findall(r"-?\d+(?:\.\d+)?", s)
        if not nums:
            return None
        vals = []
        for t in nums:
            try:
                vals.append(float(t))
            except Exception:
                continue
        return min(vals) if vals else None

    # Resolve project id column by normalized header
    def _norm_col(c: object) -> str:
        s = str(c) if c is not None else ""
        s = re.sub(r"\s*Unnamed:.*", "", s)
        s = re.sub(r"_level_\d+", "", s)
        s = s.replace("\n", " ")
        s = re.sub(r"\s+", " ", s).strip()
        return s

    norm_to_orig: dict[str, str] = {}
    for c in cols:
        n = _norm_col(c)
        if n and n not in norm_to_orig:
            norm_to_orig[n] = c

    id_norm = getattr(cfg, "report_project_id_col_norm", "מס' פרויקט")
    id_col = norm_to_orig.get(id_norm)

    col_m = _col_by_excel_letter("M")
    col_ae = _col_by_excel_letter("AE")

    print("R24 resolved:", {"id": id_col, "M": col_m, "AE": col_ae})

    missing = []
    if id_col is None:
        missing.append(id_norm)
    if col_m is None:
        missing.append("M")
    if col_ae is None:
        missing.append("AE")

    if missing:
        return [
            CheckResult(
                rule_id=f"{RULE_ID}_מבנה עמודות",
                rule_name=RULE_NAME,
                severity=Severity.CRITICAL,
                sheet_name=SHEET,
                status=Status.FAIL,
                row_index=None,
                column_name=None,
                key_context="columns_presence",
                actual_value=list(report_df.columns),
                expected_value=[id_norm, "M", "AE"],
                message=f"Missing required columns: {missing}",
            )
        ]

    def _is_real_row(df_i: int) -> bool:
        v = report_df.at[df_i, id_col]
        if _is_empty_scalar(v):
            return False
        s = str(v).strip()
        if s in {"-", ""}:
            return False
        if s.lower() == "nan":
            return False
        return True

    sum_total = 0.0
    sum_small = 0.0

    any_rows = 0                 # count of real project rows (by id)
    any_small = 0                # count of rows where min(M) < 100 (regardless of AE)

    ae_present_rows = 0          # AE has any content (non-empty)
    ae_numeric_rows = 0          # AE parsed to finite number
    ae_skipped_total = 0         # rows skipped from denominator
    ae_skipped_small = 0         # rows skipped from numerator (in small group)

    for i in range(len(report_df)):
        if not _is_real_row(i):
            continue
        any_rows += 1

        raw_m = report_df.at[i, col_m]
        m_min = _extract_numbers_min(raw_m)
        is_small = (m_min is not None) and (m_min < THRESH_M)
        if is_small:
            any_small += 1

        raw_ae = report_df.at[i, col_ae]
        if not _is_empty_scalar(raw_ae):
            ae_present_rows += 1

        ae_num = _to_number(raw_ae)
        if ae_num is None or (not math.isfinite(ae_num)):
            ae_skipped_total += 1
            if is_small:
                ae_skipped_small += 1
            continue

        ae_numeric_rows += 1
        sum_total += ae_num
        if is_small:
            sum_small += ae_num

    if any_rows == 0:
        return [
            CheckResult(
                rule_id=RULE_ID,
                rule_name=RULE_NAME,
                severity=Severity.WARNING,
                sheet_name=SHEET,
                status=Status.FAIL,
                row_index=None,
                column_name="M",
                key_context="no_project_rows",
                actual_value="לא נמצאו שורות פרויקט",
                expected_value="נדרש לפחות פרויקט אחד",
                confidence=1.0,
                method="Summary",
                message="נכשל: לא נמצאו שורות פרויקט לביצוע בדיקה",
            )
        ]

    # ALARM: all AE cells empty across all real project rows
    if ae_present_rows == 0:
        return [
            CheckResult(
                rule_id=RULE_ID,
                rule_name=RULE_NAME,
                severity=Severity.CRITICAL,
                sheet_name=SHEET,
                status=Status.FAIL,
                row_index=None,
                column_name="AE",
                key_context="ae_no_info_all_rows",
                actual_value=f"AE ריק בכל שורות הפרויקט (סה\"כ פרויקטים: {any_rows})",
                expected_value="נדרש לפחות אומדן אחד בעמודה AE",
                confidence=1.0,
                method="Summary",
                message="שגיאה קריטית: אין מידע בעמודה AE (כל הערכים ריקים), לא ניתן לחשב יחס.",
            )
        ]

    # ALARM: AE has content but none numeric/finite
    if ae_numeric_rows == 0:
        return [
            CheckResult(
                rule_id=RULE_ID,
                rule_name=RULE_NAME,
                severity=Severity.CRITICAL,
                sheet_name=SHEET,
                status=Status.FAIL,
                row_index=None,
                column_name="AE",
                key_context="ae_unparseable_all_rows",
                actual_value=f"AE לא מספרי בכל שורות הפרויקט (סה\"כ פרויקטים: {any_rows}, לא ריקים: {ae_present_rows})",
                expected_value="נדרש אומדן מספרי בעמודה AE",
                confidence=1.0,
                method="Summary",
                message="שגיאה קריטית: בעמודה AE אין אף ערך מספרי תקין, לא ניתן לחשב יחס.",
            )
        ]

    # After skipping missing AE rows, still must have a positive denominator
    if (not math.isfinite(sum_total)) or (sum_total <= 0):
        return [
            CheckResult(
                rule_id=RULE_ID,
                rule_name=RULE_NAME,
                severity=Severity.CRITICAL,
                sheet_name=SHEET,
                status=Status.FAIL,
                row_index=None,
                column_name="AE",
                key_context="total_estimate_zero_after_skips",
                actual_value=f"סה\"כ אומדנים (רק ערכים תקינים)={_safe_int(sum_total)} | דילוגים={ae_skipped_total}",
                expected_value="סה\"כ אומדנים > 0",
                confidence=1.0,
                method="Summary",
                message="נכשל: לאחר דילוג על פרויקטים ללא אומדן AE תקין, סה\"כ האומדנים הוא 0 ולכן לא ניתן לחשב יחס.",
            )
        ]

    ratio = sum_small / sum_total
    pct = ratio * 100.0

    note = ""
    if ae_skipped_total > 0:
        note = f" | דילוג על {ae_skipped_total} פרויקטים ללא אומדן AE מספרי תקין"
        if ae_skipped_small > 0:
            note += f" (מתוכם {ae_skipped_small} בקבוצת <100)"

    ok = ratio <= THRESH_RATIO
    status = Status.PASS_ if ok else Status.FAIL
    sev = Severity.INFO if ok else Severity.CRITICAL

    actual_str = f"{pct:.2f}% (סכום קטן מ-100: {_safe_int(sum_small)} מתוך {_safe_int(sum_total)}){note}"
    expected_str = "עד 5%"

    msg = (
        f"עבר: אחוז הפרויקטים עם אורך צנרת קטן מ-100 מטרים הוא {pct:.2f}%{note}"
        if ok
        else f"שגיאה: אחוז הפרויקטים עם אורך צנרת קטן מ-100 מטרים הוא {pct:.2f}% (> 5%){note}"
    )

    return [
        CheckResult(
            rule_id=RULE_ID,
            rule_name=RULE_NAME,
            severity=sev,
            sheet_name=SHEET,
            status=status,
            row_index=None,
            column_name="אורך צנרת (M) + אומדן (AE)",
            key_context=(
                f"small<100_count={any_small} | total_projects={any_rows} | "
                f"ae_numeric_rows={ae_numeric_rows} | skipped_ae_rows={ae_skipped_total}"
            ),
            actual_value=actual_str,
            expected_value=expected_str,
            confidence=1.0,
            method="SummaryRatio",
            excel_cells=None,
            message=msg,
        )
    ]


# Regex catches things like:
# "רחוב שכונת" / "רחוב שכונה" / "רחוב שכונת ..." where there's no real descriptor
INVALID_PROJECT_REGEXES = [
    re.compile(r"^\s*רחוב\s*$"),
    re.compile(r"^\s*בין\s+הבתים\s*$"),
    re.compile(r"^\s*שטח\s+פתוח\s*$"),
    re.compile(r"^\s*רחוב\s+שכונ(?:ה|ת)\s*$"),  # "רחוב שכונה/שכונת" only
]


__all__ = [
    "check_001_kinun_values_rounded",
    "check_rule02_03_asset_ratio",
    "check_004_total_program_values",
    "check_005_min_required_program",
    "check_006_rehab_upgrade_min_required",
    "check_007_total_planned_investments_by_city",
    "check_008_funding_total_and_exists_by_city",
    "check_010_pipes_any_value",
    "check_011_pipes_values_by_type",
    "check_012_project_fields_not_empty",
    "check_014_llm_project_funding_classification",
    "check_015_invalid_project_names",
    "check_018_facility_rehab_upgrade",
    "check_019_total_planned_cost_per_project",
    "check_020_project_status_planning_report",
    "check_024_short_pipe_projects_ratio",

]
