from __future__ import annotations
 
import math
import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
 
import pandas as pd
 
 
# =========================================================
# Enums and result model
# =========================================================
 
class Severity(str, Enum):
    CRITICAL = "Critical"
    WARNING = "Warning"
    INFO = "Info"
 
 
class Status(str, Enum):
    PASS_ = "Pass"
    FAIL = "Fail"
    NOT_APPLICABLE = "Not applicable"
 
 
@dataclass
class CheckResult:
    rule_id: str
    rule_name: str
    severity: Severity
    sheet_name: str
    row_index: Optional[int]          # pandas 0-based row index in plan_df
    column_name: Optional[str]
    key_context: str
    actual_value: Any
    expected_value: Any
    status: Status
    message: str
 
 
# =========================================================
# Configuration
# =========================================================
 
@dataclass
class PlanConfig:
    sheet_name: str = "סיכום תכנית השקעות"
 
    # Raw Excel column indices (0-based): A=0, B=1, ..., R=17, S=18
    label_col_idx: int = 0
    data_marker_col_idx: int = 1  # column B: contains מים/ביוב/סה"כ
    value_col_r_idx: int = 17
    value_col_s_idx: int = 18
 
    # Header detection window (rows above first 'מים')
    header_lookback_rows: int = 6
 
    # Dynamic anchor – set at runtime by loader:
    data_start_excel_row: Optional[int] = None  # 1-based Excel row number where plan_df row 0 starts
 
    # Kinun reference (flattened A–E structure)
    kinun_utility_col: str = "תאגיד מים וביוב"
    kinun_full_water_col: str = "תשתיות מים מלא"
    kinun_reduced_water_col: str = "תשתיות מים מופחת"
    kinun_full_sewer_col: str = "תשתיות ביוב מלא"
    kinun_reduced_sewer_col: str = "תשתיות ביוב מופחת"
 
    # Fixed Excel row mapping (1-based Excel rows)
    total_program_rows_excel: Dict[str, int] = None          # R_004
    min_required_program_rows_excel: Dict[str, int] = None   # R_005
    rehab_upgrade_min_rows_excel: Dict[str, int] = None      # R_006
    asset_ratio_rows_excel: Dict[str, int] = None            # R_003
 
    # Kinun mapping (plan rows for the kinun values we compare)
    kinun_plan_rows_excel: Dict[Tuple[str, str], int] = None  # (label, system) -> excel row
 
    # City-based checks (row-based)
    planned_investments_row_excel: int = 39   # R_007
    funding_total_row_excel: int = 50         # R_008 (merged with old R_009)
    water_pipe_rows_excel: Tuple[int, int] = (56, 57)  # R_010
    sewer_pipe_row_excel: int = 58                    # R_011
 
    def __post_init__(self):
        if self.total_program_rows_excel is None:
            self.total_program_rows_excel = {"מים": 8, "ביוב": 9, "סה\"כ": 10}
 
        if self.min_required_program_rows_excel is None:
            self.min_required_program_rows_excel = {"מים": 25, "ביוב": 26, "סה\"כ": 27}
 
        if self.rehab_upgrade_min_rows_excel is None:
            self.rehab_upgrade_min_rows_excel = {"מים": 28, "ביוב": 29, "סה\"כ": 30}
 
        # You calibrated this already (keep as-is)
        if self.asset_ratio_rows_excel is None:
            self.asset_ratio_rows_excel = {"מים": 20, "ביוב": 21, "סה\"כ": 22}
 
        if self.kinun_plan_rows_excel is None:
            self.kinun_plan_rows_excel = {
                ("ערך כינון מלא", "מים"): 8,
                ("ערך כינון מלא", "ביוב"): 9,
                ("ערך כינון מופחת", "מים"): 11,
                ("ערך כינון מופחת", "ביוב"): 12,
            }
 
 
# =========================================================
# Helpers
# =========================================================
 
def normalize_text(value: object) -> str:
    s = "" if value is None else str(value)
    s = s.replace("\u200f", "").replace("\u200e", "")
    s = re.sub(r"\s+", " ", s).strip()
    return s
 
 
def round_half_up(value: Any, ndigits: int = 0) -> Optional[float]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None
    factor = 10 ** ndigits
    return math.floor(x * factor + 0.5) / factor
 
 
def parse_ratio(value: Any) -> Optional[float]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
 
    if isinstance(value, str):
        s = normalize_text(value)
        if s.endswith("%"):
            try:
                return float(s[:-1]) / 100.0
            except ValueError:
                return None
        try:
            x = float(s)
        except ValueError:
            return None
    else:
        try:
            x = float(value)
        except (TypeError, ValueError):
            return None
 
    return x / 100.0 if x > 1.0 else x
 
 
def extract_utility_from_plan_filename(plan_file: str | Path) -> str:
    stem = normalize_text(Path(plan_file).stem)
    m = re.search(r"\bתאגיד\b\s*(.+)$", stem)
    if not m:
        raise ValueError(f"Cannot extract utility from filename (missing 'תאגיד'): {stem}")
    utility = normalize_text(m.group(1))
    if not utility:
        raise ValueError(f"Cannot extract utility from filename (empty after 'תאגיד'): {stem}")
    return utility
 
 
def excel_row_to_df_index(excel_row_1_based: int, cfg: PlanConfig) -> int:
    if cfg.data_start_excel_row is None:
        raise ValueError("cfg.data_start_excel_row is not set. Loader must run before checks.")
    return excel_row_1_based - cfg.data_start_excel_row
 
 
def get_cell(plan_df: pd.DataFrame, df_row_idx: int, col_idx: int) -> Any:
    return plan_df.iat[df_row_idx, col_idx]
 
 
# ---------- city header heuristics ----------
 
def is_date_like(s: str) -> bool:
    return bool(re.match(r"^\d{1,2}\.\d{1,2}\.\d{2,4}$", s))
 
 
def is_city_like_header(value: Any) -> bool:
    s = normalize_text(value)
    if not s:
        return False
    if s.startswith("Unnamed"):
        return False
    if is_date_like(s):
        return False
    if any(ch.isdigit() for ch in s):
        return False
 
    blacklist = {
        "תאגיד", "תאגיד מים וביוב", "רשויות", "רשויות/ת\"מ במתאגיד", "רשויות/ת״ם במתאגיד",
        "מים", "ביוב", "סה\"כ", "סה״כ",
        "תאריך", "תאריכים",
    }
    if s in blacklist:
        return False
 
    return any("\u0590" <= ch <= "\u05FF" for ch in s)
 
 
def longest_true_run(mask: List[bool]) -> Tuple[int, int]:
    best = (-1, -1)
    best_len = 0
    start = None
 
    for i, v in enumerate(mask):
        if v and start is None:
            start = i
        if (not v or i == len(mask) - 1) and start is not None:
            end = i if v and i == len(mask) - 1 else i - 1
            run_len = end - start + 1
            if run_len > best_len:
                best_len = run_len
                best = (start, end)
            start = None
 
    return best
 
 
def detect_city_cols(plan_df: pd.DataFrame) -> List[str]:
    cols = list(plan_df.columns)
    mask = [is_city_like_header(c) for c in cols]
    start, end = longest_true_run(mask)
    if start == -1:
        return []
    return cols[start:end + 1]
 
 
def fail_no_cities(rule_id: str, rule_name: str, cfg: PlanConfig) -> List[CheckResult]:
    return [CheckResult(
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
    )]
 
 
# =========================================================
# Plan loader: correct header selection + correct Excel-row anchor
# =========================================================
 
def load_plan_sheet_with_header_fix(plan_file: str | Path, cfg: PlanConfig) -> pd.DataFrame:
    raw = pd.read_excel(plan_file, sheet_name=cfg.sheet_name, header=None)
 
    # Find data start row (0-based in raw): first 'מים' in column B
    data_start_idx = None
    for i in range(len(raw)):
        v = normalize_text(raw.iat[i, cfg.data_marker_col_idx] if cfg.data_marker_col_idx < raw.shape[1] else None)
        if v == "מים":
            data_start_idx = i
            break
    if data_start_idx is None:
        raise ValueError("Could not detect data start row (expected to find 'מים' in column B).")
 
    # Set dynamic anchor: plan_df row 0 corresponds to this Excel row number (1-based)
    cfg.data_start_excel_row = data_start_idx + 1
 
    # Pick best header row from lookback window above data_start_idx
    lookback = max(1, int(cfg.header_lookback_rows))
    candidates = range(max(0, data_start_idx - lookback), data_start_idx)
 
    best_header_idx = None
    best_score = -1
    for r in candidates:
        row_vals = raw.iloc[r].tolist()
        score = sum(1 for x in row_vals if is_city_like_header(x))
        if score > best_score:
            best_score = score
            best_header_idx = r
 
    if best_header_idx is None or best_score <= 0:
        raise ValueError("Could not detect a city header row (no city-like headers found above the first 'מים' row).")
 
    headers = [normalize_text(x) for x in raw.iloc[best_header_idx].tolist()]
 
    df = raw.iloc[data_start_idx:].copy()
    df.columns = headers
    df.reset_index(drop=True, inplace=True)
    return df
 
 
# =========================================================
# Kinun loader (A–E format)
# =========================================================
 
def load_kinun_reference(kinun_file: str | Path) -> pd.DataFrame:
    raw = pd.read_excel(kinun_file, header=None, usecols="A:E")
 
    def is_data_row(row: pd.Series) -> bool:
        a = normalize_text(row.iloc[0])
        b, c, d, e = row.iloc[1], row.iloc[2], row.iloc[3], row.iloc[4]
        if not a:
            return False
        if a in {"תאגיד מים וביוב", "תאגיד", "תאגיד מים", "אלישיב"}:
            return False
        return any(pd.notna(x) and isinstance(x, (int, float)) for x in [b, c, d, e])
 
    start_idx = None
    for i in range(len(raw)):
        if is_data_row(raw.iloc[i]):
            start_idx = i
            break
    if start_idx is None:
        raise ValueError("Could not detect first data row in kinun file (columns A–E).")
 
    df = raw.iloc[start_idx:].copy()
    df.columns = [
        "תאגיד מים וביוב",
        "תשתיות מים מלא",
        "תשתיות מים מופחת",
        "תשתיות ביוב מלא",
        "תשתיות ביוב מופחת",
    ]
    df = df[df["תאגיד מים וביוב"].notna()].copy()
    df["תאגיד מים וביוב"] = df["תאגיד מים וביוב"].map(normalize_text)
    return df
 
 
def lookup_kinun_value(kinun_df: pd.DataFrame, cfg: PlanConfig, utility_name: str, col_name: str) -> Any:
    target = normalize_text(utility_name)
    hits = kinun_df[kinun_df[cfg.kinun_utility_col].map(normalize_text) == target]
    if hits.empty:
        raise ValueError(f"Utility '{utility_name}' not found in kinun file under column '{cfg.kinun_utility_col}'.")
    return hits.iloc[0][col_name]
 
 
# =========================================================
# Checks
# =========================================================
 
def check_001_kinun_values_rounded(plan_df: pd.DataFrame, kinun_df: pd.DataFrame, utility: str, cfg: PlanConfig) -> List[CheckResult]:
    mapping = {
        ("ערך כינון מלא", "מים"): cfg.kinun_full_water_col,
        ("ערך כינון מלא", "ביוב"): cfg.kinun_full_sewer_col,
        ("ערך כינון מופחת", "מים"): cfg.kinun_reduced_water_col,
        ("ערך כינון מופחת", "ביוב"): cfg.kinun_reduced_sewer_col,
    }
 
    results: List[CheckResult] = []
    for (label, system), excel_row in cfg.kinun_plan_rows_excel.items():
        df_idx = excel_row_to_df_index(excel_row, cfg)
        plan_raw = get_cell(plan_df, df_idx, cfg.value_col_r_idx)
 
        kinun_col = mapping[(label, system)]
        kinun_raw = lookup_kinun_value(kinun_df, cfg, utility, kinun_col)
 
        plan_round = round_half_up(plan_raw, 0)
        kinun_round = round_half_up(kinun_raw, 0)
 
        results.append(CheckResult(
            rule_id=f"R_001_{label}_{system}",
            rule_name="בדיקת ערכי כינון (עיגול לפני השוואה)",
            severity=Severity.CRITICAL,
            sheet_name=cfg.sheet_name,
            row_index=df_idx,
            column_name="R",
            key_context=f"plan_cell=R{excel_row}; kinun_col={kinun_col}",
            actual_value=plan_round,
            expected_value=kinun_round,
            status=Status.PASS_ if plan_round == kinun_round else Status.FAIL,
            message=f"Plan raw={plan_raw} -> {plan_round}; Kinun raw={kinun_raw} -> {kinun_round}.",
        ))
    return results
 
 
def check_003_asset_ratio_below_100(plan_df: pd.DataFrame, cfg: PlanConfig) -> List[CheckResult]:
    results: List[CheckResult] = []
    for system, excel_row in cfg.asset_ratio_rows_excel.items():
        df_idx = excel_row_to_df_index(excel_row, cfg)
        raw = get_cell(plan_df, df_idx, cfg.value_col_r_idx)
        ratio = parse_ratio(raw)
 
        if ratio is None:
            status = Status.FAIL
            msg = f"Asset ratio not numeric/parsable. raw={raw!r}"
        else:
            status = Status.PASS_ if 0 < ratio < 1 else Status.FAIL
            msg = f"Asset ratio={ratio}; expected 0 < ratio < 1."
 
        results.append(CheckResult(
            rule_id=f"R_003_{system}",
            rule_name="גריעת נכסים (יחס נכסים < 100%)",
            severity=Severity.CRITICAL,
            sheet_name=cfg.sheet_name,
            row_index=df_idx,
            column_name="R",
            key_context=f"plan_cell=R{excel_row}",
            actual_value=ratio,
            expected_value="0 < ratio < 1",
            status=status,
            message=msg,
        ))
    return results
 
 
def check_004_total_program_values(plan_df: pd.DataFrame, cfg: PlanConfig) -> List[CheckResult]:
    results: List[CheckResult] = []
    for system, excel_row in cfg.total_program_rows_excel.items():
        df_idx = excel_row_to_df_index(excel_row, cfg)
        val = get_cell(plan_df, df_idx, cfg.value_col_r_idx)
        results.append(CheckResult(
            rule_id=f"R_004_{system}",
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
        ))
    return results
 
 
def check_005_min_required_program(plan_df: pd.DataFrame, cfg: PlanConfig) -> List[CheckResult]:
    results: List[CheckResult] = []
    for system, excel_row in cfg.min_required_program_rows_excel.items():
        df_idx = excel_row_to_df_index(excel_row, cfg)
        val = get_cell(plan_df, df_idx, cfg.value_col_r_idx)
        results.append(CheckResult(
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
        ))
    return results
 
 
def check_006_rehab_upgrade_min_required(plan_df: pd.DataFrame, cfg: PlanConfig) -> List[CheckResult]:
    results: List[CheckResult] = []
    for system, excel_row in cfg.rehab_upgrade_min_rows_excel.items():
        df_idx = excel_row_to_df_index(excel_row, cfg)
        val = get_cell(plan_df, df_idx, cfg.value_col_s_idx)
        results.append(CheckResult(
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
        ))
    return results
 
 
def check_007_total_planned_investments_by_city(plan_df: pd.DataFrame, cfg: PlanConfig) -> List[CheckResult]:
    city_cols = detect_city_cols(plan_df)
    if not city_cols:
        return fail_no_cities("R_007", 'סה"כ השקעות מתוכננות לביצוע', cfg)
 
    excel_row = cfg.planned_investments_row_excel
    df_idx = excel_row_to_df_index(excel_row, cfg)
 
    results: List[CheckResult] = []
    for city in city_cols:
        val = plan_df.at[df_idx, city]
        results.append(CheckResult(
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
        ))
    return results
 
 
def check_008_funding_total_and_exists_by_city(plan_df: pd.DataFrame, cfg: PlanConfig) -> List[CheckResult]:
    city_cols = detect_city_cols(plan_df)
    if not city_cols:
        return fail_no_cities("R_008", 'מקורות מימון - סה"כ מקורות תקציב', cfg)
 
    excel_row = cfg.funding_total_row_excel
    df_idx = excel_row_to_df_index(excel_row, cfg)
 
    results: List[CheckResult] = []
    for city in city_cols:
        val = plan_df.at[df_idx, city]
        exists = pd.notna(val) and str(val).strip() != ""
        results.append(CheckResult(
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
        ))
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
 
    row_ws = cfg.water_pipe_rows_excel[0]      # 56
    row_wp = cfg.water_pipe_rows_excel[1]      # 57
    row_sw = cfg.sewer_pipe_row_excel          # 58
 
    idx_ws = excel_row_to_df_index(row_ws, cfg)
    idx_wp = excel_row_to_df_index(row_wp, cfg)
    idx_sw = excel_row_to_df_index(row_sw, cfg)
 
    results: List[CheckResult] = []
 
    def has_value(x: Any) -> bool:
        return pd.notna(x) and str(x).strip() != ""
 
    for city in city_cols:
        v_ws = plan_df.at[idx_ws, city]
        v_wp = plan_df.at[idx_wp, city]
        v_sw = plan_df.at[idx_sw, city]
 
        ok = has_value(v_ws) or has_value(v_wp) or has_value(v_sw)
 
        results.append(CheckResult(
            rule_id=f"R_010_{city}",
            rule_name="דיווח אורכי צנרת (לפחות ערך אחד מתוך 3 שורות)",
            severity=Severity.WARNING,
            sheet_name=cfg.sheet_name,
            row_index=None,
            column_name=str(city),
            key_context=f"rows=56,57,58; city={city}",
            actual_value={"water_steel_row56": v_ws, "water_pvc_row57": v_wp, "sewer_row58": v_sw},
            expected_value="at least one non-empty among rows 56/57/58",
            status=Status.PASS_ if ok else Status.FAIL,
            message=f"row56={v_ws}, row57={v_wp}, row58={v_sw}",
        ))
 
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
 
    row_ws = cfg.water_pipe_rows_excel[0]      # 56
    row_wp = cfg.water_pipe_rows_excel[1]      # 57
    row_sw = cfg.sewer_pipe_row_excel          # 58
 
    idx_ws = excel_row_to_df_index(row_ws, cfg)
    idx_wp = excel_row_to_df_index(row_wp, cfg)
    idx_sw = excel_row_to_df_index(row_sw, cfg)
 
    results: List[CheckResult] = []
 
    def emit(city: str, pipe_type: str, excel_row: int, df_idx: int, val: Any, suffix: str) -> None:
        # This test is "reporting"; keep it PASS unless you want blanks to be FAIL
        status = Status.PASS_
        results.append(CheckResult(
            rule_id=f"R_011_{suffix}_{city}",
            rule_name=f"דיווח אורכי צנרת - {pipe_type}",
            severity=Severity.INFO,
            sheet_name=cfg.sheet_name,
            row_index=df_idx,
            column_name=str(city),
            key_context=f"row={excel_row}; city={city}; type={pipe_type}",
            actual_value=val,
            expected_value="reported value",
            status=status,
            message=f"Value from row {excel_row} for '{city}' ({pipe_type}) = {val}",
        ))
 
    for city in city_cols:
        emit(city, "מים פלדה (PE/פלדה)", row_ws, idx_ws, plan_df.at[idx_ws, city], "WATER_STEEL")
        emit(city, "מים PVC",            row_wp, idx_wp, plan_df.at[idx_wp, city], "WATER_PVC")
        emit(city, "ביוב",               row_sw, idx_sw, plan_df.at[idx_sw, city], "SEWER")
 
    return results
 
 
 
# =========================================================
# Orchestrator + Export
# =========================================================
 
def run_summary_sheet_checks(plan_file: str | Path, kinun_file: str | Path, cfg: Optional[PlanConfig] = None) -> pd.DataFrame:
    cfg = cfg or PlanConfig()
 
    plan_df = load_plan_sheet_with_header_fix(plan_file, cfg)
    utility = extract_utility_from_plan_filename(plan_file)
    kinun_df = load_kinun_reference(kinun_file)
 
    results: List[CheckResult] = []
    results.extend(check_001_kinun_values_rounded(plan_df, kinun_df, utility, cfg))
    results.extend(check_003_asset_ratio_below_100(plan_df, cfg))
    results.extend(check_004_total_program_values(plan_df, cfg))
    results.extend(check_005_min_required_program(plan_df, cfg))
    results.extend(check_006_rehab_upgrade_min_required(plan_df, cfg))
    results.extend(check_007_total_planned_investments_by_city(plan_df, cfg))
    results.extend(check_008_funding_total_and_exists_by_city(plan_df, cfg))
    results.extend(check_010_pipes_any_value(plan_df, cfg))
    results.extend(check_011_pipes_values_by_type(plan_df, cfg))
 
 
    out = pd.DataFrame([r.__dict__ for r in results])
    out.insert(0, "utility_name", utility)
    return out
 
 
def save_results_to_excel(df: pd.DataFrame, output_path: str | Path) -> None:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="All_Checks", index=False)
 
 
# =========================================================
# Main
# =========================================================
 
if __name__ == "__main__":
    plan_path = r"C:\Users\davidbo\Documents\Projects\רשות המים\תכניות השקעה\דוגמאות נתונים\תכנית השקעות וביצוע בפועל 2024-2026 תאגיד הרי נצרת.xlsx"
    kinun_path = r"C:\Users\davidbo\Documents\Projects\רשות המים\תכניות השקעה\ערכי כינון לתכנית השקעות 2024.xlsx"
 
    cfg = PlanConfig(
        sheet_name="סיכום תכנית השקעות",
        header_lookback_rows=6,
    )
 
    # Keep your calibrated asset ratio rows
    cfg.asset_ratio_rows_excel = {"מים": 20, "ביוב": 21, "סה\"כ": 22}
 
    results_df = run_summary_sheet_checks(plan_path, kinun_path, cfg=cfg)
    save_results_to_excel(results_df, "checks_output.xlsx")
 
    print("Validation complete. Results saved to checks_output.xlsx")