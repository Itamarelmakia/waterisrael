# src/water_validation/runner.py
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from pathlib import Path
import re

#from config import PlanConfig
from .config import PlanConfig, KINUN_VALUES_PATH

#from excel_io import load_kinun_reference, load_plan_sheet_with_header_fix
from .excel_io import load_kinun_reference, load_plan_sheet_with_header_fix,load_report_sheet

#from utils import extract_utility_from_plan_filename
from .utils import extract_utility_from_plan_filename

from .checks import (
    # ── Macro summary (R_1–R_4) ──────────────────────────────────────────────
    check_001_kinun_values_rounded,
    check_002_asset_ratio,
    check_003_defined_value_percent,
    check_004_total_program_values,
    # ── Standardised city checks (R_5–R_9) ───────────────────────────────────
    check_005_total_planned_investments_cross_row,
    check_006_sync_budget_sources_missing,
    check_007_sync_budget_deficit,
    check_008_pipe_lengths_water,
    check_009_pipe_lengths_sewer,
    # ── Project-level checks (R_12–R_25) ─────────────────────────────────────
    check_012_project_fields_not_empty,
    check_014_llm_project_funding_classification,
    check_015_invalid_project_names,
    check_016_wells_classification,
    check_018_facility_rehab_upgrade,
    check_019_total_planned_cost_per_project,
    check_020_project_status_planning_report,
    check_021_diameter_jump_matching_row,
    check_023_pipe_cost_rule_of_thumb,
    check_024_short_pipe_projects_ratio,
    check_025_pipe_delimiter_colon_only,
    load_kinun_store,
)

def _parse_rules_arg(rules: str) -> set[str]:
    """
    Accepts:
      - "all"
      - "R_15" / "R_12,R_15"
      - "15" / "12,15"
      - "R15"
    Returns a canonical set like {"R_15","R_12"} or {"all"}.
    """
    s = (rules or "all").strip()
    if s.lower() == "all":
        return {"all"}

    parts = [p.strip() for p in s.split(",") if p.strip()]
    out: set[str] = set()

    for p in parts:
        pu = p.upper()
        if pu.startswith("R_"):
            out.add(pu)
            continue
        if pu.isdigit():
            out.add(f"R_{int(pu)}")
            continue
        m = re.fullmatch(r"R(\d+)", pu)  # allows "R15"
        if m:
            out.add(f"R_{int(m.group(1))}")
            continue
        raise ValueError(f"Invalid --rules value: {p!r}")

    return out


def run_summary_sheet_checks(
    plan_file: str | Path,
    kinun_file: str | Path | None = None,
    cfg: Optional[PlanConfig] = None,
    rules: str = "all",
) -> pd.DataFrame:
    cfg = cfg or PlanConfig()

    plan_df, _info = load_plan_sheet_with_header_fix(plan_file, cfg)

    try:
        utility = extract_utility_from_plan_filename(plan_file)
    except ValueError:
        # Filename does not contain a recognisable city/utility name.
        # Fall back: read cell E1 from "גיליון דיווח" sheet.
        try:
            _fb = pd.read_excel(
                plan_file,
                sheet_name=cfg.report_sheet_name,
                header=None,
                nrows=1,
                usecols="E",
            )
            utility = str(_fb.iloc[0, 0]).strip().replace("_", " ")
            if not utility or utility.lower() == "nan":
                raise ValueError("E1 is empty")
            print(f"[runner] utility name read from {cfg.report_sheet_name}!E1: '{utility}'")
        except Exception as _e2:
            raise ValueError(
                f"Cannot determine utility name: filename parse failed and "
                f"{cfg.report_sheet_name}!E1 fallback failed ({_e2}). "
                f"File: {Path(plan_file).name}"
            )

    print(f"\n=== {Path(plan_file).name} ===")

    kinun_json_path = Path(kinun_file) if kinun_file else KINUN_VALUES_PATH
    kinun_data = load_kinun_store(str(kinun_json_path))
    kinun_store = kinun_data["utilities"]
    kinun_year = kinun_data.get("year")
    selected = _parse_rules_arg(rules)
    run_all = ("all" in selected)

    def want(rule_id: str) -> bool:
        return run_all or (rule_id in selected)

    from .models import Status  # add near imports

    def _is_fail_status(s) -> bool:
        # supports Enum (Status.FAIL) and string ("Fail")
        return s == Status.FAIL or str(s) == "Fail"

    def _kpi_counts(rule_results) -> tuple[int, int]:
        if not rule_results:
            return (0, 0)

        row_idxs = [r.row_index for r in rule_results if getattr(r, "row_index", None) is not None]
        if row_idxs:
            total = len(set(row_idxs))
            fail = len(set(
                r.row_index for r in rule_results
                if _is_fail_status(getattr(r, "status", None)) and getattr(r, "row_index", None) is not None
            ))
            return (fail, total)

        total = len(rule_results)
        fail = sum(1 for r in rule_results if _is_fail_status(getattr(r, "status", None)))
        return (fail, total)


    def _run_rule(rule_id: str, fn):
        """
        Runs a rule, extends `results`, prints KPI line.
        `fn` must be a zero-arg callable returning List[CheckResult].
        """
        if not want(rule_id):
            return

        rule_results = fn()
        fail, total = _kpi_counts(rule_results)
        print(f"{rule_id}: {fail}/{total} FAIL")
        results.extend(rule_results)


    results = []
    print("Validation rules: R_1–R_9, R_12–R_25 (R_10,R_11 disabled)")

    # ── Macro summary ─────────────────────────────────────────────────────────
    _run_rule("R_1", lambda: check_001_kinun_values_rounded(plan_df, kinun_store, utility, cfg, kinun_year=kinun_year))
    _run_rule("R_2", lambda: check_002_asset_ratio(plan_df, cfg))
    _run_rule("R_3", lambda: check_003_defined_value_percent(plan_df, cfg))
    _run_rule("R_4", lambda: check_004_total_program_values(plan_df, cfg))
    # ── Standardised city checks ──────────────────────────────────────────────
    _run_rule("R_5", lambda: check_005_total_planned_investments_cross_row(plan_df, cfg))
    _run_rule("R_6", lambda: check_006_sync_budget_sources_missing(plan_df, cfg))
    _run_rule("R_7", lambda: check_007_sync_budget_deficit(plan_df, cfg))
    _run_rule("R_8", lambda: check_008_pipe_lengths_water(plan_df, cfg))
    _run_rule("R_9", lambda: check_009_pipe_lengths_sewer(plan_df, cfg))
    # R_10 and R_11 disabled — removed from active rule set

    report_df = load_report_sheet(
        str(plan_file),
        sheet_name=cfg.report_sheet_name,
        header_row=cfg.report_header_row,
    )

    # ── Project-level checks ──────────────────────────────────────────────────
    _run_rule("R_12", lambda: check_012_project_fields_not_empty(report_df, cfg))
    _run_rule("R_14", lambda: check_014_llm_project_funding_classification(report_df, cfg, utility_name=utility))
    _run_rule("R_15", lambda: check_015_invalid_project_names(report_df, cfg))
    _run_rule("R_16", lambda: check_016_wells_classification(report_df, cfg))
    _run_rule("R_18", lambda: check_018_facility_rehab_upgrade(report_df, cfg))
    _run_rule("R_19", lambda: check_019_total_planned_cost_per_project(report_df, cfg))
    _run_rule("R_20", lambda: check_020_project_status_planning_report(report_df, cfg))
    _run_rule("R_21", lambda: check_021_diameter_jump_matching_row(report_df, cfg))
    _run_rule("R_23", lambda: check_023_pipe_cost_rule_of_thumb(report_df, cfg))
    _run_rule("R_24", lambda: check_024_short_pipe_projects_ratio(report_df, cfg))
    _run_rule("R_25", lambda: check_025_pipe_delimiter_colon_only(report_df, cfg))

    # ---- End rules ----


    records = [r.to_record() for r in results]
    out = pd.DataFrame.from_records(records)

    # guarantee base columns even if empty
    base_cols = ["rule_id", "rule_name", "severity", "sheet_name", "status", "message",
                "row_index", "column_name", "key_context", "actual_value", "expected_value",
                "confidence", "method", "excel_cells"]
    for c in base_cols:
        if c not in out.columns:
            out[c] = ""

    out.insert(0, "utility_name", utility)
    out.insert(1, "plan_file", Path(plan_file).name)
    return out
