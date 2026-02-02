# src/water_validation/runner.py
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from pathlib import Path
import re

DEFAULT_KINUN_JSON = Path(__file__).resolve().parents[2] / "baseline" / "kinun_values_2024.json"

#from config import PlanConfig
from .config import PlanConfig

#from excel_io import load_kinun_reference, load_plan_sheet_with_header_fix
from .excel_io import load_kinun_reference, load_plan_sheet_with_header_fix,load_report_sheet

#from utils import extract_utility_from_plan_filename
from .utils import extract_utility_from_plan_filename

from .checks import (
    check_001_kinun_values_rounded,
    check_rule02_03_asset_ratio,
    check_004_total_program_values,
    check_005_min_required_program,
    check_006_rehab_upgrade_min_required,
    check_007_total_planned_investments_by_city,
    check_008_funding_total_and_exists_by_city,
    check_010_pipes_any_value,
    check_011_pipes_values_by_type,
    check_012_project_fields_not_empty,
    check_014_llm_project_funding_classification,
    check_015_invalid_project_names,
    check_018_facility_rehab_upgrade,
    check_019_total_planned_cost_per_project,
    check_020_project_status_planning_report,
    check_023_pipe_cost_rule_of_thumb,
    check_024_short_pipe_projects_ratio,
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
    utility = extract_utility_from_plan_filename(plan_file)
    print(f"\n=== {Path(plan_file).name} ===")

    kinun_json_path = Path(kinun_file) if kinun_file else DEFAULT_KINUN_JSON
    kinun_store = load_kinun_store(str(kinun_json_path))
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

    # ---- Summary sheet rules ----
    _run_rule("R_1",   lambda: check_001_kinun_values_rounded(plan_df, kinun_store, utility, cfg))
    _run_rule("R_2_3", lambda: check_rule02_03_asset_ratio(plan_df, cfg))
    _run_rule("R_4",   lambda: check_004_total_program_values(plan_df, cfg))
    _run_rule("R_5",   lambda: check_005_min_required_program(plan_df, cfg))
    _run_rule("R_6",   lambda: check_006_rehab_upgrade_min_required(plan_df, cfg))
    _run_rule("R_7",   lambda: check_007_total_planned_investments_by_city(plan_df, cfg))
    _run_rule("R_8",   lambda: check_008_funding_total_and_exists_by_city(plan_df, cfg))
    _run_rule("R_10",  lambda: check_010_pipes_any_value(plan_df, cfg))
    _run_rule("R_11",  lambda: check_011_pipes_values_by_type(plan_df, cfg))

    report_df = load_report_sheet(
        str(plan_file),
        sheet_name=cfg.report_sheet_name,
        header_row=cfg.report_header_row,
    )

    # ---- Report sheet rules ----
    _run_rule("R_12", lambda: check_012_project_fields_not_empty(report_df, cfg))
    _run_rule("R_15", lambda: check_015_invalid_project_names(report_df, cfg))
    _run_rule("R_18", lambda: check_018_facility_rehab_upgrade(report_df, cfg))
    _run_rule("R_19", lambda: check_019_total_planned_cost_per_project(report_df, cfg))
    _run_rule("R_20", lambda: check_020_project_status_planning_report(report_df, cfg))

    _run_rule("R_14", lambda: check_014_llm_project_funding_classification(report_df, cfg))
    _run_rule("R_23", lambda: check_023_pipe_cost_rule_of_thumb(report_df, cfg))
    _run_rule("R_24", lambda: check_024_short_pipe_projects_ratio(report_df, cfg))

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
