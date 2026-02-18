#!/usr/bin/env python3
"""
Run the real validation pipeline on a specific Excel file.
Uses the same logic as the CLI/runner: no mocks, real context and config.
"""
import sys
import os
from pathlib import Path

# Add src to path so we can import the package when run from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__) or os.getcwd(), "src"))

from water_validation.runner import run_summary_sheet_checks
from water_validation import config
from water_validation.report import build_executive_summary, build_summary_table, format_all_checks_for_export

# Specific file to validate (relative to project root)
PLAN_FILENAME = "נספח 1 - דיווח שנתי - תוכנית השקעות ביצוע בפועל וגריעת נכסים_דיווח 4092_מי רעננה_2026_תכנית השקעות.xlsx"


def main() -> None:
    project_root = Path(__file__).resolve().parent
    plan_path = project_root / "data" / PLAN_FILENAME

    if not plan_path.exists():
        print(f"❌ File not found: {plan_path}")
        print("Please ensure the file exists under data/ in the project root.")
        sys.exit(1)

    print("🚀 Running real validation (runner logic, config.KINUN_VALUES_PATH)...")
    print(f"   Plan: {plan_path.name}")
    print(f"   Kinun: {config.KINUN_VALUES_PATH}")

    cfg = config.PlanConfig()
    results_df = run_summary_sheet_checks(
        plan_path,
        kinun_file=config.KINUN_VALUES_PATH,
        cfg=cfg,
        rules="all",
    )

    # Console report: summary tables + full checks
    headline, counts, fails, top_rules = build_executive_summary(results_df)
    summary_table = build_summary_table(results_df)

    print("\n" + "=" * 60 + " EXECUTIVE SUMMARY " + "=" * 60)
    print(headline.to_string(index=False))
    print("\n--- Summary table (sample) ---")
    print(summary_table.head(20).to_string(index=False))
    print("\n--- All check results ---")
    export_df = format_all_checks_for_export(results_df)
    print(export_df.to_string(index=False))

    n_fail = len(results_df[results_df["status"].astype(str).str.contains("נכשל|Fail", na=False)])
    print(f"\nDone. Total results: {len(results_df)}, failing: {n_fail}")


if __name__ == "__main__":
    main()
