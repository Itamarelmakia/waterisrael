from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Sequence, Optional
import warnings
import re

#from config import InputDiscoveryConfig, PlanConfig
from .config import InputDiscoveryConfig, PlanConfig

#from excel_io import discover_inputs
from .excel_io import discover_inputs


#from runner import run_summary_sheet_checks
from .runner import run_summary_sheet_checks

#from report import build_executive_summary, build_summary_table, format_all_checks_for_export
from .report import build_executive_summary, build_summary_table, format_all_checks_for_export


PROJECT_ROOT = Path(__file__).resolve().parents[2]  # .../waterisrael
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

def setup_logging(verbose: bool) -> None:
    level = logging.INFO if verbose else logging.WARNING
    logging.basicConfig(level=level, format="%(levelname)s %(name)s: %(message)s")


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Validate Water Authority investment plan Excels.")

    parser.add_argument(
        "--input-dir",
        type=str,
        default=str(Path.cwd()),
        help="Folder containing plan files (default: current folder)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="validation_output.xlsx",
        help="Output Excel filename/path",
    )
    parser.add_argument(
    "--disable-llm",
    action="store_true",
    help="Disable LLM fallback (LLM is enabled by default)",
    )

    parser.add_argument(
        "--rules",
        type=str,
        default="all",
        help='Which rules to run. Examples: "all" | "R_15" | "R_12,R_15" | "12,15"',
)


    # NEW: allow overriding kinun JSON path (optional)
    default_kinun_json = Path(__file__).resolve().parents[2] / "baseline" / "kinun_values_2024.json"
    parser.add_argument(
        "--kinun-json",
        type=str,
        default=str(default_kinun_json),
        help=f"Path to kinun JSON baseline (default: {default_kinun_json})",
    )

    parser.add_argument("--verbose", action="store_true", help="Enable INFO logs")

    args = parser.parse_args(argv)

    import warnings
    warnings.filterwarnings(
        "ignore",
        message=r"Data Validation extension is not supported and will be removed",
        category=UserWarning,
    )

    setup_logging(args.verbose)

    input_dir = Path(args.input_dir)

    rules_arg = (args.rules or "all").strip()


    # Decide output filename automatically *only* if user kept the default
    if args.output == "validation_output.xlsx":
        suffix = _rules_suffix(rules_arg)

        if rules_arg.lower() == "all":
            output_filename = "validation_output.xlsx"
        else:
            output_filename = f"validation_output_debug{suffix}.xlsx"

        output_path = OUTPUTS_DIR / output_filename
    else:
        # if user passed an explicit output, respect it
        output_path = Path(args.output)



    kinun_json_path = Path(args.kinun_json)


    if not kinun_json_path.exists():
        raise FileNotFoundError(f"Kinun JSON not found: {kinun_json_path}")

    disco = InputDiscoveryConfig()
    plan_files, _kinun_excel_file = discover_inputs(input_dir, disco)  # keep discovery, ignore XLSX

    cfg = PlanConfig(sheet_name="סיכום תכנית השקעות", header_lookback_rows=6)
    cfg.asset_ratio_rows_excel = {"מים": 20, "ביוב": 21, "סה\"כ": 22}

    # LLM is enabled by default (from config.py), disable only if flag is passed
    if args.disable_llm:
        cfg.llm_enabled = False

    all_dfs = []
    for plan_path in plan_files:
        # IMPORTANT: pass JSON path (not excel)
        df = run_summary_sheet_checks(plan_path, kinun_json_path, cfg=cfg, rules=rules_arg)

        all_dfs.append(df)

    import pandas as pd
    all_checks = pd.concat(all_dfs, ignore_index=True)
    all_checks_export = format_all_checks_for_export(all_checks)

    headline, counts, fails, top_rules = build_executive_summary(all_checks)
    summary_table = build_summary_table(all_checks)




    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        summary_table.to_excel(writer, sheet_name="Summary_Table", index=False)
        headline.to_excel(writer, sheet_name="Executive_Summary", index=False)
        fails.to_excel(writer, sheet_name="Fails_By_File", index=False)
        counts.to_excel(writer, sheet_name="Counts", index=False)
        top_rules.to_excel(writer, sheet_name="Top_Failing_Rules", index=False)
        all_checks_export.to_excel(writer, sheet_name="All_Checks", index=False)


    # אחרי שהקובץ נשמר וסגור:
    #from excel_io import apply_red_highlights
    from .excel_io import apply_red_highlights

    apply_red_highlights(str(output_path), all_checks_export)

    print(f"Validation complete. Output saved to: {output_path.resolve()}")
    return 0



def _rules_suffix(rules_arg: str | None) -> str:
    if not rules_arg:
        return ""
    r = rules_arg.strip().lower()
    if r in {"all", "*"}:
        return ""
    # support "R_15" or "R_12,R_15" or "R_12 R_15"
    parts = [p.strip() for p in re.split(r"[,\s]+", rules_arg.strip()) if p.strip()]
    # normalize order for stable file names
    parts = sorted(parts)
    return "_" + "_".join(parts)


if __name__ == "__main__":
    raise SystemExit(main())
