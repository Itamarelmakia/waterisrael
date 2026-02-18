import sys
import os
from pathlib import Path
import json
# Add src to path so we can import internal modules
sys.path.append(os.path.join(os.getcwd(), "src"))

from water_validation import excel_io, config, checks

def main():
    print("🚀 Starting validation for 2026 data...")

    # 1. Define the specific Hebrew filename
    filename = "נספח 1 - דיווח שנתי - תוכנית השקעות ביצוע בפועל וגריעת נכסים_דיווח 4092_מי רעננה_2026_תכנית השקעות.xlsx"
    # Check if file is in 'data' folder or root
    file_path = Path("data") / filename
    if not file_path.exists():
        file_path = Path(filename)

    if not file_path.exists():
        print(f"❌ File not found: {filename}")
        print("Please make sure the file is in the 'data' folder or root directory.")
        return

    print(f"📂 Found Excel: {filename}")
    # 2. Load the Excel Sheet
    try:
        print("⏳ Loading Excel sheet...")
        plan_cfg = config.PlanConfig()  # sheet_name default is "סיכום תכנית השקעות"
        df, _ = excel_io.load_plan_sheet_with_header_fix(file_path, plan_cfg)
        print("✅ Excel loaded successfully.")
    except Exception as e:
        print(f"❌ Excel error: {e}")
        return

    # 3. Load 2026 Values from Config
    print(f"📂 Loading Config path: {config.KINUN_VALUES_PATH}")
    try:
        with open(config.KINUN_VALUES_PATH, 'r', encoding='utf-8') as f:
            kinun_data = json.load(f)
        print("✅ Kinun 2026 data loaded.")
    except Exception as e:
        print(f"❌ Failed to load JSON: {e}")
        return
    # 4. Run Checks 1-11
    # Create a context object like the runner does
    class Context:
        def __init__(self):
            self.kinun_values = kinun_data
    ctx = Context()

    checks_list = [
        checks.check_001_kinun_values_rounded,
        checks.check_rule02_03_asset_ratio,
        checks.check_004_total_program_values,
        checks.check_005_min_required_program,
        checks.check_006_rehab_upgrade_min_required,
        checks.check_007_total_planned_investments_by_city,
        checks.check_008_funding_total_and_exists_by_city,
        checks.check_010_pipes_any_value,
        checks.check_011_pipes_values_by_type
    ]

    print("\n--- 🔍 RESULTS ---")
    for check in checks_list:
        try:
            res = check(df, ctx)
            if res.status == "עבר":
                status = "✅ PASS"
            elif res.status == "נכשל":
                status = f"❌ FAIL ({res.message})"
            else:
                status = f"⚠️ {res.status}"

            print(f"{res.rule_id}: {status}")
        except Exception as e:
            print(f"💥 Error running {check.__name__}: {e}")

if __name__ == "__main__":
    main()
