# src/water_validation/report.py
from __future__ import annotations

import re
import json
import pandas as pd

from pathlib import Path
# --- RULE DETAILS (פירוט הבדיקה) ---
RULE_DETAILS = {
    "R_1": "השוואת ערכי כינון לפי קובץ השקעות וביצוע לבין ערכי הכינון",
    "R_2_3": "ערך מצטבר - גריעת נכסים",
    "R_3": "לציין את הערך שהוגדר",
    "R_4": "שקיים ערך",
    "R_5": "בדיקה שקיים ערך",
    "R_6": "בדיקה שקיים ערך",
    "R_7": "בדיקה שקיים ערך\nהערך צריך לצאת במינוס או 0",
    "R_8": "בדיקה שקיים ערך",
    "R_9": "בדיקה שקיים ערך",
    "R_10": "לכל ישוב - סה\"כ אורכי צנרת מים שדווחו לטיפול השנה מהווים ווה או יותר מ1/35 מסה\"כ צנרת בישוב שמדווחת בסכום תוכנית השקיעות למים (שורות 56 ו/או 57) להגדרת לוגיקת חישוב מדוייקת בהמשך",
    "R_11": "לשאול את איתי למה צריך את זה כי הרבה פעמים זה ריק",
    "R_12": "שדות חובה  שיהיה מלא",
    "R_13": "במידה ומוגדר בעמודה F \"שקום ושדרוג\" ובעמודה G קווי מים או קווי ביוב\nעמודות H עד M צריכות להיות מלאות",
    "R_14": "בדיקת סיווג פרויקט לפי הנחיות",
    "R_15": "שמות הפרויקטים לא יכולים להיות \"רחוב\" בלבד, \"בין הבתים\", \"שטח פתוח\", \"רחוב שכונת\"",
    "R_16": "במידה ובשם הפרויקט מופיע \"באר\" או \"קידוח\"  -  סיווג הפרויקט צריך להיות \"קידוח\"",
    "R_17": "בדיקה לפי שם הפרויקט האם הוא עומד בתנאים של שיקום/שדרוג / פיתוח והאם הוא עומד בתנאים להיחשב כהשקעה. אם הסיווג אינו מתאים יש לתת על כך הערה לבודק.",
    "R_18": "עמודה N צריכה להכיל שנת הקמה תקינה\nעמודה P צריכה להיות מלאה\nצריך היות מסומן X אחד לפחות בעמודות Q עד U",
    "R_19": "AE >= X+AA+AD",
    "R_20": "עמודה AH צריכה מלאה",
    "R_21": "במידה ובסיווג פרויקט \"שיקום ושדרוג\" הקפיצה בקוטר מקיים (I) למתוכן (L) היא ביותר מקפיצה אחת (מדרגות הקפיצה לפי קוטר ווחומר מפורט בגליון חומרים וקטרים)\nעל צריכה להיות שורה נוספת עם שם פרוייקט זהה (וקוד פרויקט שונה) עם אותם פרטים בדיוק .\nלכל שורה יועמסו אחוזי פרויקט שונים בעמודות X,AA,AD וAE - לפי מחשבון \"כלי עזר\"",
    "R_22": "בדיקה שסה\"כ מחיר הצנרת אשר יחושב בהתאם למחירי התקן ולפי הנתונים של חומר מבנה, הקוטר והאורך של הקווים אינו עולה על סכום סה\"כ אומדן הפרויקט.",
    "R_23": "בדיקה שסה\"כ מחיר הצנרת אשר יחושב בהתאם למחירי התקן ולפי הנתונים של חומר מבנה, הקוטר והאורך של הקווים אינו עולה על סכום סה\"כ אומדן הפרויקט.",
    "R_24": "סך כל הפרויקטים שאורך הצנרת (עמודה M ) קטן מ 100 לא יעלה על 5% מסה\"כ אומדן הפרויקטים\nעמודה AI - צריכה להכיל הסבר",
    "R_25": "",
    "R_26": "בדיקה שסכום התכנון גבוה מהמינימום הנדרש לפי סיווג הפרויקט",
    "R_27": "בדיקת אם קיימת חריגה של מעל ל- 10% בין הביצוע בפועל לתכנון.",
    "R_28": "בדיקה שהקוטר הכי גדול שבוצע אינו שונה מהכי גדול שתוכנן",
    "R_29": "הפירוט הקיים בתכנון צריך להיות זהה לפירוט הקיים בביצוע",
    "R_30": "כאשר יש ביצוע בפועל לא יתכן שהפרויקט יהיה בסטטוס 'מוקפא'",
    "R_31": "אם הוציאו כסף על הפרויקט בפועל והסכומים שווים או גדולים מהסכום שתוכנן אז סטטוס הפרויקט צריך להיות 'הסתיים' (Q1-Q4) / 'ביצוע'",
    "R_32": "השוואת תוכנית ההשקעה של שנה מסוימת בין הקבצים שהתקבלו בה מופיעה אותה שם כדי לבדוק שאין שינוי בנתונים",
    "R_33": "בדיקה שסך ההוצאות בפועל מדוחות של שנים קודמות תואמות לסכום המצטבר בדוח האחרון",
    "R_34": "שם פרויקט שהופיע בעבר בסטטוס 'הסתיים' אינו יכול לחזור על עצמו בפרק זמן כשלהו",
    "R_35": "לזהות שיש 2 חתימות על גבי הקובץ",
    "R_36": "בדיקה לפי שם הפרויקט האם הוא עומד בתנאים של שיקום/שדרוג / פיתוח והאם הוא עומד בתנאים להיחשב כהשקעה. אם הסיווג אינו מתאים יש לתת על כך הערה לבודק. ",
}

RENAME_MAP = {
    "utility_name": "רשות מקומית",
    "plan_file": "שם הקובץ",
    "sheet_name": "לשונית באקסל",
    "rule_id": "מפתח בדיקה",
    "rule_name": "שם הבדיקה",
    "severity": "חומרת בדיקה",
    "actual_value": "ערך בפועל",
    "confidence": "ביטחון",
    "method": "שיטה",
    "expected_value": "ערך צפוי",
    "status": "סטטוס",
    "column_name": "עמודה באקסל",
    "key_context": "בדיקה שבוצעה",
    "message": "הערות מערכת",
}

FINAL_COLS = [
    "רשות מקומית",
    "שם הקובץ",
    "לשונית באקסל",
    "מפתח בדיקה",
    "מזהה בדיקה",
    "שם הבדיקה",
    "רמת בדיקה",
    "חומרת בדיקה",
    "ערך בפועל",
    "ערך צפוי",
    "ביטחון",
    "סטטוס",
    "עמודה באקסל",
    "בדיקה שבוצעה",
    "הערות מערכת",
    "פירוט הבדיקה",
]

import re

def _extract_check_id(rule_id: str) -> str:
    """
    Extract short check id from rule_id without leading zeros.

    Works for:
      "R_1_ערך כינון מלא_מים"   -> "R_1"
      "R_001_ערך כינון..."      -> "R_1"
      "R_2_3_משהו"              -> "R_2_3"
      "R_2_03_משהו"             -> "R_2_3"
      "R_10_..."                -> "R_10"
    """
    if rule_id is None:
        return ""

    s = str(rule_id).strip()
    if not s.startswith("R_"):
        return ""

    parts = [p.strip() for p in s.split("_") if p.strip()]
    # parts example: ["R", "1", "ערך כינון מלא", "מים"]
    if len(parts) < 2:
        return ""

    # parts[1] must be numeric
    if not parts[1].isdigit():
        return ""

    first = int(parts[1])  # removes leading zeros

    # optional second numeric token (for rules like R_2_3_...)
    if len(parts) >= 3 and parts[2].isdigit():
        second = int(parts[2])
        return f"R_{first}_{second}"

    return f"R_{first}"



def _extract_level(rule_id: str) -> str:
    # take the last token after '_' (e.g., מים / מים פלדה)
    if rule_id is None:
        return ""
    parts = [p.strip() for p in str(rule_id).split("_") if p.strip()]
    if len(parts) < 2:
        return ""
    last = parts[-1]
    if re.fullmatch(r"\d+", last):
        return ""
    return last


def _extract_checked_cols_from_key_context(s: str) -> str:
    # expects "... | checked_cols=שם פרויקט / מיקום פרויקט / סיווג פרויקט"
    if not s:
        return ""
    s = str(s)
    marker = "checked_cols="
    if marker not in s:
        return ""
    return s.split(marker, 1)[1].strip()

def format_all_checks_for_export(all_checks: pd.DataFrame) -> pd.DataFrame:
    # Handle empty results (e.g., running a single rule that produced no FAILs)
    if all_checks is None or len(all_checks) == 0:
        return pd.DataFrame(columns=FINAL_COLS)

    df = all_checks.copy()

    # If df exists but has no columns (pd.DataFrame([]) case)
    if df.empty and len(df.columns) == 0:
        return pd.DataFrame(columns=FINAL_COLS)

    # Ensure required base columns exist before accessing them
    if "rule_id" not in df.columns:
        df["rule_id"] = ""
    if "key_context" not in df.columns:
        df["key_context"] = ""

    # new cols from original fields
    df["מזהה בדיקה"] = df["rule_id"].apply(_extract_check_id)
    df["רמת בדיקה"] = df["rule_id"].apply(_extract_level)

    # ✅ OVERRIDE לרול 12: רמת בדיקה = העמודות שנבדקו
    mask12 = df["מזהה בדיקה"].eq("R_12")
    df.loc[mask12, "רמת בדיקה"] = df.loc[mask12, "key_context"].apply(_extract_checked_cols_from_key_context)

    df["פירוט הבדיקה"] = df["מזהה בדיקה"].map(RULE_DETAILS).fillna("")
    
    # rename to Hebrew
    df = df.rename(columns=RENAME_MAP)


    # status to Hebrew
    df["סטטוס"] = df["סטטוס"].replace({
        "Pass": "עבר",
        "Fail": "נכשל",
        "Not applicable": "לא רלוונטי",
    })

    # low confidence flag
    conf = pd.to_numeric(df.get("ביטחון", pd.Series([None]*len(df))), errors="coerce")
    mask_low = df["סטטוס"].eq("נכשל") & conf.notna() & (conf < 0.50)
    df.loc[mask_low, "סטטוס"] = "נכשל (ביטחון נמוך)"

    # ensure columns exist
    for c in FINAL_COLS:
        if c not in df.columns:
            df[c] = ""

    df = df[FINAL_COLS]
    return df


def build_executive_summary(all_checks_df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a compact summary per utility + file:
      - counts by severity + status
      - total fails
    """
    df = all_checks_df.copy()

    # Basic pivots
    counts = (
        df.groupby(["utility_name", "plan_file", "severity", "status"])
          .size()
          .reset_index(name="count")
    )

    # Total FAILS
    fails = (
        df[df["status"] == "Fail"]
        .groupby(["utility_name", "plan_file"])
        .size()
        .reset_index(name="total_fails")
    )

    # Top failing rules (optional but helpful)
    top_rules = (
        df[df["status"] == "Fail"]
        .groupby(["utility_name", "plan_file", "rule_id", "rule_name"])
        .size()
        .reset_index(name="fail_count")
        .sort_values(["utility_name", "plan_file", "fail_count"], ascending=[True, True, False])
    )

    # Keep top 5 rules per file
    top_rules["rank"] = top_rules.groupby(["utility_name", "plan_file"]).cumcount() + 1
    top_rules = top_rules[top_rules["rank"] <= 5].drop(columns=["rank"])

    # Merge a compact “headline” summary
    headline = (
        df.groupby(["utility_name", "plan_file"])
          .agg(
              total_checks=("rule_id", "count"),
              critical_fails=("severity", lambda s: ((df.loc[s.index, "severity"] == "Critical") & (df.loc[s.index, "status"] == "Fail")).sum()),
          )
          .reset_index()
    )

    # Return multiple tables? easiest: return headline, and you’ll export others too.
    return headline, counts, fails, top_rules




def build_summary_table(all_checks_df: pd.DataFrame) -> pd.DataFrame:
    """
    Summary_Table (Hebrew):
    שם קובץ | שם הבדיקה | פירוט הבדיקה | מיקום הבדיקה | ממצאים | סטטוס | הערות | הערת משתמש | פעולה נדרשת מול התאגיד
    """

    FINAL_COLS = [
        "שם קובץ",
        "שם הבדיקה",
        "פירוט הבדיקה",
        "מיקום הבדיקה",
        "ממצאים",
        "סטטוס",
        "הערות",
        "הערת משתמש",
        "פעולה נדרשת מול התאגיד",
    ]

    if all_checks_df is None or len(all_checks_df) == 0:
        return pd.DataFrame(columns=FINAL_COLS)

    df = all_checks_df.copy()


    # --- Normalize inputs from both raw results and Hebrew-export results ---
    # If df already came from format_all_checks_for_export(), it may have Hebrew column names.
    if "plan_file" not in df.columns and "שם הקובץ" in df.columns:
        df["plan_file"] = df["שם הקובץ"]

    if "rule_name" not in df.columns and "שם הבדיקה" in df.columns:
        df["rule_name"] = df["שם הבדיקה"]

    if "rule_id" not in df.columns:
        if "מזהה בדיקה" in df.columns:
            df["rule_id"] = df["מזהה בדיקה"]
        elif "מפתח בדיקה" in df.columns:
            df["rule_id"] = df["מפתח בדיקה"]

    if "sheet_name" not in df.columns and "לשונית באקסל" in df.columns:
        df["sheet_name"] = df["לשונית באקסל"]

    if "status" not in df.columns and "סטטוס" in df.columns:
        # reverse-map Hebrew statuses back to English-like tokens used in KPI logic
        df["status"] = df["סטטוס"].replace({
            "עבר": "Pass",
            "נכשל": "Fail",
            "לא רלוונטי": "Not applicable",
            "נכשל (ביטחון נמוך)": "Fail",
        })


    # Safety: guarantee columns exist
    needed = [
        "plan_file",
        "rule_id",
        "rule_name",
        "sheet_name",
        "status",
        "column_name",
        "row_index",
        "key_context",
        "excel_cells",
    ]

    # If rule_id missing but check_id exists under another common column, recover it
    if df["rule_id"].isna().all() and "מזהה בדיקה" in df.columns:
        df["rule_id"] = df["מזהה בדיקה"]


    for c in needed:
        if c not in df.columns:
            df[c] = None

    # --- Defensive: always have a usable plan_file / file name ---
    if df["plan_file"].isna().all():
        df["plan_file"] = ""

    # --- Defensive: always have 'שם קובץ' even in API flows ---
    if "שם קובץ" not in df.columns or df["שם קובץ"].isna().all():
        df["שם קובץ"] = df["plan_file"].apply(lambda x: Path(str(x)).stem if str(x).strip() else "")


    # Build grouping keys
    df["check_id"] = df["rule_id"].apply(_extract_check_id)
    df = df[df["check_id"].astype(str).str.strip().ne("")].copy()
    df["שם קובץ"] = df["plan_file"].apply(lambda x: Path(str(x)).stem)

    def _pick_first_nonempty(series: pd.Series) -> str:
        for x in series.dropna():
            t = str(x).strip()
            if t and t.lower() != "nan":
                return t
        return ""

    def _status_is_fail(v: object) -> bool:
        return str(v).strip().lower() == "fail"

    # Same KPI logic as the CLI printing logic
    def _kpi_counts(gdf: pd.DataFrame) -> tuple[int, int]:
        row_idxs = [int(x) for x in gdf["row_index"].dropna().tolist() if str(x).strip().lower() != "nan"]
        if row_idxs:
            total = len(set(row_idxs))
            fail = len(set(
                int(r) for r, s in zip(gdf["row_index"].tolist(), gdf["status"].tolist())
                if r is not None and str(r).strip().lower() != "nan" and _status_is_fail(s)
            ))
            return fail, total

        total = len(gdf)
        fail = sum(1 for s in gdf["status"].tolist() if _status_is_fail(s))
        return fail, total

    def _compact_int_ranges(nums: list[int]) -> str:
        nums = sorted({int(n) for n in nums if n is not None})
        if not nums:
            return ""
        out = []
        start = prev = nums[0]
        for n in nums[1:]:
            if n == prev + 1:
                prev = n
                continue
            out.append(f"{start}-{prev}" if start != prev else f"{start}")
            start = prev = n
        out.append(f"{start}-{prev}" if start != prev else f"{start}")
        return ",".join(out)

    def _rows_from_group(gdf: pd.DataFrame) -> str:
        rows: list[int] = []

        # Prefer explicit Excel cell references (Sheet!B12)
        for v in gdf["excel_cells"].dropna().tolist():
            cells = v if isinstance(v, list) else None
            if cells is None:
                # sometimes stored as a stringified list
                if isinstance(v, str) and v.strip().startswith("[") and v.strip().endswith("]"):
                    try:
                        cells = json.loads(v)
                    except Exception:
                        cells = None
            if not cells:
                continue

            for cell in cells:
                if not cell:
                    continue
                try:
                    addr = str(cell).split("!", 1)[1]
                except Exception:
                    addr = str(cell)
                m = re.search(r"[A-Za-z]+(\d+)", addr)
                if m:
                    rows.append(int(m.group(1)))

        if rows:
            return _compact_int_ranges(rows)

        # Fallback: key_context often has "excel_row=<n>"
        for kc in gdf["key_context"].dropna().astype(str).tolist():
            for m in re.finditer(r"excel_row\s*=\s*(\d+)", kc):
                rows.append(int(m.group(1)))

        if rows:
            return _compact_int_ranges(rows)

        # Last resort: df row numbers (1-based)
        idxs = [int(x) + 1 for x in gdf["row_index"].dropna().tolist() if str(x).strip().lower() != "nan"]
        return _compact_int_ranges(idxs)

    def _columns_from_group(gdf: pd.DataFrame) -> str:
        cols = [str(x).strip() for x in gdf["column_name"].dropna().tolist()]
        cols = [c for c in cols if c and c.lower() != "nan"]
        seen = set()
        out = []
        for c in cols:
            if c not in seen:
                seen.add(c)
                out.append(c)
        return " / ".join(out)
    
    def _location_from_group(gdf: pd.DataFrame) -> str:
        sheet = _pick_first_nonempty(gdf["sheet_name"])
        cols = _columns_from_group(gdf)
        rows = _rows_from_group(gdf)

        return "\n".join([
            f"לשונית באקסל: {sheet}" if sheet else "לשונית באקסל:",
            f"עמודות: {cols}" if cols else "עמודות:",
            f"שורות: {rows}" if rows else "שורות:",
        ])


    def _status_rollup(fail: int, total: int) -> str:
        if total <= 0 or fail <= 0:
            return "Pass"
        if fail >= total:
            return "Fail"
        return "Partial Fail"

    def _sort_key(check_id: str) -> tuple[int, int]:
        s = str(check_id).strip()
        s = s[2:] if s.startswith("R_") else s
        parts = s.split("_")
        a = int(parts[0]) if parts and parts[0].isdigit() else 10**9
        b = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else -1
        return (a, b)

    rows_out = []
    for (file_stem, check_id), gdf in df.groupby(["שם קובץ", "check_id"], dropna=False):
        fail, total = _kpi_counts(gdf)
        rows_out.append({
            "שם קובץ": str(file_stem),
            "check_id": str(check_id),  # internal for sorting
            "שם הבדיקה": _pick_first_nonempty(gdf["rule_name"]),
            "פירוט הבדיקה": RULE_DETAILS.get(str(check_id), ""),
            "מיקום הבדיקה": _location_from_group(gdf),
            "ממצאים": f"{check_id}: {fail}/{total} FAIL",
            "סטטוס": _status_rollup(fail, total),
            "הערות": "שדה ריק למילוי המשתמש",
            "הערת משתמש": "שדה ריק למילוי המשתמש",
            "פעולה נדרשת מול התאגיד": "שדה ריק למילוי המשתמש",
        })

    out = pd.DataFrame(rows_out)

    # --- Defensive: guarantee file column exists for sorting ---
    if "שם קובץ" not in out.columns:
        if "שם הקובץ" in out.columns:
            out["שם קובץ"] = out["שם הקובץ"]
        elif "plan_file" in out.columns:
            out["שם קובץ"] = out["plan_file"]
        else:
            out["שם קובץ"] = ""

    # --- Defensive: guarantee check_id exists for sorting ---
    if "check_id" not in out.columns:
        if "מזהה בדיקה" in out.columns:
            out["check_id"] = out["מזהה בדיקה"].astype(str)
        elif "מפתח בדיקה" in out.columns:
            out["check_id"] = out["מפתח בדיקה"].astype(str)
        elif "rule_id" in out.columns:
            out["check_id"] = out["rule_id"].astype(str)
        else:
            out["check_id"] = ""

    out = out.sort_values(
        by=["שם קובץ", "check_id"],
        key=lambda s: s.map(_sort_key) if s.name == "check_id" else s,
        kind="mergesort",
    ).reset_index(drop=True)



    out = out.drop(columns=["check_id"], errors="ignore")

    for c in FINAL_COLS:
        if c not in out.columns:
            out[c] = ""
    return out[FINAL_COLS]
