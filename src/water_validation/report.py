# src/water_validation/report.py
from __future__ import annotations

import re
import json
import pandas as pd

from pathlib import Path
# --- RULE DETAILS (פירוט הבדיקה) ---
RULE_DETAILS = {
    "R_1": "השוואת ערכי כינון לפי קובץ השקעות וביצוע לבין ערכי הכינון",
    "R_2": "יחס נכסים",
    "R_2_3": "ערך מצטבר - גריעת נכסים",
    "R_3": "לציין את הערך שהוגדר - ערך קיים ובטווח 0–100% (עמודה R שורות 20–22)",
    # R_4 is split in the summary table into:
    # - R_4_1 (4.1): מינימום נדרש + שיקום ושדרוג (עמודה R, שורות 25–30)
    # - R_4_2 (4.2): יחס (עמודה S, שורות 28–30)
    "R_4": "שקיים ערך",
    "R_4_1": "שקיים ערך",
    "R_4_2": "בדיקה שקיימים ערכים והיחס גדול מ 100%",
    # Backwards compatibility (older exports)
    "R_4_יחס": "בדיקה שקיימים ערכים והיחס גדול מ 100%",
    "R_5": "דיווח סה\"כ השקעות מתוכננות לביצוע: לכל ישוב מדווח ברשות מקומית חייב להיות ערך לא ריק ולא 0 ב־סה\"כ השקעה מתוכננת (בטווח דוח סיכום תכנית השקעות)",
    "R_6": "בדיקת סנכרון השקעות ומקורות מימון: לכל יישוב בשורה 4 חייב להיות ערך מקורות תקציב (שורה 50) לא ריק ולא 0",
    "R_7": "בדיקת סנכרון השקעות ומקורות מימון: מקורות המימון (שורה 50) חייבים לכסות את ההשקעה המתוכננת (שורה 39) לכל יישוב",
    "R_8": "דיווח אורכי צנרת מים: לכל יישוב (מלבד כפר סבא) חייב להיות ערך לא ריק ולא 0 בשורה 56 או 57",
    "R_9": "דיווח אורכי צנרת ביוב: לכל יישוב (מלבד כפר סבא) חייב להיות ערך לא ריק ולא 0 בשורה 58",
    "R_10": "לכל ישוב - סה\"כ אורכי צנרת מים שדווחו לטיפול השנה מהווים ווה או יותר מ1/35 מסה\"כ צנרת בישוב שמדווחת בסכום תוכנית השקיעות למים (שורות 56 ו/או 57) להגדרת לוגיקת חישוב מדוייקת בהמשך",
    "R_11": "לשאול את איתי למה צריך את זה כי הרבה פעמים זה ריק",
    "R_12": "שדות חובה  שיהיה מלא",
    "R_13": "במידה ומוגדר בעמודה F \"שקום ושדרוג\" ובעמודה G קווי מים או קווי ביוב\nעמודות H עד M צריכות להיות מלאות",
    "R_14": "בדיקת סיווג פרויקט לפי הנחיות",
    "R_15": "שמות הפרויקטים לא יכולים להיות \"רחוב\" בלבד, \"בין הבתים\", \"שטח פתוח\", \"רחוב שכונת\"",
    "R_16": "במידה ובשם הפרויקט מופיע \"באר\" או \"קידוח\"  -  סיווג הפרויקט צריך להיות \"קידוח\"",
    "R_17": "בדיקה לפי שם הפרויקט האם הוא עומד בתנאים של שיקום/שדרוג / פיתוח והאם הוא עומד בתנאים להיחשב כהשקעה. אם הסיווג אינו מתאים יש לתת על כך הערה לבודק.",
    "R_18": (
        "עמודה N צריכה להכיל שנת הקמה תקינה (אם זה פרויקט שהקוד ההנדסי שלו הוא מתקני מים /מתקני ביוב/ קידוחים\n"
        "עמודות O & P צריכות להיות מלאות\n"
        "צריך היות מסומן X אחד לפחות בעמודות Q עד U"
    ),
    "R_19": "AE >= X+AA+AD",
    "R_20": "עמודה AH צריכה מלאה",
    "R_21": "במידה ובסיווג פרויקט \"שיקום ושדרוג\" הקפיצה בקוטר מקיים (I) למתוכן (L) היא ביותר מקפיצה אחת (מדרגות הקפיצה לפי קוטר ווחומר מפורט בגליון חומרים וקטרים)\nעל צריכה להיות שורה נוספת עם שם פרוייקט זהה (וקוד פרויקט שונה) עם אותם פרטים בדיוק .\nלכל שורה יועמסו אחוזי פרויקט שונים בעמודות X,AA,AD וAE - לפי מחשבון \"כלי עזר\"",
    "R_22": "בדיקה שסה\"כ מחיר הצנרת אשר יחושב בהתאם למחירי התקן ולפי הנתונים של חומר מבנה, הקוטר והאורך של הקווים אינו עולה על סכום סה\"כ אומדן הפרויקט.",
    "R_23": "בדיקת מחיר צנרת לפי כלל אצבע: השוואת עלות מחושבת (קוטר×1.2×150) לעלות קבלנית מדווחת (AE×1000/M). רלוונטי רק לקווי מים.",
    "R_24": "סך כל הפרויקטים שאורך הצנרת (עמודה M ) קטן מ 100 לא יעלה על 5% מסה\"כ אומדן הפרויקטים\nעמודה AI - צריכה להכיל הסבר",
    "R_25": "בדיקת מפריד בעמודות צנרת (J,K,L,M): ערכים מרובים חייבים להיות מופרדים רק בנקודתיים (:). שימוש ב-+ / - אסור.",
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
    "שורה באקסל",
    "בדיקה שבוצעה",
    "הערות מערכת",
    "פירוט הבדיקה",
]

import re

# --- Excel cell reference parsing helpers ---
_CELL_RE = re.compile(r"!?([A-Z]{1,3})(\d+)$")  # matches "G8" or "!G8" at end


def _parse_excel_cells(excel_cells) -> list[tuple[str, int]]:
    """
    Parse excel_cells list (e.g. ['גיליון דיווח!G8']) into [(letter, row), ...].
    """
    if not excel_cells:
        return []
    cells = excel_cells
    if isinstance(cells, str):
        try:
            cells = json.loads(cells)
        except Exception:
            cells = [cells]
    if not isinstance(cells, list):
        return []
    out = []
    for cell in cells:
        if not cell:
            continue
        s = str(cell)
        # extract the "G8" part after "!"
        addr = s.split("!", 1)[1] if "!" in s else s
        m = re.match(r"([A-Z]{1,3})(\d+)$", addr.strip())
        if m:
            out.append((m.group(1), int(m.group(2))))
    return out


def _extract_excel_row_from_context(key_context: str) -> str:
    """
    Extract excel row number from key_context patterns like:
    - excel_row=8
    - plan_cell=R8
    - row=39
    - rows=56,57,58
    """
    if not key_context:
        return ""
    s = str(key_context)
    # excel_row=N
    m = re.search(r"excel_row\s*=\s*(\d+)", s)
    if m:
        return m.group(1)
    # plan_cell=R8 or plan_cell=S12
    m = re.search(r"plan_cell\s*=\s*[A-Z]{1,3}(\d+)", s)
    if m:
        return m.group(1)
    # rows=56,57,58 (multiple rows)
    m = re.search(r"\brows\s*=\s*([\d,]+)", s)
    if m:
        return m.group(1)
    # row=39
    m = re.search(r"\brow\s*=\s*(\d+)", s)
    if m:
        return m.group(1)
    return ""


def _extract_col_letter_from_context(key_context: str) -> str:
    """
    Extract column letter from key_context patterns like plan_cell=R8.
    """
    if not key_context:
        return ""
    s = str(key_context)
    m = re.search(r"plan_cell\s*=\s*([A-Z]{1,3})\d+", s)
    if m:
        return m.group(1)
    return ""


def _is_excel_letter(s: str) -> bool:
    """Check if a string is a pure Excel column letter like R, S, M, AE."""
    return bool(re.fullmatch(r"[A-Z]{1,3}", s.strip()))


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

    # Split R_4 into 4.1 and 4.2 in the summary:
    # - R_4_מינימום_* / R_4_שיקום_* -> R_4_1
    # - R_4_יחס_* -> R_4_2
    if first == 4 and len(parts) >= 3:
        token = parts[2]
        if token in ("מינימום", "שיקום"):
            return "R_4_1"
        if token == "יחס":
            return "R_4_2"

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

    # ✅ OVERRIDE לרול 14: רמת בדיקה = שלב ההחלטה (keyword/fuzzy/llm/fail_llm/no_decision)
    mask14 = df["מזהה בדיקה"].eq("R_14")
    if mask14.any() and "method" in df.columns:
        _R14_METHOD_MAP = {
            "keyword": "keyword",
            "fuzzy": "fuzzy",
            "llm": "llm",
            "prior": "fuzzy",       # prior is a deterministic/statistical stage
            "fail_llm": "fail_llm",
            "no_decision": "no_decision",
            "none": "no_decision",  # legacy fallback
        }
        df.loc[mask14, "רמת בדיקה"] = (
            df.loc[mask14, "method"]
            .map(_R14_METHOD_MAP)
            .fillna("no_decision")
        )

    df["פירוט הבדיקה"] = df["מזהה בדיקה"].map(RULE_DETAILS).fillna("")

    # R_1: מפתח בדיקה should show Hebrew name only (ערכי כינון מים מלא etc.)
    r1_prefix = "R_1_"
    mask_r1 = df["rule_id"].astype(str).str.startswith(r1_prefix, na=False)
    df.loc[mask_r1, "rule_id"] = df.loc[mask_r1, "rule_id"].str[len(r1_prefix):]

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

    # --- Build "שורה באקסל" and enrich "עמודה באקסל" with column letter ---
    # Access raw columns before rename (excel_cells, key_context, row_index)
    # Note: after rename, column_name → "עמודה באקסל", key_context → "בדיקה שבוצעה"
    excel_cells_col = "excel_cells" if "excel_cells" in df.columns else None
    key_ctx_col = "בדיקה שבוצעה"  # renamed from key_context
    col_name_col = "עמודה באקסל"   # renamed from column_name
    row_idx_col = "row_index" if "row_index" in df.columns else None

    excel_rows = []
    enriched_col_names = []

    for i in range(len(df)):
        row_num = ""
        col_letter = ""

        # 1. Try to extract from excel_cells
        raw_cells = df.iloc[i][excel_cells_col] if excel_cells_col else None
        parsed = _parse_excel_cells(raw_cells)
        if parsed:
            # Use first cell reference
            col_letter = parsed[0][0]
            row_num = str(parsed[0][1])

        # 2. Fallback: extract from key_context (now renamed to בדיקה שבוצעה)
        if not row_num:
            kc = str(df.iloc[i].get(key_ctx_col, "") or "")
            row_num = _extract_excel_row_from_context(kc)
            if not col_letter:
                col_letter = _extract_col_letter_from_context(kc)

        # 3. Fallback for row: compute from row_index for report sheet rules
        if not row_num and row_idx_col:
            ri = df.iloc[i].get(row_idx_col)
            sheet_name = str(df.iloc[i].get("לשונית באקסל", "") or "")
            if ri is not None and str(ri).strip() not in {"", "nan", "None"}:
                try:
                    ri_int = int(float(ri))
                    # Report sheet: excel_row = row_index + header_row + 2 = row_index + 8
                    if "דיווח" in sheet_name:
                        row_num = str(ri_int + 8)
                except (ValueError, TypeError):
                    pass

        excel_rows.append(row_num)

        # Enrich column name with letter
        cur_col = str(df.iloc[i].get(col_name_col, "") or "").strip()
        if col_letter and cur_col:
            # If column_name is already a pure letter (e.g., "R", "AE"), keep as-is
            # Check if ANY part is already a letter (for multi-column like "R / S")
            parts = [p.strip() for p in cur_col.split("/")]
            all_letters = all(_is_excel_letter(p) for p in parts if p)
            if not all_letters:
                # Only add letter if column_name is not already the letter
                if col_letter not in cur_col:
                    cur_col = f"{cur_col}({col_letter})"
        elif not col_letter and cur_col:
            # For multi-column values like "H / I / J / K / L / M", letters are already there
            pass

        enriched_col_names.append(cur_col)

    df["שורה באקסל"] = excel_rows
    df["עמודה באקסל"] = enriched_col_names

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

    # Total FAILS (support both English "Fail" and Hebrew "נכשל")
    fail_mask = df["status"].isin(["Fail", "נכשל"])
    fails = (
        df[fail_mask]
        .groupby(["utility_name", "plan_file"])
        .size()
        .reset_index(name="total_fails")
    )

    # Top failing rules (optional but helpful)
    top_rules = (
        df[fail_mask]
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
              critical_fails=("severity", lambda s: ((df.loc[s.index, "severity"] == "Critical") & (df.loc[s.index, "status"].isin(["Fail", "נכשל"]))).sum()),
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
        "רשות מקומית",
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

    # --- Defensive: always have 'רשות מקומית' ---
    if "רשות מקומית" not in df.columns:
        if "utility_name" in df.columns:
            df["רשות מקומית"] = df["utility_name"]
        else:
            df["רשות מקומית"] = ""

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
        s = str(v).strip().lower()
        return s in ("fail", "נכשל")

    def _status_is_pass(v: object) -> bool:
        s = str(v).strip().lower()
        return s in ("pass", "עבר")

    # Total = number of sub-checks (rows in group). Fail = count of failed. Pass = count of passed.
    def _kpi_counts(gdf: pd.DataFrame) -> tuple[int, int, int]:
        statuses = gdf["status"].tolist()
        total = len(statuses)
        fail = sum(1 for s in statuses if _status_is_fail(s))
        pass_ct = sum(1 for s in statuses if _status_is_pass(s))
        return fail, total, pass_ct

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
        fail, total, pass_ct = _kpi_counts(gdf)
        rows_out.append({
            "רשות מקומית": _pick_first_nonempty(gdf["רשות מקומית"]),
            "שם קובץ": str(file_stem),
            "check_id": str(check_id),  # internal for sorting
            "שם הבדיקה": _pick_first_nonempty(gdf["rule_name"]),
            "פירוט הבדיקה": RULE_DETAILS.get(str(check_id), ""),
            "מיקום הבדיקה": _location_from_group(gdf),
            "ממצאים": f"{check_id}: {pass_ct}/{total} {'Pass' if fail == 0 else 'FAIL'}",
            "סטטוס": _status_rollup(fail, total),
            "הערות": "",
            "הערת משתמש":  "",
            "פעולה נדרשת מול התאגיד": "",
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


# ---------------------------------------------------------------------------
# LLM Executive Summary
# ---------------------------------------------------------------------------

def _extract_year_from_filename(plan_file: str) -> str:
    """Extract year (e.g. '2026') from plan filename."""
    parts = str(plan_file).replace("\\", "/").split("/")[-1].split("_")
    for p in parts:
        p = p.strip()
        if re.fullmatch(r"20(?:2[4-9]|3[0-7])", p):
            return p
    # fallback: find any 4-digit year in filename
    m = re.search(r"(20[2-3]\d)", str(plan_file))
    return m.group(1) if m else ""


def build_executive_summary_prompt(
    file_checks_df: pd.DataFrame,
    utility_name: str,
    plan_file: str,
) -> str:
    """
    Build an English-instruction prompt that produces a regulator-style Hebrew executive summary.
    Engineering findings from R_16, R_18, R_21, R_23, R_24, R_25 are extracted and injected
    as explicit bullet points so the LLM can cite them without hallucinating.
    """
    import json as _json

    df = file_checks_df.copy()

    fail_mask = df["status"].isin(["Fail", "נכשל"])
    total = len(df)
    fail_count = int(fail_mask.sum())
    pass_count = int(df["status"].isin(["Pass", "עבר"]).sum())
    info_count = total - fail_count - pass_count  # Not Applicable / INFO rows

    year = _extract_year_from_filename(plan_file)
    yr = f"{year}-{int(year)+4}" if year else "?"

    def _rule_fails(prefix: str) -> "pd.DataFrame":
        if "rule_id" not in df.columns:
            return df.iloc[0:0]
        return df[df["rule_id"].str.startswith(prefix) & fail_mask]

    # ── Step 1: Macro positive findings (R_1 Kinun, R_2 Asset Ratios) ─────────
    macro_findings: list[str] = []

    if "rule_id" in df.columns:
        r1_fails = _rule_fails("R_1")
        if r1_fails.empty and df["rule_id"].str.startswith("R_1").any():
            macro_findings.append(
                "POSITIVE — Kinun values: ערכי הכינון המדווחים תואמים לנתוני רשות המים ונמצאו תקינים לחלוטין."
            )

        # R_3 rows (column R, rows 20-22) hold the actual % values for מים, ביוב, סה"כ
        r3_rows = df[df["rule_id"].str.startswith("R_3")]
        if not r3_rows.empty:
            _label_map = {"מים": None, "ביוב": None, "סהכ": None}
            for _, r3r in r3_rows.iterrows():
                rid = str(r3r.get("rule_id", ""))
                av = r3r.get("actual_value")
                for key in _label_map:
                    if rid.endswith(key) and av is not None:
                        # actual_value may be "48.82%" or a float; extract the number
                        try:
                            _label_map[key] = round(float(str(av).replace("%", "").strip()), 1)
                        except (TypeError, ValueError):
                            _label_map[key] = av
            _display = {"מים": _label_map["מים"], "ביוב": _label_map["ביוב"], 'סה"כ': _label_map["סהכ"]}
            asset_ratio_table = (
                "| תחום | יחס נכסים |\n"
                "| :--- | :--- |\n"
                + "\n".join(
                    f"| {label} | {(str(val) + '%') if val is not None else 'N/A'} |"
                    for label, val in _display.items()
                )
            )
            macro_findings.append(
                f"POSITIVE — Asset ratios: יחסי ההשקעות לנכסים נמצאו תקינים ועומדים במינימום הנדרש:\n\n"
                f"{asset_ratio_table}"
            )

    # ── Step 2: Engineering failure findings (R_23, R_24, R_21, R_18, R_16, R_25) ──
    findings: list[str] = []

    # R_23: cost-per-meter outliers
    r23 = _rule_fails("R_23")
    if not r23.empty:
        costs = []
        for _, row in r23.iterrows():
            av = row.get("actual_value", {})
            if isinstance(av, str):
                try:
                    av = _json.loads(av.replace("'", '"'))
                except Exception:
                    av = {}
            if isinstance(av, dict) and "cost_per_meter_nis" in av:
                costs.append(round(float(av["cost_per_meter_nis"]), 0))
        if costs:
            findings.append(
                f"- Pipe cost-per-meter anomalies: {len(r23)} projects deviate from the "
                f"regulator baseline (0.15 × diameter × 1000 ₪/m ±20%). "
                f"Observed range: {int(min(costs))}–{int(max(costs))} ₪/m."
            )
        else:
            findings.append(
                f"- Pipe cost-per-meter anomalies: {len(r23)} projects deviate from the regulatory baseline."
            )

    # R_24: short pipes
    r24_per_row = _rule_fails("R_24")
    short_pipe_rows = (
        r24_per_row[r24_per_row["method"].eq("ShortPipeLength")]
        if "method" in r24_per_row.columns else r24_per_row.iloc[0:0]
    )
    if not short_pipe_rows.empty:
        findings.append(
            f"- Short pipe segments (<100 m): {len(short_pipe_rows)} projects — "
            f"קו קצר מ-100 מטר ללא הסבר נלווה."
        )

    # R_21: missing rehab/development split rows
    r21 = _rule_fails("R_21")
    if not r21.empty:
        findings.append(
            f"- Diameter jump without mandatory split row: {len(r21)} projects with "
            f"planned diameter >2\" above existing have no matching 'פיתוח'/'שדרוג' row."
        )

    # R_18: facility / drilling data gaps
    r18 = _rule_fails("R_18")
    if not r18.empty:
        findings.append(
            f"- Missing mandatory facility/drilling data: {len(r18)} projects "
            f"(שיקום ושדרוג — מתקני מים/ביוב/קידוחים) are missing required fields "
            f"(שנת הקמה, סוג מתקן, נפח/ספיקה, or X marks in Q–U)."
        )

    # R_16: wells misclassified
    r16 = _rule_fails("R_16")
    if not r16.empty:
        names = r16["key_context"].dropna().unique().tolist()[:4]
        names_str = ", ".join(str(n) for n in names)
        findings.append(
            f"- Wells misclassified: {len(r16)} projects with 'באר/בארות' in their name "
            f"are classified as 'מתקני מים' instead of 'קידוחים'."
            + (f" Projects: {names_str}." if names_str else "")
        )

    # R_25: invalid delimiters
    r25 = _rule_fails("R_25")
    if not r25.empty:
        findings.append(
            f"- Invalid delimiters in pipe data columns J–M: {len(r25)} cells use '+', '/', "
            f"or '-' instead of the required colon ':' separator."
        )

    # ── Step 3: Combine macro positives + engineering failures ────────────────
    all_lines = macro_findings + (findings if findings else ["- No specific engineering anomalies detected."])
    engineering_findings_text = "\n".join(all_lines)

    prompt = f"""
You are Michal Peleg Lubovsky, Engineering Control Manager at the Israeli Water Authority.
Your task is to write a formal, dry, and highly professional audit email (מייל בקרה הנדסית) in Hebrew, addressed to the management of the water utility '{utility_name}' regarding their investment plan for {yr}.

General Audit Stats:
- Total checks performed: {total}
- Passed successfully: {pass_count}
- Failed/Anomalies: {fail_count}
- Not Applicable / Skipped: {info_count} (This explains the math: Total = Passed + Failed + Not Applicable).

Here are the specific macro and engineering findings from the system:
{engineering_findings_text}

OUTPUT FORMAT AND STYLE RULES:
1. **DO NOT** write an "Executive Summary". Write a formal email starting exactly with: "שלום רב,\nלהלן ריכוז ממצאים והערות לבקרת תוכנית ההשקעות לשנת {year}:"
2. **Contextual Math:** In the opening, briefly state the stats, e.g., "בוצעו {total} בדיקות, מתוכן {fail_count} נמצאו כחורגות ({info_count} סווגו כלא-רלוונטיות לשורות הספציפיות)."
3. **Structuring the Findings:** Group the findings logically using bullet points:
   - נתוני מאקרו ויחסי השקעה (Include the POSITIVE Kinun and Asset Ratio notes marked above. **Preserve the Markdown table for Asset Ratios exactly as provided — do not reformat it into prose.**).
   - סבירות כלכלית (Economic Feasibility — cost per meter anomalies).
   - לוגיקה הנדסית (Engineering Logic — short pipes, diameter splits).
   - חוסר בשלמות נתונים (Data Integrity — missing facility/drilling data, illegal delimiters).
4. **Tone:** Dry, technical, direct. Give positive credit where due (e.g., Kinun), but be firm on the failures. Do NOT use dramatic words like "מעוררת דאגה", "קיצוני", or "מדד אמת". Use regulatory language like "נמצאה חריגה מנוסחת הבסיס", "יש לפצל את הפרויקט", "חסרים נתוני חובה".
5. **No Hallucinations / No Meta-Text:** Do NOT invent general warnings. If a rule passed, state it passed. Do NOT print rule IDs (like R_23) or meta-tags in the output.
6. **Closing:** End the email exactly with:
"נא התייחסותכם ותיקון הקובץ בהתאם.
בברכה,
מיכל פלג לובובסקי
בקרה הנדסית, הרשות הממשלתית למים ולביוב"
"""
    return prompt


def generate_executive_summaries(
    all_checks_df: pd.DataFrame,
    cfg,
) -> dict:
    """
    Generate LLM executive summary per file.
    Returns: {utility_name: summary_text}
    """
    from .llm_client import generate_text, LLMQuotaError

    if not cfg.llm_enabled:
        return {}

    summaries = {}
    for (utility, plan_file), gdf in all_checks_df.groupby(["utility_name", "plan_file"]):
        prompt = build_executive_summary_prompt(gdf, str(utility), str(plan_file))
        try:
            text = generate_text(
                prompt,
                provider=cfg.llm_provider,
                model=cfg.llm_model,
            )
            summaries[str(utility)] = text
        except LLMQuotaError:
            summaries[str(utility)] = "שגיאה: חריגת מכסת LLM. לא ניתן ליצור תקציר."
        except Exception as e:
            summaries[str(utility)] = f"שגיאה ביצירת תקציר: {e}"

    return summaries
