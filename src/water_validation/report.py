# src/water_validation/report.py
from __future__ import annotations

import re
import json
import pandas as pd

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


import re

def build_summary_table(all_checks_df: pd.DataFrame) -> pd.DataFrame:
    """
    Summary_Table (Hebrew):
    מזהה בדיקה | שם הבדיקה | פירוט הבדיקה | רמת בדיקה | מיקום הבדיקה | סטטוס | ממצאים | הערות | הערת משתמש | פעולה נדרשת מול התאגיד
    """

    df = all_checks_df.copy()

    # ---------- 1) Normalize check id ----------
    def extract_check_id(rule_id: str) -> str:
        """
        Works for:
          R_1_... -> R_1
          R_001_... -> R_1
          R_2_03_... -> R_2_3
          R_2_3_... -> R_2_3
        """
        if rule_id is None:
            return ""
        s = str(rule_id).strip()
        if not s.startswith("R_"):
            return ""
        parts = [p.strip() for p in s.split("_") if p.strip()]
        if len(parts) < 2 or not parts[1].isdigit():
            return ""
        a = int(parts[1])
        if len(parts) >= 3 and parts[2].isdigit():
            b = int(parts[2])
            return f"R_{a}_{b}"
        return f"R_{a}"

    def extract_level(rule_id: str) -> str:
        # last token after "_" is the level (מים / ביוב / מים פלדה...)
        if rule_id is None:
            return ""
        parts = [p.strip() for p in str(rule_id).split("_") if p.strip()]
        if len(parts) < 2:
            return ""
        last = parts[-1]
        # if last is numeric, it’s not a level
        if re.fullmatch(r"\d+", last):
            return ""
        return last

    df["check_id"] = df["rule_id"].apply(extract_check_id)
    df["level"] = df["rule_id"].apply(extract_level)

    # ---------- 2) “רמת בדיקה” = unique levels per check ----------
    def levels_str(s: pd.Series) -> str:
        vals = sorted({str(x).strip() for x in s.dropna() if str(x).strip()})
        return ",".join(vals)

    # ---------- 3) “מיקום הבדיקה” = compress key_context ----------
    def parse_kv(s: str) -> dict:
        # "plan_cell=R8; kinun_col=water_full" -> {"plan_cell":"R8", "kinun_col":"water_full"}
        out = {}
        if not s:
            return out
        for part in str(s).split(";"):
            part = part.strip()
            if not part:
                continue
            if "=" in part:
                k, v = part.split("=", 1)
                out[k.strip()] = v.strip()
            else:
                out[part] = ""
        return out

    def compress_cell_range(cells):
        """
        cells like ["R8","R9","R11"] -> "R8-R11" (min-max per letter group).
        If multiple letters exist, returns "R8-R12,AA3-AA7" etc.
        """
        groups = {}
        for c in cells:
            c = str(c).strip()
            m = re.fullmatch(r"([A-Za-z]+)(\d+)", c)
            if not m:
                groups.setdefault("_other_", set()).add(c)
                continue
            col, row = m.group(1).upper(), int(m.group(2))
            groups.setdefault(col, set()).add(row)

        parts = []
        for col, rows in sorted(groups.items(), key=lambda x: x[0]):
            if col == "_other_":
                parts.extend(sorted(rows))
                continue
            rows = sorted(rows)
            if not rows:
                continue
            if len(rows) == 1:
                parts.append(f"{col}{rows[0]}")
            else:
                parts.append(f"{col}{rows[0]}-{col}{rows[-1]}")
        return ",".join(parts)

    def location_from_key_context(series: pd.Series) -> str:
        # aggregate values per key across all unique key_context strings
        agg = {}
        for raw in sorted({str(x).strip() for x in series.dropna() if str(x).strip()}):
            kv = parse_kv(raw)
            for k, v in kv.items():
                if v is None:
                    continue
                agg.setdefault(k, set()).add(str(v).strip())

        # Build compact string. Put plan_cell first if exists.
        out_parts = []
        if "plan_cell" in agg:
            cell_range = compress_cell_range(agg["plan_cell"])
            if cell_range:
                out_parts.append(f"plan_cell={cell_range}")

        for k in sorted(agg.keys()):
            if k == "plan_cell":
                continue
            vals = sorted({v for v in agg[k] if v != ""})
            if vals:
                out_parts.append(f"{k}=" + ",".join(vals))
            else:
                out_parts.append(k)

        return "; ".join(out_parts)

    # ---------- 4) name + details ----------
    def pick_first_nonempty(series: pd.Series) -> str:
        for x in series.dropna():
            t = str(x).strip()
            if t:
                return t
        return ""

    def details_from_id(check_id: str) -> str:
        return RULE_DETAILS.get(check_id, "")

    # ---------- 5) status rollup ----------
    def rollup_status(series: pd.Series) -> str:
        vals = [str(v) for v in series.dropna()]
        if any(v.lower() == "fail" for v in vals):
            return "Fail"
        if any("review" in v.lower() for v in vals):
            return "Requires review"
        return "Pass"

    # ---------- 6) Build summary ----------
    g = df.groupby("check_id", dropna=False)

    out = pd.DataFrame({
        "מזהה בדיקה": g.size().index,  # will convert to R1 format below
        "שם הבדיקה": g["rule_name"].apply(pick_first_nonempty),
        "פירוט הבדיקה": g.size().index.map(details_from_id),
        "רמת בדיקה": g["level"].apply(levels_str),
        "מיקום הבדיקה": g["key_context"].apply(location_from_key_context),
        "סטטוס": g["status"].apply(rollup_status),
        "ממצאים": "",
        "הערות": "",
        "הערת משתמש": "",
        "פעולה נדרשת מול התאגיד": "",
    }).reset_index(drop=True)

    # Convert "R_1" -> "R1" for Summary_Table display
    out["מזהה בדיקה"] = out["מזהה בדיקה"].astype(str).str.replace("R_", "R", regex=False)

    # Sort by numeric id (R1, R2_3, R10...)
    def sort_key(x: str):
        # x like "R2_3" or "R10"
        x = str(x).replace("R", "", 1)
        parts = x.split("_")
        a = int(parts[0]) if parts[0].isdigit() else 10**9
        b = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else -1
        return (a, b)

    out = out.sort_values(by="מזהה בדיקה", key=lambda s: s.map(sort_key)).reset_index(drop=True)
    return out