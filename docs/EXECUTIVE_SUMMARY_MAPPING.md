# Executive Summary (סיכום מנהלים) – Regulator Alignment

This document maps the **water authority regulator’s manual findings** (from their email) to **existing checks** in `checks.py`, and defines the **output format** needed so the LLM can produce a Hebrew executive summary in the regulator’s style.

---

## 1. Regulator’s three main findings (from email)

| # | Regulator finding | Description |
|---|-------------------|-------------|
| **A** | **Technical missing data** | Missing data in columns **N–U** (establishment year, volume, nature of work) for specific projects. |
| **B** | **Classification errors** | Projects named "Wells" (בארות) wrongly classified as "Water Facilities" instead of "Drillings" (קידוחים). |
| **C** | **Cost per meter anomalies (Critical)** | They calculate cost per meter for pipe projects and flag outliers (e.g. 8" sewage 2,450–3,000 ₪/m, or 6" water >1,700 ₪/m). Need sentence like: *"נמצאו 13 פרויקטים בעלות של 2,450-3,000 ₪ למטר - נדרש הסבר לעלויות"*. |

---

## 2. Customer requirement → existing rule → required output format

### Finding A: Technical missing data (columns N–U)

| Customer requirement | Existing rule | What the rule does | Required output for LLM |
|----------------------|---------------|--------------------|-------------------------|
| Missing data in **N–U** (establishment year, volume, nature of work) | **`check_018_facility_rehab_upgrade`** (R_18) | For rows with סיווג = "שיקום ושדרוג" and קוד הנדסי = "מתקני מים" or "מתקני ביוב": checks **N** (שנת הקמה), **O** (סוג מתקן), **P** (נפח/ספיקה), **Q–U** (פירוט העבודות – at least one X). Emits per-project PASS/FAIL. | **Aggregate:** Count of projects missing year / volume / nature (Q–U). Example sentence: *"נמצאו X פרויקטים בשיקום ושדרוג עם נתונים חסרים (שנת הקמה, נפח/ספיקה או פירוט העבודות)."* |
| Mandatory project/location/classification fields | **`check_012_project_fields_not_empty`** (R_12) | Ensures **שם פרויקט**, **מיקום פרויקט**, **סיווג פרויקט** are non-empty (not strictly N–U; different columns). | Count of rows with missing name/location/classification. Example: *"חסרים שדות חובה (שם/מיקום/סיווג) ב-X שורות."* |

**Note:** R_18 is the main source for “missing N–U” for **rehab/upgrade facility** projects. If the regulator also expects missing N–U for other project types, that may require an additional rule or extension.

---

### Finding B: Classification errors (Wells → קידוחים)

| Customer requirement | Existing rule | What the rule does | Required output for LLM |
|----------------------|---------------|--------------------|-------------------------|
| Projects named "בארות" / "Wells" must be classified as **קידוחים** (Drillings), not "מתקני מים" | **`check_014_llm_project_funding_classification`** (R_14) | LLM + keyword/fuzzy: classifies **מימון** (funding) and can flag mismatches. Does **not** currently enforce "באר/קידוח → סיווג = קידוח". | Either **extend R_14** to detect name "באר"/"קידוח" and check סיווג, or use **R_16** (see below). |
| Wells/Bore in name → classification must be "קידוח" | **R_16** (if implemented) | RULE_DETAILS says: *"במידה ובשם הפרויקט מופיע 'באר' או 'קידוח' - סיווג הפרויקט צריך להיות 'קידוח'"*. | If R_16 exists in code: count of projects with "באר"/"קידוח" in name but סיווג ≠ קידוח. Example: *"נמצאו X פרויקטים עם 'באר'/'קידוח' בשם שסווגו שלא כקידוח - נדרש תיקון סיווג."* |

**Gap:** R_14 focuses on **funding** classification. The regulator’s “Wells → קידוחים” is a **project classification** (סיווג פרויקט) rule. You need a check that: (1) finds rows where project name contains באר/קידוח, (2) checks that סיווג = קידוח. If R_16 is not implemented, add it or extend R_14 with this logic and expose a clear failure count + list for the LLM.

---

### Finding C: Cost per meter anomalies (Critical)

| Customer requirement | Existing rule | What the rule does | Required output for LLM |
|----------------------|---------------|--------------------|-------------------------|
| Flag **cost per meter** outliers (e.g. 8" sewage 2,450–3,000 ₪/m; 6" water >1,700 ₪/m) | **`check_023_pipe_cost_rule_of_thumb`** (R_23) | **Water pipes only** (G = "קווי מים"). Cost per meter = AE×1000/M. Compares to rule-of-thumb: diameter×1.2×150; FAIL if outside [calc, 1.5×calc]. Emits **one CheckResult per failing row** with `actual_value=contr_round` (₪/m), `message` with cost and range. | **Aggregate R_23 failures:** Group by `actual_value` (or by rounded range, e.g. 2,450–3,000). Pass to LLM: *"R_23: N כשלונות; עלויות למטר: [רשימה או טווחים]."* So the LLM can generate: *"נמצאו 13 פרויקטים בעלות של 2,450-3,000 ₪ למטר - נדרש הסבר לעלויות."* |
| Same idea but **sewage** and/or **fixed thresholds** (2,450–3,000; >1,700) | **Not fully covered** | R_23 is water-only and uses a **formula** (1.5× rule-of-thumb), not the regulator’s fixed bands. | Either: (1) **Extend R_23** to sewage and add configurable thresholds (e.g. 8" sewage: 2,450–3,000; 6" water: >1,700), or (2) **New rule** that only computes cost/m and flags by threshold. Then expose **count + cost range** per threshold for the LLM. |

**Important:** **`check_019_total_planned_cost_per_project`** (R_19) does **not** do cost per meter. It checks **אומדן פרויקט (AE) ≥ X+AA+AD** (total estimate ≥ sum of cost components). So:

- **R_19** → “סה"כ עלות מתוכננת לפרויקט” (consistency of total estimate).
- **R_23** → “מחיר צנרת לפי כלל אצבע” (cost **per meter** for water pipes).

For the regulator’s “cost per meter” sentence, use **R_23** (and any future sewage/threshold rule), not R_19.

---

### Other rules you mentioned (for completeness)

| Rule | Function | Relevance to regulator summary |
|------|----------|-------------------------------|
| **R_15** | `check_015_invalid_project_names` | Flags generic names (רחוב, בין הבתים, שטח פתוח). Can be summarized as: *"X פרויקטים עם שמות לא מפורטים."* |
| **R_20** | `check_020_project_status_planning_report` | Ensures סטטוס (AH) is non-empty. Summary: count of projects with missing status. |
| **R_24** | `check_024_short_pipe_projects_ratio` | Ratio of projects with pipe length <100 m; FAIL if >5%. Already has a dedicated line in the prompt (`R_24 פרויקטים קטנים: X/Y כשלונות`). |

---

## 3. What to pass to the LLM for regulator-style sentences

To get output like the regulator’s (e.g. *"נמצאו 13 פרויקטים בעלות של 2,450-3,000 ₪ למטר - נדרש הסבר לעלויות"*), the **executive summary prompt** should receive **structured aggregates** from the check results, not only “R_X: fail count”. Suggested additions in `build_executive_summary_prompt()` (or equivalent):

1. **R_18 (missing N–U for facilities)**  
   - Count of FAILs by sub-type (e.g. שנת הקמה / נפח/ספיקה / פירוט העבודות) or a single total.  
   - Example line: `"R_18: X פרויקטים עם נתונים חסרים (שנת הקמה/נפח/ספיקה/פירוט העבודות)."`

2. **R_12 (mandatory fields)**  
   - Count of rows with missing שם/מיקום/סיווג.  
   - Example: `"R_12: X שורות עם שדות חובה חסרים."`

3. **R_23 (cost per meter – water)**  
   - From FAILed CheckResults: collect `actual_value` (cost in ₪/m) and optionally pipe type/diameter from `message` or `key_context`.  
   - Compute ranges (e.g. 2,450–3,000) or bins and counts.  
   - Example: `"R_23: N פרויקטים (קווי מים) בעלות חריגה למטר: טווחים [רשימה] - נדרש הסבר לעלויות."`

4. **Wells / קידוחים (if R_16 or extended R_14)**  
   - Count of projects with באר/קידוח in name but סיווג ≠ קידוח.  
   - Example: `"X פרויקטים עם 'באר'/'קידוח' בשם שסווגו שלא כקידוח."`

5. **R_24**  
   - Already in the prompt; keep as is.

6. **Instruction to the LLM**  
   - Ask the model to phrase these aggregates in **regulator-style Hebrew**, e.g.:  
     - *"נמצאו X פרויקטים בעלות של 2,450-3,000 ₪ למטר - נדרש הסבר לעלויות"* for cost-per-meter.  
     - *"חסרים נתונים טכניים (שנת הקמה, נפח, פירוט עבודות) ב-X פרויקטים."*  
     - *"נמצאו X פרויקטים בהם סיווג הפרויקט לא תואם לשם (בארות/קידוח)."*

---

## 4. Summary table: requirement → rule → output format

| Regulator requirement | Existing check | Output format for LLM |
|-----------------------|----------------|------------------------|
| Missing data N–U (year, volume, nature) | **R_18** `check_018_facility_rehab_upgrade` | Count (+ optional breakdown) of projects with missing שנת הקמה / נפח/ספיקה / פירוט Q–U. Sentence: *"X פרויקטים עם נתונים חסרים (…)."* |
| Mandatory name/location/classification | **R_12** `check_012_project_fields_not_empty` | Count of rows missing שם/מיקום/סיווג. |
| Wells → must be סיווג קידוח | **R_16** (if exists) or extend **R_14** | Count of projects with באר/קידוח in name and סיווג ≠ קידוח. Sentence: *"X פרויקטים עם באר/קידוח בשם שסווגו שלא כקידוח."* |
| Cost per meter outliers (water) | **R_23** `check_023_pipe_cost_rule_of_thumb` | Aggregate FAILs: count + list/range of ₪/m. Sentence: *"נמצאו X פרויקטים בעלות של [טווח] ₪ למטר - נדרש הסבר לעלויות."* |
| Cost per meter (sewage / fixed thresholds) | **Gap** – extend R_23 or new rule | Same format: count + cost range per threshold. |
| Total estimate vs components | **R_19** `check_019_total_planned_cost_per_project` | Not cost-per-meter; use for "אומדן vs רכיבים" only. Optional: count of R_19 failures. |
| Short pipe ratio | **R_24** `check_024_short_pipe_projects_ratio` | Already in prompt. |

---

## 5. Recommended code changes (short)

1. **`report.py` – `build_executive_summary_prompt()`**  
   - Add aggregation of **R_18** failures (count, optionally by sub-check).  
   - Add aggregation of **R_23** failures: from `actual_value` (and message) build a short summary: count + cost ranges (e.g. "2,450-3,000 ₪ למטר").  
   - Add R_12 failure count if not already implied by `top_fails`.  
   - If R_16 (or R_14 extension) exists: add count of Wells misclassified.  
   - Add one instruction line: ask the LLM to phrase the summary in the regulator’s style (e.g. *"נדרש הסבר לעלויות"*, *"נתונים חסרים"*, *"תיקון סיווג"*).

2. **Optional: extend R_23 or add a rule**  
   - For sewage pipes and/or fixed thresholds (2,450–3,000; >1,700), so the data for the “cost per meter” sentence matches the regulator’s methodology.

3. **Wells / קידוחים**  
   - Implement or locate R_16; if it’s only in RULE_DETAILS, add the check and feed its failure count into the prompt as above.

This gives you a clear **Customer Requirement → Existing Rule → Required Output Format** mapping and the changes needed so the LLM can produce a סיכום מנהלים that matches the regulator’s format.
