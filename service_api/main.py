# service_api/main.py

import os
import shutil
import tempfile
from pathlib import Path
from typing import Dict, Any, List, Tuple

import pandas as pd
from fastapi import FastAPI, UploadFile, File, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse, JSONResponse

from water_validation.config import PlanConfig
from water_validation.runner import run_summary_sheet_checks
from water_validation.report import (
    format_all_checks_for_export,
    build_executive_summary,
    build_summary_table,
    generate_executive_summaries,
)

app = FastAPI()


from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {"service": "waterisrael-api", "status": "ok"}


@app.get("/health")
def health():
    return {"status": "ok"}


from fastapi import Request

@app.get("/debug_headers")
def debug_headers(request: Request):
    return {
        "origin": request.headers.get("origin"),
        "host": request.headers.get("host"),
        "headers_present": {
            "origin": "origin" in request.headers,
            "referer": "referer" in request.headers,
        },
    }


@app.get("/config")
def show_config():
    """Show current LLM configuration (no secrets)."""
    cfg = _build_config()
    has_gemini_key = bool(os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY"))
    has_openai_key = bool(os.getenv("OPENAI_API_KEY"))
    return {
        "llm_enabled": cfg.llm_enabled,
        "llm_provider": cfg.llm_provider,
        "llm_model": cfg.llm_model,
        "has_gemini_key": has_gemini_key,
        "has_openai_key": has_openai_key,
        "llm_ready": cfg.llm_enabled and (
            (cfg.llm_provider == "gemini" and has_gemini_key) or
            (cfg.llm_provider == "openai" and has_openai_key)
        ),
    }


def _safe_remove(path: str) -> None:
    try:
        os.remove(path)
    except FileNotFoundError:
        return
    except Exception:
        return


def _save_upload_to_tmp(file: UploadFile) -> str:
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename")

    tmp_in = tempfile.NamedTemporaryFile(
        suffix=f"_{Path(file.filename).name}",
        delete=False,
    )
    in_path = tmp_in.name
    tmp_in.close()

    try:
        with open(in_path, "wb") as f:
            shutil.copyfileobj(file.file, f)
    finally:
        try:
            file.file.close()
        except Exception:
            pass

    return in_path


def _build_config() -> PlanConfig:
    """Build PlanConfig from environment variables."""
    cfg = PlanConfig()

    # LLM configuration from environment
    llm_enabled_env = os.getenv("LLM_ENABLED", "true").strip().lower()
    cfg.llm_enabled = llm_enabled_env in ("true", "1", "yes")

    llm_provider = os.getenv("LLM_PROVIDER", "gemini").strip().lower()
    if llm_provider:
        cfg.llm_provider = llm_provider

    llm_model = os.getenv("LLM_MODEL", "").strip()
    if llm_model:
        cfg.llm_model = llm_model

    return cfg


def _run_validation(in_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]]:
    cfg = _build_config()

    all_checks_df = run_summary_sheet_checks(
        plan_file=in_path,
        kinun_file=None,
        cfg=cfg,
        rules="all",
    )

    summary_table_df = build_summary_table(all_checks_df)

    # IMPORTANT: this returns a tuple: (headline, counts, fails, top_rules)
    exec_tuple = build_executive_summary(all_checks_df)

    return all_checks_df, summary_table_df, exec_tuple


def _build_summary_rows(summary_table_df: pd.DataFrame) -> List[Dict[str, Any]]:
    df = summary_table_df.copy().fillna("")

    # Base44 table columns: מיקום הבדיקה | תיאור | סטטוס
    # Our summary_table has: "מיקום הבדיקה", "פירוט הבדיקה", "סטטוס"
    if "פירוט הבדיקה" in df.columns and "תיאור" not in df.columns:
        df = df.rename(columns={"פירוט הבדיקה": "תיאור"})

    needed = ["מיקום הבדיקה", "תיאור", "סטטוס"]
    for c in needed:
        if c not in df.columns:
            df[c] = ""

    return df[needed].to_dict(orient="records")


def _is_fail(status) -> bool:
    s = str(status).strip().lower()
    return s in ("fail", "נכשל")


def _build_insights_by_check(all_checks_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Per-check failure breakdown: check_id, name, total, fail, pct."""
    import re
    df = all_checks_df.copy()
    if "rule_id" not in df.columns or "status" not in df.columns:
        return []

    def _short_id(rid):
        s = str(rid).strip()
        if not s.startswith("R_"):
            return s
        parts = [p.strip() for p in s.split("_") if p.strip()]
        if len(parts) < 2 or not parts[1].isdigit():
            return s
        first = int(parts[1])
        if len(parts) >= 3 and parts[2].isdigit():
            return f"R_{first}_{int(parts[2])}"
        return f"R_{first}"

    df["check_id"] = df["rule_id"].apply(_short_id)
    df = df[df["check_id"].astype(str).str.strip().ne("")]

    rows = []
    for check_id, gdf in df.groupby("check_id"):
        total = len(gdf)
        fail = sum(1 for s in gdf["status"] if _is_fail(s))
        name = ""
        if "rule_name" in gdf.columns:
            names = gdf["rule_name"].dropna().tolist()
            name = str(names[0]).strip() if names else ""
        rows.append({
            "check_id": str(check_id),
            "name": name,
            "total": total,
            "fail": fail,
            "pct": round(fail / total * 100, 1) if total > 0 else 0,
        })

    rows.sort(key=lambda r: (-r["pct"], -r["fail"]))
    return rows


def _build_insights_by_row(all_checks_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Per-row failure breakdown: row_index (Excel), total checks, fail checks, pct."""
    df = all_checks_df.copy()
    if "row_index" not in df.columns or "status" not in df.columns:
        return []

    # Filter rows that have a real row_index
    df = df[df["row_index"].notna()].copy()
    df = df[df["row_index"].astype(str).str.strip().apply(lambda x: x.lower() not in ("", "nan", "none"))]
    if df.empty:
        return []

    df["row_int"] = df["row_index"].apply(lambda x: int(float(x)))

    # Compute excel row (report sheet offset +9)
    def _excel_row(row):
        sheet = str(row.get("sheet_name", "") or "")
        if "דיווח" in sheet:
            return row["row_int"] + 9
        return row["row_int"]

    df["excel_row"] = df.apply(_excel_row, axis=1)

    rows = []
    for excel_row, gdf in df.groupby("excel_row"):
        total = len(gdf)
        fail = sum(1 for s in gdf["status"] if _is_fail(s))
        rows.append({
            "excel_row": int(excel_row),
            "total": total,
            "fail": fail,
            "pct": round(fail / total * 100, 1) if total > 0 else 0,
        })

    rows.sort(key=lambda r: (-r["pct"], -r["fail"]))
    return rows


@app.post("/validate")
async def validate_json(file: UploadFile = File(...)):
    in_path = _save_upload_to_tmp(file)

    try:
        all_checks_df, summary_table_df, exec_tuple = _run_validation(in_path)

        summary_rows = _build_summary_rows(summary_table_df)
        insights_by_check = _build_insights_by_check(all_checks_df)
        insights_by_row = _build_insights_by_row(all_checks_df)

        return JSONResponse(content={
            "summary_rows": summary_rows,
            "insights_by_check": insights_by_check,
            "insights_by_row": insights_by_row,
        })

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Validation failed: {e}")
    finally:
        _safe_remove(in_path)


@app.post("/validate_download")
async def validate_download(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    in_path = _save_upload_to_tmp(file)

    tmp_out = tempfile.NamedTemporaryFile(suffix="_validation_output.xlsx", delete=False)
    out_path = tmp_out.name
    tmp_out.close()

    background_tasks.add_task(_safe_remove, in_path)
    background_tasks.add_task(_safe_remove, out_path)

    try:
        all_checks_df, summary_table_df, exec_tuple = _run_validation(in_path)

        # Unpack executive tuple
        executive_headline, executive_counts, executive_fails, executive_top_rules = exec_tuple

        all_checks_export = format_all_checks_for_export(all_checks_df)

        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            summary_table_df.to_excel(writer, index=False, sheet_name="Summary_Table")
            executive_headline.to_excel(writer, index=False, sheet_name="Executive_Headline")
            executive_counts.to_excel(writer, index=False, sheet_name="Executive_Counts")
            executive_fails.to_excel(writer, index=False, sheet_name="Executive_Fails")
            executive_top_rules.to_excel(writer, index=False, sheet_name="Executive_TopRules")
            all_checks_export.to_excel(writer, index=False, sheet_name="All_Checks")

    except Exception as e:
        _safe_remove(out_path)
        raise HTTPException(status_code=500, detail=f"Validation failed: {e}")

    return FileResponse(
        out_path,
        filename="validation_output.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        background=background_tasks,
    )


@app.post("/executive_summary")
async def executive_summary(file: UploadFile = File(...)):
    """Generate LLM executive summary for the uploaded file."""
    in_path = _save_upload_to_tmp(file)

    try:
        all_checks_df, _, _ = _run_validation(in_path)
        cfg = _build_config()
        summaries = generate_executive_summaries(all_checks_df, cfg)
        return JSONResponse(content={"summaries": summaries})

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Executive summary failed: {e}")
    finally:
        _safe_remove(in_path)
