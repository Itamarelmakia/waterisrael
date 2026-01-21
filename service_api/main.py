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
)

app = FastAPI()


@app.get("/")
def root():
    return {"service": "waterisrael-api", "status": "ok"}


@app.get("/health")
def health():
    return {"status": "ok"}


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


def _run_validation(in_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]]:
    cfg = PlanConfig()

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


@app.post("/validate")
async def validate_json(file: UploadFile = File(...)):
    in_path = _save_upload_to_tmp(file)

    try:
        all_checks_df, summary_table_df, exec_tuple = _run_validation(in_path)

        summary_rows = _build_summary_rows(summary_table_df)

        return JSONResponse(content={"summary_rows": summary_rows})

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
