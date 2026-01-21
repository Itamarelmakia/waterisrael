# service_api/main.py

import os
import shutil
import tempfile
from pathlib import Path

from fastapi import FastAPI, UploadFile, File, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse

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

def _as_df(x):
    return x[0] if isinstance(x, tuple) else x


def _build_outputs(in_path: str, out_path: str | None = None):
    cfg = PlanConfig()

    all_checks_df = run_summary_sheet_checks(
        plan_file=in_path,
        kinun_file=None,
        cfg=cfg,
        rules="all",
    )

    all_checks_export = _as_df(format_all_checks_for_export(all_checks_df))
    summary_table_df = _as_df(build_summary_table(all_checks_df))
    executive_df = _as_df(build_executive_summary(all_checks_df))

    if out_path is not None:
        import pandas as pd
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            summary_table_df.to_excel(writer, index=False, sheet_name="Summary_Table")
            executive_df.to_excel(writer, index=False, sheet_name="Executive_Summary")
            all_checks_export.to_excel(writer, index=False, sheet_name="All_Checks")

    summary_rows = summary_table_df.to_dict(orient="records")
    return summary_rows


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/validate")
async def validate(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename")

    tmp_in = tempfile.NamedTemporaryFile(suffix=f"_{Path(file.filename).name}", delete=False)
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

    background_tasks.add_task(_safe_remove, in_path)

    try:
        summary_rows = _build_outputs(in_path, out_path=None)
        return {"summary_rows": summary_rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Validation failed: {e}")


    # 4) החזרת הקובץ להורדה
    #download_name = "validation_output.xlsx"
    #print("DEBUG out_path =", out_path, "exists?", os.path.exists(out_path))

    #return FileResponse(
    #    out_path,
    #    filename=download_name,
    #    media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    #    background=background_tasks,  # חשוב: כדי למחוק אחרי השליחה
    #)

    # summary_rows = רשומות לטבלה ב-Base44
    summary_rows = summary_table_df.to_dict(orient="records")
    return {"summary_rows": summary_rows}

@app.post("/validate_download")
async def validate_download(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename")

    tmp_in = tempfile.NamedTemporaryFile(suffix=f"_{Path(file.filename).name}", delete=False)
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

    background_tasks.add_task(_safe_remove, in_path)

    tmp_out = tempfile.NamedTemporaryFile(suffix="_validation_output.xlsx", delete=False)
    out_path = tmp_out.name
    tmp_out.close()

    background_tasks.add_task(_safe_remove, out_path)

    try:
        _build_outputs(in_path, out_path=out_path)
    except Exception as e:
        _safe_remove(out_path)
        raise HTTPException(status_code=500, detail=f"Validation failed: {e}")

    return FileResponse(
        out_path,
        filename="validation_output.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        background=background_tasks,
    )


def _safe_remove(path: str) -> None:
    try:
        os.remove(path)
    except FileNotFoundError:
        return
    except Exception:
        return
