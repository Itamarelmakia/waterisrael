# src/water_validation/utils.py
from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any, List, Optional, Tuple

import pandas as pd

#from config import PlanConfig
from .config import PlanConfig


def normalize_text(value: object) -> str:
    s = "" if value is None else str(value)
    s = s.replace("\u200f", "").replace("\u200e", "")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def round_half_up(value: Any, ndigits: int = 0) -> Optional[float]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None
    factor = 10 ** ndigits
    return math.floor(x * factor + 0.5) / factor


def parse_ratio(value: Any) -> Optional[float]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    if isinstance(value, str):
        s = normalize_text(value)
        if s.endswith("%"):
            try:
                return float(s[:-1]) / 100.0
            except ValueError:
                return None
        try:
            x = float(s)
        except ValueError:
            return None
    else:
        try:
            x = float(value)
        except (TypeError, ValueError):
            return None

    return x / 100.0 if x > 1.0 else x


def extract_utility_from_plan_filename(plan_file: str | Path) -> str:
    stem = normalize_text(Path(plan_file).stem)
    m = re.search(r"\bתאגיד\b\s*(.+)$", stem)
    if not m:
        raise ValueError(f"Cannot extract utility from filename (missing 'תאגיד'): {stem}")
    utility = normalize_text(m.group(1))
    if not utility:
        raise ValueError(f"Cannot extract utility from filename (empty after 'תאגיד'): {stem}")
    return utility


def excel_row_to_df_index(excel_row_1_based: int, cfg: PlanConfig) -> int:
    if cfg.data_start_excel_row is None:
        raise ValueError("cfg.data_start_excel_row is not set. Loader must run before checks.")
    return excel_row_1_based - cfg.data_start_excel_row


def is_date_like(s: str) -> bool:
    return bool(re.match(r"^\d{1,2}\.\d{1,2}\.\d{2,4}$", s))


def is_city_like_header(value: Any) -> bool:
    s = normalize_text(value)
    if not s:
        return False
    if s.startswith("Unnamed"):
        return False
    if is_date_like(s):
        return False
    if any(ch.isdigit() for ch in s):
        return False

    blacklist = {
        "תאגיד", "תאגיד מים וביוב", "רשויות", "רשויות/ת\"מ במתאגיד", "רשויות/ת״ם במתאגיד",
        "מים", "ביוב", "סה\"כ", "סה״כ",
        "תאריך", "תאריכים",
    }
    if s in blacklist:
        return False

    return any("\u0590" <= ch <= "\u05FF" for ch in s)


def longest_true_run(mask: List[bool]) -> Tuple[int, int]:
    best = (-1, -1)
    best_len = 0
    start = None

    for i, v in enumerate(mask):
        if v and start is None:
            start = i
        if (not v or i == len(mask) - 1) and start is not None:
            end = i if v and i == len(mask) - 1 else i - 1
            run_len = end - start + 1
            if run_len > best_len:
                best_len = run_len
                best = (start, end)
            start = None

    return best


def detect_city_cols(df: pd.DataFrame) -> List[str]:
    cols = list(df.columns)
    mask = [is_city_like_header(c) for c in cols]
    start, end = longest_true_run(mask)
    if start == -1:
        return []
    return cols[start:end + 1]
