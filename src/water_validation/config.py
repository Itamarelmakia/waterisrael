# src/water_validation/config.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple


@dataclass
class PlanConfig:
    sheet_name: str = "סיכום תכנית השקעות"

    # Raw Excel column indices (0-based): A=0, B=1, ..., R=17, S=18
    label_col_idx: int = 0
    data_marker_col_idx: int = 1
    value_col_r_idx: int = 17
    value_col_s_idx: int = 18

    header_lookback_rows: int = 6


    report_sheet_name: str = "גיליון דיווח"
    report_header_row: int = 6  # pandas header row (0-based). Excel header is row 7.
    report_project_id_col_norm: str = "מס' פרויקט"


    # Dynamic anchor (set at runtime by loader)
    data_start_excel_row: Optional[int] = None  # 1-based Excel row number where plan_df row 0 starts

    # Kinun reference (flattened A–E structure)
    kinun_utility_col: str = "תאגיד מים וביוב"
    kinun_full_water_col: str = "תשתיות מים מלא"
    kinun_reduced_water_col: str = "תשתיות מים מופחת"
    kinun_full_sewer_col: str = "תשתיות ביוב מלא"
    kinun_reduced_sewer_col: str = "תשתיות ביוב מופחת"

    # Fixed Excel row mapping (1-based Excel rows)
    total_program_rows_excel: Dict[str, int] = field(default_factory=lambda: {"מים": 8, "ביוב": 9, "סה\"כ": 10})
    min_required_program_rows_excel: Dict[str, int] = field(default_factory=lambda: {"מים": 25, "ביוב": 26, "סה\"כ": 27})
    rehab_upgrade_min_rows_excel: Dict[str, int] = field(default_factory=lambda: {"מים": 28, "ביוב": 29, "סה\"כ": 30})
    asset_ratio_rows_excel: Dict[str, int] = field(default_factory=lambda: {"מים": 20, "ביוב": 21, "סה\"כ": 22})

    # (label, system) -> excel row
    kinun_plan_rows_excel: Dict[Tuple[str, str], int] = field(default_factory=lambda: {
        ("ערך כינון מלא", "מים"): 8,
        ("ערך כינון מלא", "ביוב"): 9,
        ("ערך כינון מופחת", "מים"): 11,
        ("ערך כינון מופחת", "ביוב"): 12,
    })

    planned_investments_row_excel: int = 39
    funding_total_row_excel: int = 50
    water_pipe_rows_excel: Tuple[int, int] = (56, 57)
    sewer_pipe_row_excel: int = 58

    llm_enabled: bool = True
    llm_model: str = "gpt-4o-mini"
    llm_validate_all: bool = True


@dataclass(frozen=True)
class InputDiscoveryConfig:
    """
    File discovery defaults for 'all files in same folder'.
    Adjust patterns to your real filenames.
    """
    plan_glob: str = "תכנית השקעות*.xlsx"
    kinun_glob: str = "ערכי כינון*.xlsx"


# From here Itamar add this logic :