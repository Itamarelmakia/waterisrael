from __future__ import annotations
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Any, Optional, List


class Severity(str, Enum):
    CRITICAL = "Critical"
    WARNING = "Warning"
    INFO = "Info"


class Status(str, Enum):
    PASS_ = "Pass"
    FAIL = "Fail"
    NOT_APPLICABLE = "Not applicable"


@dataclass
class CheckResult:
    # --- required ---
    rule_id: str
    rule_name: str
    severity: Severity
    sheet_name: str
    status: Status
    message: str

    # --- optional / contextual ---
    row_index: Optional[int] = None
    column_name: Optional[str] = None
    key_context: str = ""

    actual_value: Any = None
    expected_value: Any = None

    confidence: Optional[float] = None   # LLM / fuzzy confidence
    method: Optional[str] = None         # keyword | fuzzy | llm

    excel_cells: Optional[List[str]] = None

    def to_record(self) -> dict:
        rec = asdict(self)
        rec["status"] = self.status.value
        rec["severity"] = self.severity.value
        return rec
