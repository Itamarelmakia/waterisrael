"""
Tests for R21 diameter normalization: _normalize_to_nearest and grade table consistency.
"""
import pytest
from water_validation.checks import _normalize_to_nearest, _GRADE_MM, _GRADE_INCH


class TestNormalizeToNearest:
    """Verify nearest-value snapping with tie-break-up rule."""

    def test_exact_match(self):
        assert _normalize_to_nearest(200, _GRADE_MM) == 200
        assert _normalize_to_nearest(12, _GRADE_INCH) == 12

    def test_225mm_tie_rounds_up(self):
        # 225 equidistant from 200 and 250 -> tie -> choose larger
        assert _normalize_to_nearest(225, _GRADE_MM) == 250

    def test_224mm_rounds_down(self):
        # 224 closer to 200 (distance 24) than 250 (distance 26)
        assert _normalize_to_nearest(224, _GRADE_MM) == 200

    def test_226mm_rounds_up(self):
        # 226 closer to 250 (distance 24) than 200 (distance 26)
        assert _normalize_to_nearest(226, _GRADE_MM) == 250

    def test_below_minimum(self):
        # 50mm is below 100 (min) -> snaps to 100
        assert _normalize_to_nearest(50, _GRADE_MM) == 100

    def test_above_maximum(self):
        # 1700mm is above 1600 (max) -> snaps to 1600
        assert _normalize_to_nearest(1700, _GRADE_MM) == 1600

    def test_inch_boundary(self):
        # 11 is between 10 and 12, equidistant -> tie -> 12
        assert _normalize_to_nearest(11, _GRADE_INCH) == 12
        # 9 is closer to 10 (dist 1) than 8 (dist 1) -> tie -> 10
        assert _normalize_to_nearest(9, _GRADE_INCH) == 10


class TestGradeTableConsistency:
    """Verify the authoritative inch ↔ mm mapping."""

    def test_table_lengths_match(self):
        assert len(_GRADE_INCH) == len(_GRADE_MM) == 21

    def test_36_inch_maps_to_914mm(self):
        idx = _GRADE_INCH.index(36)
        assert _GRADE_MM[idx] == 914

    def test_tables_sorted_ascending(self):
        assert _GRADE_INCH == sorted(_GRADE_INCH)
        assert _GRADE_MM == sorted(_GRADE_MM)

    def test_all_entries_present(self):
        expected_inch = [4, 6, 8, 10, 12, 14, 16, 18, 20, 24, 28, 30, 32, 36, 40, 42, 46, 48, 54, 60, 64]
        expected_mm = [100, 160, 200, 250, 315, 355, 400, 450, 500, 630, 710, 750, 800, 914, 1000, 1050, 1150, 1200, 1350, 1500, 1600]
        assert _GRADE_INCH == expected_inch
        assert _GRADE_MM == expected_mm
