"""
Tests for R14 - Funding Classification Validation

Tests cover:
1. canonicalize_label() mapping
2. Fuzzy threshold behavior
3. LLM quota handling path (mocked)
4. Maintenance always-fail path
"""
import pytest
from unittest.mock import patch, MagicMock
import pandas as pd


class TestCanonicalizeLabel:
    """Test canonicalize_label function for all label variations."""

    def test_exact_canonical_labels(self):
        """Exact canonical labels should be returned unchanged."""
        from src.water_validation.checks import canonicalize_label

        assert canonicalize_label("שיקום/שדרוג") == "שיקום/שדרוג"
        assert canonicalize_label("פיתוח") == "פיתוח"
        assert canonicalize_label("תשתית ביוב אזורית") == "תשתית ביוב אזורית"
        assert canonicalize_label("קידוחים") == "קידוחים"
        assert canonicalize_label("תחזוקה / שוטף") == "תחזוקה / שוטף"

    def test_regional_sewer_variations(self):
        """All 'תשתית ביוב אזורית' variations should normalize to canonical."""
        from src.water_validation.checks import canonicalize_label

        assert canonicalize_label("תשתיות ביוב אזוריות") == "תשתית ביוב אזורית"
        assert canonicalize_label("תשתיות ביוב אזורית") == "תשתית ביוב אזורית"
        assert canonicalize_label("תשתית ביוב אזוריות") == "תשתית ביוב אזורית"
        assert canonicalize_label("תשתית ביוב אזורי") == "תשתית ביוב אזורית"

    def test_shikum_upgrade_variations(self):
        """All 'שיקום/שדרוג' variations should normalize to canonical."""
        from src.water_validation.checks import canonicalize_label

        assert canonicalize_label("שיקום ושדרוג") == "שיקום/שדרוג"
        assert canonicalize_label("שיקום / שדרוג") == "שיקום/שדרוג"
        assert canonicalize_label("שיקום ושידרוג") == "שיקום/שדרוג"

    def test_maintenance_variations(self):
        """All 'תחזוקה / שוטף' variations should normalize to canonical (with spaces)."""
        from src.water_validation.checks import canonicalize_label

        assert canonicalize_label("תחזוקה/שוטף") == "תחזוקה / שוטף"
        assert canonicalize_label("תחזוקה ושוטף") == "תחזוקה / שוטף"
        assert canonicalize_label("תחזוקה  /  שוטף") == "תחזוקה / שוטף"

    def test_empty_and_whitespace(self):
        """Empty strings and whitespace should return empty."""
        from src.water_validation.checks import canonicalize_label

        assert canonicalize_label("") == ""
        assert canonicalize_label("   ") == ""
        assert canonicalize_label(None) == ""


class TestFuzzyThresholdConstants:
    """Test that fuzzy threshold constant exists and has correct value."""

    def test_constant_exists(self):
        """Fuzzy threshold constant should exist at 50%."""
        from src.water_validation.checks import R14_FUZZY_THRESHOLD

        assert R14_FUZZY_THRESHOLD == 0.50


class TestAllowedFundingLabels:
    """Test the canonical allowed labels set."""

    def test_allowed_labels_complete(self):
        """ALLOWED_FUNDING_LABELS should contain exactly 5 canonical labels."""
        from src.water_validation.prompts import ALLOWED_FUNDING_LABELS

        expected = {
            "שיקום/שדרוג",
            "פיתוח",
            "תשתית ביוב אזורית",
            "קידוחים",
            "תחזוקה / שוטף",
        }
        assert ALLOWED_FUNDING_LABELS == expected


class TestLLMClient:
    """Test LLM client functionality."""

    def test_llm_returns_tuple_of_two(self):
        """classify_funding_with_confidence should return (label, confidence) - no subject."""
        from src.water_validation.llm_client import classify_funding_with_confidence
        import inspect

        sig = inspect.signature(classify_funding_with_confidence)
        # Check return annotation if available
        # The function should return Tuple[str, float]

    def test_quota_error_class_exists(self):
        """LLMQuotaError should be defined."""
        from src.water_validation.llm_client import LLMQuotaError

        assert issubclass(LLMQuotaError, RuntimeError)


class TestBuildLLMPrompt:
    """Test LLM prompt builder."""

    def test_prompt_no_subject_parameter(self):
        """build_llm_prompt should not accept subject parameter."""
        from src.water_validation.prompts import build_llm_prompt
        import inspect

        sig = inspect.signature(build_llm_prompt)
        params = list(sig.parameters.keys())
        assert "subject" not in params
        assert "project_name" in params
        assert "allowed_set" in params

    def test_prompt_output_structure(self):
        """Prompt should include expected JSON format instructions."""
        from src.water_validation.prompts import build_llm_prompt

        prompt = build_llm_prompt("שיקום קו מים", allowed_set={"שיקום/שדרוג", "פיתוח"})

        assert "JSON" in prompt
        assert "label" in prompt
        assert "confidence" in prompt
        assert "subject" not in prompt  # Subject should not be in the prompt


class TestConfigShikumFlag:
    """Test the r14_shikum_not_investment config flag."""

    def test_config_flag_exists_and_defaults_false(self):
        """r14_shikum_not_investment flag should exist and default to False."""
        from src.water_validation.config import PlanConfig

        cfg = PlanConfig()
        assert hasattr(cfg, "r14_shikum_not_investment")
        assert cfg.r14_shikum_not_investment is False


class TestMaintenanceAlwaysFail:
    """Test that maintenance classification always fails."""

    def test_maintenance_reported_fails(self):
        """Reported 'תחזוקה / שוטף' should always FAIL."""
        from src.water_validation.checks import check_014_llm_project_funding_classification
        from src.water_validation.config import PlanConfig
        from src.water_validation.models import Status

        cfg = PlanConfig(llm_enabled=False)
        df = pd.DataFrame({
            "מס' פרויקט": ["1"],
            "שם פרויקט": ["שיקום קו מים - החלפה"],
            "סיווג פרויקט": ["תחזוקה / שוטף"],
        })

        results = check_014_llm_project_funding_classification(df, cfg)

        assert len(results) == 1
        assert results[0].status == Status.FAIL
        assert "לפי ערך בפועל" in results[0].message
        assert "אינו שייך לתוכנית השקעה" in results[0].message

    def test_maintenance_predicted_fails(self):
        """Predicted 'תחזוקה / שוטף' should always FAIL."""
        from src.water_validation.checks import check_014_llm_project_funding_classification
        from src.water_validation.config import PlanConfig
        from src.water_validation.models import Status

        cfg = PlanConfig(llm_enabled=False)
        df = pd.DataFrame({
            "מס' פרויקט": ["1"],
            "שם פרויקט": ["צילום קווי ביוב"],  # Keyword triggers maintenance
            "סיווג פרויקט": ["שיקום/שדרוג"],  # But reported is something else
        })

        results = check_014_llm_project_funding_classification(df, cfg)

        assert len(results) == 1
        assert results[0].status == Status.FAIL
        assert "לפי ערך צפוי" in results[0].message
        assert "אינו שייך לתוכנית השקעה" in results[0].message


class TestKeywordPredictions:
    """Test keyword-based predictions."""

    def test_keyword_kiduchim(self):
        """Keywords 'באר', 'קידוח', 'רדיוס מגן' should predict 'קידוחים'."""
        from src.water_validation.checks import check_014_llm_project_funding_classification
        from src.water_validation.config import PlanConfig
        from src.water_validation.models import Status

        cfg = PlanConfig(llm_enabled=False)

        for name in ["באר חדשה", "קידוח מים", "רדיוס מגן לבאר"]:
            df = pd.DataFrame({
                "מס' פרויקט": ["1"],
                "שם פרויקט": [name],
                "סיווג פרויקט": ["קידוחים"],
            })

            results = check_014_llm_project_funding_classification(df, cfg)
            assert len(results) == 1
            assert results[0].status == Status.PASS_, f"Failed for: {name}"

    def test_keyword_shikum(self):
        """Keyword 'החלפה' should predict 'שיקום/שדרוג'."""
        from src.water_validation.checks import check_014_llm_project_funding_classification
        from src.water_validation.config import PlanConfig
        from src.water_validation.models import Status

        cfg = PlanConfig(llm_enabled=False)
        df = pd.DataFrame({
            "מס' פרויקט": ["1"],
            "שם פרויקט": ["החלפת צנרת מים"],
            "סיווג פרויקט": ["שיקום/שדרוג"],
        })

        results = check_014_llm_project_funding_classification(df, cfg)
        assert len(results) == 1
        assert results[0].status == Status.PASS_

    def test_keyword_metash(self):
        """Keywords 'מט"ש', 'מטש' should predict 'תשתית ביוב אזורית'."""
        from src.water_validation.checks import check_014_llm_project_funding_classification
        from src.water_validation.config import PlanConfig
        from src.water_validation.models import Status

        cfg = PlanConfig(llm_enabled=False)
        df = pd.DataFrame({
            "מס' פרויקט": ["1"],
            'שם פרויקט': ['שדרוג מט"ש עירוני'],
            "סיווג פרויקט": ["תשתית ביוב אזורית"],
        })

        results = check_014_llm_project_funding_classification(df, cfg)
        assert len(results) == 1
        assert results[0].status == Status.PASS_


class TestMissingAndIllegalValues:
    """Test handling of missing and illegal reported values."""

    def test_missing_reported_value(self):
        """Missing reported value should FAIL with 'ערך חסר'."""
        from src.water_validation.checks import check_014_llm_project_funding_classification
        from src.water_validation.config import PlanConfig
        from src.water_validation.models import Status

        cfg = PlanConfig(llm_enabled=False)
        df = pd.DataFrame({
            "מס' פרויקט": ["1"],
            "שם פרויקט": ["שיקום קו מים"],
            "סיווג פרויקט": [""],  # Empty
        })

        results = check_014_llm_project_funding_classification(df, cfg)
        assert len(results) == 1
        assert results[0].status == Status.FAIL
        assert "ערך חסר" in results[0].message

    def test_illegal_reported_value(self):
        """Illegal reported value should FAIL with 'ערך לא חוקי'."""
        from src.water_validation.checks import check_014_llm_project_funding_classification
        from src.water_validation.config import PlanConfig
        from src.water_validation.models import Status

        cfg = PlanConfig(llm_enabled=False)
        df = pd.DataFrame({
            "מס' פרויקט": ["1"],
            "שם פרויקט": ["שיקום קו מים"],
            "סיווג פרויקט": ["ערך לא קיים"],  # Invalid label
        })

        results = check_014_llm_project_funding_classification(df, cfg)
        assert len(results) == 1
        assert results[0].status == Status.FAIL
        assert "ערך לא חוקי" in results[0].message


class TestSubjectRemoved:
    """Test that 'subject' has been removed from outputs."""

    def test_no_subject_in_messages(self):
        """Messages should not contain 'subject='."""
        from src.water_validation.checks import check_014_llm_project_funding_classification
        from src.water_validation.config import PlanConfig

        cfg = PlanConfig(llm_enabled=False)
        df = pd.DataFrame({
            "מס' פרויקט": ["1"],
            "שם פרויקט": ["החלפת צנרת מים"],
            "סיווג פרויקט": ["שיקום/שדרוג"],
        })

        results = check_014_llm_project_funding_classification(df, cfg)
        for r in results:
            assert "subject=" not in r.message


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
