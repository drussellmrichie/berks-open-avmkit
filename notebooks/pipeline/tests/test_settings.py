"""
Structural validation of settings.json.

These tests guard against accidental edits to the config that would silently
break the pipeline (wrong ind_vars, missing model groups, disabled heuristics
accidentally re-enabled, etc.).
"""

import json
from pathlib import Path

import pytest

SETTINGS_PATH = (
    Path(__file__).parent.parent / "data" / "us-pa-berks" / "in" / "settings.json"
)


@pytest.fixture(scope="module")
def settings():
    with open(SETTINGS_PATH) as f:
        return json.load(f)


class TestSettingsFile:
    def test_file_exists(self):
        assert SETTINGS_PATH.exists(), f"settings.json not found at {SETTINGS_PATH}"

    def test_parses_as_valid_json(self, settings):
        assert isinstance(settings, dict)


class TestModelGroups:
    def test_all_four_groups_present(self, settings):
        groups = settings["modeling"]["model_groups"]
        for expected in ("residential_sf", "residential_mf", "commercial", "vacant"):
            assert expected in groups, f"model_group '{expected}' missing"


def _ind_vars(settings, model_name):
    """Return ind_vars list for a model, handling the default sub-key."""
    model = settings["modeling"]["models"][model_name]
    return model.get("ind_vars") or model.get("default", {}).get("ind_vars", [])


class TestIndVars:
    def test_main_model_has_key_features(self, settings):
        ind_vars = _ind_vars(settings, "main")
        for feature in (
            "bldg_area_finished_sqft",
            "land_area_sqft",
            "dist_to_cbd",
            "bldg_condition_num",
            "neighborhood",
            "school_district",
        ):
            assert feature in ind_vars, f"'{feature}' missing from main ind_vars"

    def test_vacant_model_has_key_features(self, settings):
        ind_vars = _ind_vars(settings, "vacant")
        for feature in ("land_area_sqft", "dist_to_cbd", "neighborhood"):
            assert feature in ind_vars, f"'{feature}' missing from vacant ind_vars"

    def test_no_census_tract_in_main_ind_vars(self, settings):
        """census_tract must not appear — Census API key is not configured."""
        ind_vars = _ind_vars(settings, "main")
        assert "census_tract" not in ind_vars, (
            "census_tract found in main ind_vars but the Census API key is not set; "
            "remove it or run check_census_and_patch.py only after confirming fill rate."
        )

    def test_no_census_tract_in_hedonic_ind_vars(self, settings):
        models = settings["modeling"]["models"]
        if "hedonic" in models:
            ind_vars = _ind_vars(settings, "hedonic")
            assert "census_tract" not in ind_vars, (
                "census_tract in hedonic ind_vars but Census API key is not set."
            )

    def test_no_bldg_quality_num_in_any_model(self, settings):
        """bldg_quality_num does not exist in CAMA Residential — must not appear."""
        for model_name in settings["modeling"]["models"]:
            ind_vars = _ind_vars(settings, model_name)
            assert "bldg_quality_num" not in ind_vars, (
                f"bldg_quality_num in {model_name} ind_vars but this field does not exist "
                "in the Berks CAMA data."
            )

    def test_no_zoning_in_any_model(self, settings):
        """Zoning is not available in Berks GIS data."""
        for model_name in settings["modeling"]["models"]:
            ind_vars = _ind_vars(settings, model_name)
            assert "zoning" not in ind_vars, (
                f"zoning in {model_name} ind_vars but zoning is not available in Berks GIS."
            )


class TestSalesScrutiny:
    def test_heuristics_disabled(self, settings):
        """Heuristics must stay disabled to prevent false-flagging of month-start sales.
        See CLAUDE.md — enabling this suppressed ~934 valid vacant sales."""
        scrutiny = settings.get("analysis", {}).get("sales_scrutiny", {})
        assert scrutiny.get("heuristics_enabled") is False, (
            "sales_scrutiny.heuristics_enabled must be false. "
            "Enabling it causes month-start synthetic dates to collide and removes valid sales."
        )


class TestMetadata:
    def test_valuation_date(self, settings):
        assert settings["modeling"]["metadata"]["valuation_date"] == "2026-01-01"

    def test_fips_code(self, settings):
        """Berks County FIPS must be 42011."""
        fips = str(
            settings.get("data", {})
            .get("process", {})
            .get("enrich", {})
            .get("census", {})
            .get("fips", "")
        )
        assert fips == "42011", f"Expected FIPS 42011, got '{fips}'"
