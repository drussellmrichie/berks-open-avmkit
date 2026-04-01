"""
Tests for Berks County enrichment helpers: add_dist_to_cbd, fill_universe_nulls.
"""

import numpy as np
import pandas as pd
import pytest

from berks_helpers import CITY_HALL_LAT, CITY_HALL_LON, add_dist_to_cbd, fill_universe_nulls


# ---------------------------------------------------------------------------
# add_dist_to_cbd
# ---------------------------------------------------------------------------

class TestAddDistToCbd:
    def test_city_hall_to_itself_is_zero(self):
        df = pd.DataFrame({"latitude": [CITY_HALL_LAT], "longitude": [CITY_HALL_LON]})
        result = add_dist_to_cbd(df)
        assert result["dist_to_cbd"].iloc[0] == pytest.approx(0.0, abs=1e-6)

    def test_known_distance_to_philadelphia(self):
        # Philadelphia City Hall is roughly 50 miles SE of Reading
        df = pd.DataFrame({"latitude": [39.9526], "longitude": [-75.1652]})
        result = add_dist_to_cbd(df)
        assert result["dist_to_cbd"].iloc[0] == pytest.approx(50.0, abs=5.0)

    def test_distance_is_non_negative(self):
        df = pd.DataFrame({
            "latitude":  [40.0, 40.5, 39.8],
            "longitude": [-76.0, -75.5, -75.0],
        })
        result = add_dist_to_cbd(df)
        assert (result["dist_to_cbd"] >= 0).all()

    def test_does_not_mutate_input(self):
        df = pd.DataFrame({"latitude": [40.0], "longitude": [-76.0]})
        add_dist_to_cbd(df)
        assert "dist_to_cbd" not in df.columns

    def test_output_column_added(self):
        df = pd.DataFrame({"latitude": [40.0], "longitude": [-76.0]})
        result = add_dist_to_cbd(df)
        assert "dist_to_cbd" in result.columns

    def test_dtype_is_float64(self):
        df = pd.DataFrame({"latitude": [40.3], "longitude": [-75.9]})
        result = add_dist_to_cbd(df)
        assert result["dist_to_cbd"].dtype == np.float64

    def test_multiple_rows_vectorised(self):
        # Same point twice → same distance
        df = pd.DataFrame({
            "latitude":  [CITY_HALL_LAT, CITY_HALL_LAT],
            "longitude": [CITY_HALL_LON, CITY_HALL_LON],
        })
        result = add_dist_to_cbd(df)
        assert result["dist_to_cbd"].iloc[0] == pytest.approx(result["dist_to_cbd"].iloc[1])

    def test_farther_point_has_larger_distance(self):
        # Point 0.1° away (~7 mi) vs. point 1° away (~69 mi) from City Hall
        close = pd.DataFrame({"latitude": [CITY_HALL_LAT + 0.1], "longitude": [CITY_HALL_LON]})
        far   = pd.DataFrame({"latitude": [CITY_HALL_LAT + 1.0], "longitude": [CITY_HALL_LON]})
        assert add_dist_to_cbd(close)["dist_to_cbd"].iloc[0] < \
               add_dist_to_cbd(far)["dist_to_cbd"].iloc[0]


# ---------------------------------------------------------------------------
# fill_universe_nulls
# ---------------------------------------------------------------------------

def _make_universe(n_improved=10, n_vacant=5, condition_val=4.0):
    """Build a minimal universe DataFrame for testing."""
    n = n_improved + n_vacant
    return pd.DataFrame({
        "is_vacant":              [False] * n_improved + [True] * n_vacant,
        "model_group":            ["residential_sf"] * n,
        "bldg_condition_num":     [condition_val] * n_improved + [np.nan] * n_vacant,
        "bldg_stories":           [2.0] * n_improved + [np.nan] * n_vacant,
        "bldg_rooms_bath":        [1.0] * n_improved + [np.nan] * n_vacant,
        "bldg_rooms_bath_half":   [0.0] * n_improved + [np.nan] * n_vacant,
        "bldg_rooms_bed":         [3.0] * n_improved + [np.nan] * n_vacant,
        "bldg_garage_cars":       [1.0] * n_improved + [np.nan] * n_vacant,
        "bldg_fireplaces":        [0.0] * n_improved + [np.nan] * n_vacant,
        "bldg_area_finished_sqft":[1500.0] * n_improved + [np.nan] * n_vacant,
    })


class TestFillUniverseNulls:
    def test_fills_null_in_improved_parcel_with_group_median(self):
        df = _make_universe()
        df.loc[0, "bldg_condition_num"] = np.nan  # poke a null into an improved row
        result = fill_universe_nulls(df)
        # Median of remaining 9 improved rows (all 4.0) → 4.0
        assert result["bldg_condition_num"].iloc[0] == pytest.approx(4.0)

    def test_vacant_parcels_nulls_filled_with_improved_median(self):
        df = _make_universe()
        result = fill_universe_nulls(df)
        # Vacant rows (indices 10-14) had NaN; should now equal improved median
        for i in range(10, 15):
            assert result["bldg_condition_num"].iloc[i] == pytest.approx(4.0)

    def test_bldg_area_zero_filled_not_median_filled(self):
        df = _make_universe()
        df.loc[0, "bldg_area_finished_sqft"] = np.nan
        result = fill_universe_nulls(df)
        assert result["bldg_area_finished_sqft"].iloc[0] == pytest.approx(0.0)

    def test_per_model_group_medians_applied_separately(self):
        """Each model group should use only its own improved parcels for the median."""
        df = pd.DataFrame({
            "is_vacant":          [False, False, False, False],
            "model_group":        ["residential_sf", "residential_sf", "commercial", "commercial"],
            "bldg_condition_num": [2.0, np.nan, 5.0, np.nan],
            "bldg_area_finished_sqft": [1000.0, np.nan, 2000.0, np.nan],
        })
        result = fill_universe_nulls(df)
        assert result["bldg_condition_num"].iloc[1] == pytest.approx(2.0)
        assert result["bldg_condition_num"].iloc[3] == pytest.approx(5.0)

    def test_absent_column_skipped_without_error(self):
        """Columns in _IMPR_FILL_MEDIAN that are absent from the DataFrame are ignored."""
        df = pd.DataFrame({
            "is_vacant":   [False],
            "model_group": ["residential_sf"],
            "bldg_area_finished_sqft": [1000.0],
            # No bldg_condition_num, bldg_stories, etc.
        })
        result = fill_universe_nulls(df)  # must not raise
        assert "bldg_condition_num" not in result.columns

    def test_no_nulls_in_data_unchanged(self):
        df = _make_universe()
        # Manually fill the vacant rows to remove NaNs
        for col in ["bldg_condition_num", "bldg_stories", "bldg_rooms_bath",
                    "bldg_rooms_bath_half", "bldg_rooms_bed", "bldg_garage_cars",
                    "bldg_fireplaces", "bldg_area_finished_sqft"]:
            df[col] = df[col].fillna(0.0)
        result = fill_universe_nulls(df)
        pd.testing.assert_series_equal(result["bldg_condition_num"], df["bldg_condition_num"])

    def test_does_not_mutate_input(self):
        df = _make_universe()
        original_nulls = df["bldg_condition_num"].isna().sum()
        fill_universe_nulls(df)
        assert df["bldg_condition_num"].isna().sum() == original_nulls

    def test_fallback_to_global_improved_median_when_group_is_all_vacant(self):
        """A group with no improved parcels falls back to the global improved median."""
        df = pd.DataFrame({
            "is_vacant":          [False, True],
            "model_group":        ["residential_sf", "vacant_only"],
            "bldg_condition_num": [4.0, np.nan],
            "bldg_area_finished_sqft": [1000.0, np.nan],
        })
        result = fill_universe_nulls(df)
        # "vacant_only" group has no improved parcels; global improved median = 4.0
        assert result["bldg_condition_num"].iloc[1] == pytest.approx(4.0)

    def test_multiple_groups_multiple_nulls(self):
        """Regression: filling works correctly across 3 groups."""
        df = pd.DataFrame({
            "is_vacant":          [False, False, False, False, False, False],
            "model_group":        ["a", "a", "b", "b", "c", "c"],
            "bldg_condition_num": [2.0, np.nan, 4.0, np.nan, 6.0, np.nan],
            "bldg_area_finished_sqft": [500.0, np.nan, 1000.0, np.nan, 2000.0, np.nan],
        })
        result = fill_universe_nulls(df)
        assert result["bldg_condition_num"].iloc[1] == pytest.approx(2.0)
        assert result["bldg_condition_num"].iloc[3] == pytest.approx(4.0)
        assert result["bldg_condition_num"].iloc[5] == pytest.approx(6.0)
