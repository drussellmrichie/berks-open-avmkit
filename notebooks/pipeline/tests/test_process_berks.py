"""
Tests for process_berks.py validation helpers: _dtype_category, _check_schema.
"""

import pandas as pd
import pytest

from process_berks import _dtype_category, _check_schema


# ---------------------------------------------------------------------------
# _dtype_category
# ---------------------------------------------------------------------------

class TestDtypeCategory:
    def test_integer_series_is_numeric(self):
        assert _dtype_category(pd.Series([1, 2, 3])) == "numeric"

    def test_float_series_is_numeric(self):
        assert _dtype_category(pd.Series([1.0, 2.5])) == "numeric"

    def test_object_series_is_string(self):
        assert _dtype_category(pd.Series(["a", "b"])) == "string"

    def test_bool_series_is_bool(self):
        assert _dtype_category(pd.Series([True, False])) == "bool"

    def test_datetime_series_is_datetime(self):
        s = pd.Series(pd.to_datetime(["2022-01-01", "2023-06-15"]))
        assert _dtype_category(s) == "datetime"

    def test_nullable_int_is_numeric(self):
        s = pd.array([1, 2, None], dtype="Int64")
        assert _dtype_category(pd.Series(s)) == "numeric"

    def test_nullable_bool_is_bool(self):
        s = pd.array([True, False, None], dtype="boolean")
        assert _dtype_category(pd.Series(s)) == "bool"


# ---------------------------------------------------------------------------
# _check_schema
# ---------------------------------------------------------------------------

def _base_df():
    return pd.DataFrame({
        "key":        ["A", "B"],
        "sale_price": [100_000.0, 200_000.0],
        "valid_sale": [True, False],
        "sale_date":  pd.to_datetime(["2022-01-01", "2023-06-15"]),
    })


class TestCheckSchema:
    def test_fully_valid_schema_returns_no_errors(self):
        df = _base_df()
        required = {
            "key":        "string",
            "sale_price": "numeric",
            "valid_sale": "bool",
            "sale_date":  "datetime",
        }
        assert _check_schema(df, required, "test") == []

    def test_missing_column_reported(self):
        df = _base_df()
        errors = _check_schema(df, {"nonexistent": "numeric"}, "test")
        assert len(errors) == 1
        assert "MISSING" in errors[0]
        assert "nonexistent" in errors[0]

    def test_wrong_dtype_reported(self):
        df = _base_df()
        # "key" is string but we claim it should be numeric
        errors = _check_schema(df, {"key": "numeric"}, "test")
        assert len(errors) == 1
        assert "WRONG dtype" in errors[0]
        assert "key" in errors[0]

    def test_multiple_errors_all_reported(self):
        df = _base_df()
        required = {
            "missing_a": "numeric",
            "missing_b": "string",
            "key":       "numeric",   # wrong dtype
        }
        errors = _check_schema(df, required, "test")
        assert len(errors) == 3

    def test_subset_of_required_columns_ok(self):
        df = _base_df()
        # Only check a subset of columns
        errors = _check_schema(df, {"key": "string"}, "test")
        assert errors == []

    def test_empty_required_dict_returns_no_errors(self):
        df = _base_df()
        assert _check_schema(df, {}, "test") == []

    def test_empty_dataframe_reports_missing_columns(self):
        df = pd.DataFrame()
        errors = _check_schema(df, {"key": "string"}, "test")
        assert any("MISSING" in e for e in errors)
