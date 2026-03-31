"""
Validate the pipeline input parquets produced by download_berks_parcels.py:

  data/us-pa-berks/in/berks_parcels.parquet   (universe — all parcels)
  data/us-pa-berks/in/sales.parquet           (arm's-length sales)

This script does NOT re-download or re-process raw data — that is handled
entirely by download_berks_parcels.py.  Run this script after the download
to confirm the produced files are schema-complete and ready for the pipeline.

Exit codes
----------
  0  — all checks passed
  1  — one or more checks failed (details printed to stdout)

Usage
-----
Run from notebooks/pipeline/:
    python process_berks.py
"""

import os
import sys

os.environ["PYTHONIOENCODING"] = "utf-8"

import pandas as pd
import geopandas as gpd
from pathlib import Path

# ---------------------------------------------------------------------------
# Expected schemas (must match settings.json data.load and pipeline contracts)
# ---------------------------------------------------------------------------

# Required columns and their expected dtypes (broad category: numeric / string / bool / geometry)
PARCELS_REQUIRED = {
    "key":                    "string",
    "land_area_sqft":         "numeric",
    "bldg_area_finished_sqft":"numeric",
    "bldg_year_built":        "numeric",
    "bldg_condition_num":     "numeric",
    "bldg_rooms_bed":         "numeric",
    "bldg_rooms_bath":        "numeric",
    "bldg_rooms_bath_half":   "numeric",
    "bldg_stories":           "numeric",
    "bldg_type":              "string",
    "bldg_ext_wall":          "string",
    "bldg_bsmt_type":         "string",
    "bldg_garage_cars":       "numeric",
    "bldg_fireplaces":        "numeric",
    "category_code":          "string",
    "neighborhood":           "string",
    "assr_land_value":        "numeric",
    "assr_impr_value":        "numeric",
    "assr_market_value":      "numeric",
    "is_vacant":              "bool",
    "geometry":               "geometry",
}

SALES_REQUIRED = {
    "key_sale":   "string",
    "key":        "string",
    "sale_date":  "datetime",
    "sale_price": "numeric",
    "valid_sale": "bool",
    "vacant_sale":"bool",
}

IN_DIR = Path(__file__).parent / "data" / "us-pa-berks" / "in"
PARCELS_PATH = IN_DIR / "berks_parcels.parquet"
SALES_PATH   = IN_DIR / "sales.parquet"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dtype_category(series: pd.Series) -> str:
    if hasattr(series, "geom_type"):
        return "geometry"
    if pd.api.types.is_bool_dtype(series):
        return "bool"
    if pd.api.types.is_numeric_dtype(series):
        return "numeric"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"
    return "string"


def _check_schema(df: pd.DataFrame, required: dict, label: str) -> list[str]:
    """Return list of error strings (empty = all good)."""
    errors = []
    for col, expected_kind in required.items():
        if col not in df.columns:
            errors.append(f"  MISSING column: {col}")
            continue
        actual_kind = _dtype_category(df[col])
        if actual_kind != expected_kind:
            errors.append(
                f"  WRONG dtype for '{col}': expected {expected_kind}, got {actual_kind}"
            )
    return errors


def _print_section(title: str):
    print(f"\n{'='*60}")
    print(title)
    print("="*60)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_parcels() -> bool:
    _print_section("Parcel universe (berks_parcels.parquet)")
    if not PARCELS_PATH.exists():
        print(f"  FAIL — file not found: {PARCELS_PATH}")
        print("  Run download_berks_parcels.py first.")
        return False

    gdf = gpd.read_parquet(PARCELS_PATH)
    print(f"  Rows      : {len(gdf):,}")
    print(f"  Columns   : {len(gdf.columns)}")

    errors = _check_schema(gdf, PARCELS_REQUIRED, "parcels")
    if errors:
        print("  Schema errors:")
        for e in errors:
            print(e)
    else:
        print("  Schema    : OK (all required columns present)")

    # Key uniqueness
    n_dupes = gdf["key"].duplicated().sum() if "key" in gdf.columns else 0
    if n_dupes:
        print(f"  WARN — {n_dupes:,} duplicate 'key' values (pipeline will drop them)")
    else:
        print("  Key unique: YES")

    # Coverage stats
    if "is_vacant" in gdf.columns:
        n_vacant   = int(gdf["is_vacant"].sum())
        n_improved = len(gdf) - n_vacant
        print(f"  Improved  : {n_improved:,}  |  Vacant: {n_vacant:,}")

    if "category_code" in gdf.columns:
        print("  Category code distribution:")
        for code, cnt in gdf["category_code"].value_counts().head(10).items():
            print(f"    {code:>6}  {cnt:>8,}")

    if "neighborhood" in gdf.columns:
        n_nbhd = gdf["neighborhood"].nunique()
        print(f"  Neighborhoods (municipalities): {n_nbhd}")

    # Null rates for key model fields
    print("  Null rates for key model fields:")
    key_fields = [
        "bldg_area_finished_sqft", "bldg_condition_num",
        "bldg_rooms_bed", "bldg_rooms_bath", "bldg_stories",
        "bldg_garage_cars", "bldg_fireplaces",
        "assr_land_value", "assr_market_value",
    ]
    for f in key_fields:
        if f in gdf.columns:
            null_pct = 100.0 * gdf[f].isna().mean()
            print(f"    {f:<30} {null_pct:5.1f}% null")

    # CRS
    if hasattr(gdf, "crs") and gdf.crs is not None:
        print(f"  CRS       : {gdf.crs}")
    else:
        print("  WARN — no CRS set on geometry column")

    return len(errors) == 0


def validate_sales() -> bool:
    _print_section("Sales (sales.parquet)")
    if not SALES_PATH.exists():
        print(f"  FAIL — file not found: {SALES_PATH}")
        print("  Run download_berks_parcels.py first.")
        return False

    df = pd.read_parquet(SALES_PATH)
    print(f"  Rows (total)   : {len(df):,}")

    errors = _check_schema(df, SALES_REQUIRED, "sales")
    if errors:
        print("  Schema errors:")
        for e in errors:
            print(e)
    else:
        print("  Schema         : OK (all required columns present)")

    # Key_sale uniqueness
    if "key_sale" in df.columns:
        n_dupes = df["key_sale"].duplicated().sum()
        if n_dupes:
            print(f"  WARN — {n_dupes:,} duplicate key_sale values (pipeline will drop them)")
        else:
            print("  key_sale unique: YES")

    # Valid sale counts
    if "valid_sale" in df.columns:
        n_valid = int(df["valid_sale"].sum())
        print(f"  Valid sales    : {n_valid:,}  ({100*n_valid/len(df):.1f}%)")

    # Date range
    if "sale_date" in df.columns:
        dates = pd.to_datetime(df["sale_date"], errors="coerce")
        print(f"  Date range     : {dates.min().date()} to {dates.max().date()}")
        yr_counts = dates.dt.year.value_counts().sort_index()
        print("  Sales per year :")
        for yr, cnt in yr_counts.items():
            print(f"    {int(yr)}  {cnt:>8,}")

    # Price range
    if "sale_price" in df.columns:
        valid_mask = df.get("valid_sale", pd.Series(True, index=df.index))
        prices = df.loc[valid_mask, "sale_price"]
        print(f"  Price (valid)  : min=${prices.min():,.0f}  median=${prices.median():,.0f}  max=${prices.max():,.0f}")

    return len(errors) == 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    print("process_berks.py — pipeline input validation")
    print(f"Looking in: {IN_DIR}")

    parcels_ok = validate_parcels()
    sales_ok   = validate_sales()

    _print_section("Summary")
    status = {
        "berks_parcels.parquet": "PASS" if parcels_ok else "FAIL",
        "sales.parquet":         "PASS" if sales_ok   else "FAIL",
    }
    for fname, result in status.items():
        print(f"  {result}  {fname}")

    if parcels_ok and sales_ok:
        print("\nAll checks passed — ready to run run_01_assemble.py")
        return 0
    else:
        print("\nOne or more checks failed — review output above before running the pipeline")
        return 1


if __name__ == "__main__":
    sys.exit(main())
