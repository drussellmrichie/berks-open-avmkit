"""
Download Berks County parcel polygons from PASDA ArcGIS REST service
and write berks_parcels.parquet.

Data source: PASDA (PA Spatial Data Access) — PA Parcels MapServer
  https://www.pasda.psu.edu/
  REST service: https://apps.pasda.psu.edu/arcgis/rest/services/PA_Parcels/MapServer

Alternative sources (if PASDA is unavailable or missing assessment attributes):
  - Berks County Open Data Hub: https://opendata.berkspa.gov
  - Direct GIS request: gis@berkspa.gov

Expected output:
  data/us-pa-berks/in/berks_parcels.parquet

Required standardized columns (mapped from raw PASDA fields via FIELD_MAP below):
  key, land_area_sqft, bldg_area_finished_sqft, bldg_year_built,
  bldg_condition_num, bldg_quality_num, bldg_rooms_bed, bldg_rooms_bath,
  bldg_stories, bldg_type, category_code, zoning, neighborhood,
  census_tract, assr_land_value, assr_impr_value, assr_market_value,
  is_vacant, geometry (polygon, EPSG:4326)

Usage:
  Run this script first to download data and print raw column names.
  Then update FIELD_MAP below (and settings.json data.load) to match.
"""

import math
import os
import time

import geopandas as gpd
import pandas as pd
import requests
from pathlib import Path

os.environ["PYTHONIOENCODING"] = "utf-8"

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# PASDA ArcGIS MapServer for PA Parcels.
# Layer 0 is parcel polygons with assessment attributes.
PASDА_BASE = "https://apps.pasda.psu.edu/arcgis/rest/services/PA_Parcels/MapServer"
LAYER = 0

# County filter — PASDA PA_Parcels commonly uses COUNTY_NAME.
# If the first run fails with "invalid field", inspect the service metadata at
# {PASDA_BASE}/{LAYER}?f=json and adjust COUNTY_FIELD / COUNTY_VALUE accordingly.
COUNTY_FIELD = "COUNTY_NAME"
COUNTY_VALUE = "BERKS"

PAGE_SIZE = 1000  # ArcGIS default max records per page; lower if timeouts occur

OUT_DIR = Path(__file__).parent / "data" / "us-pa-berks" / "in"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH = OUT_DIR / "berks_parcels.parquet"

# ---------------------------------------------------------------------------
# FIELD MAPPING
# ---------------------------------------------------------------------------
# Keys   = standardized openavmkit field names (must match settings.json data.load)
# Values = raw column names in the PASDA download
#
# Run this script once without changing anything; it will print all available
# columns. Then update the values below to match the actual PASDA field names
# and re-run. Also update the right-hand side of settings.json data.load to
# match these same raw field names.
#
# Fields marked (REQUIRED) must be present; others are used if available.
FIELD_MAP = {
    # REQUIRED
    "key":                     "PARCEL_ID",        # unique parcel identifier
    "category_code":           "PROP_CLASS",        # PA standard property class code
    # Geometry / area
    "land_area_sqft":          "SHAPE_AREA",        # parcel area in sq ft (or SHAPE_Area)
    # Building characteristics
    "bldg_area_finished_sqft": "SQFT_FINISHED",     # finished living area
    "bldg_year_built":         "YEAR_BUILT",
    "bldg_condition_num":      "CONDITION",         # numeric 1-8 (or similar)
    "bldg_quality_num":        "QUALITY",           # numeric 1-7 (or similar)
    "bldg_rooms_bed":          "BEDROOMS",
    "bldg_rooms_bath":         "BATHROOMS",
    "bldg_stories":            "STORIES",
    "bldg_type":               "BUILDING_DESC",     # building description / type code
    # Location / classification
    "zoning":                  "ZONING",
    "neighborhood":            "MUNICIPALITY",      # municipality is the strongest geo
                                                    # proxy in Berks County (44 munis)
    "census_tract":            "CENSUS_TRACT",
    # Assessed values
    "assr_land_value":         "LAND_VALUE",
    "assr_impr_value":         "IMPR_VALUE",
    "assr_market_value":       "TOTAL_VALUE",
}


def _derive_is_vacant(df: pd.DataFrame) -> pd.Series:
    """
    Derive is_vacant from category_code and finished square footage.

    PA standard codes: 700-799 = Vacant; also treat parcels with 0 finished
    area and non-commercial codes as vacant.  Adjust thresholds once actual
    codes are confirmed in the data.
    """
    cat = df["category_code"].astype(str).str.strip().str.zfill(3)
    sqft = pd.to_numeric(
        df.get("bldg_area_finished_sqft", pd.Series(0, index=df.index)),
        errors="coerce",
    ).fillna(0)
    return (cat.str.startswith("7") | (sqft == 0)).astype(bool)


# ---------------------------------------------------------------------------
# ArcGIS REST helpers
# ---------------------------------------------------------------------------

def _get_record_count(base: str, layer: int, where: str) -> int:
    url = f"{base}/{layer}/query"
    resp = requests.get(
        url,
        params={"where": where, "returnCountOnly": "true", "f": "json"},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"ArcGIS error: {data['error']}")
    return int(data.get("count", 0))


def _query_page(
    base: str, layer: int, where: str, offset: int, count: int
) -> list:
    url = f"{base}/{layer}/query"
    resp = requests.get(
        url,
        params={
            "where": where,
            "outFields": "*",
            "returnGeometry": "true",
            "outSR": "4326",
            "f": "geojson",
            "resultOffset": offset,
            "resultRecordCount": count,
        },
        timeout=120,
    )
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"ArcGIS error on page offset={offset}: {data['error']}")
    return data.get("features", [])


def _download_all_features(base: str, layer: int, where: str) -> list:
    total = _get_record_count(base, layer, where)
    print(f"  Total records matching filter: {total:,}")
    if total == 0:
        raise RuntimeError(
            f"No records returned for filter '{where}'. "
            f"Inspect the service at {base}/{layer}?f=json to verify field names."
        )

    pages = math.ceil(total / PAGE_SIZE)
    features = []
    for i in range(pages):
        offset = i * PAGE_SIZE
        print(f"  Page {i + 1}/{pages}  (offset {offset:,}) ...", end="", flush=True)
        for attempt in range(3):
            try:
                page = _query_page(base, layer, where, offset, PAGE_SIZE)
                break
            except Exception as exc:
                if attempt == 2:
                    raise
                wait = 5 * (attempt + 1)
                print(f" error ({exc}), retrying in {wait}s ...", end="", flush=True)
                time.sleep(wait)
        features.extend(page)
        print(f" {len(page)} features")
        time.sleep(0.3)  # be polite to PASDA's server

    return features


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    where = f"{COUNTY_FIELD} = '{COUNTY_VALUE}'"
    print(f"\nDownloading Berks County parcels from PASDA ...")
    print(f"  Service : {PASDA_BASE}")
    print(f"  Layer   : {LAYER}")
    print(f"  Filter  : {where}")

    features = _download_all_features(PASDA_BASE, LAYER, where)
    print(f"\nTotal features downloaded: {len(features):,}")

    gdf = gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")

    # Print raw columns so the developer can verify / update FIELD_MAP
    print(f"\n{'='*60}")
    print(f"Raw columns in downloaded data ({len(gdf.columns)}):")
    for col in sorted(gdf.columns):
        non_null = gdf[col].notna().sum()
        print(f"  {col:<40}  ({non_null:,} non-null / {len(gdf):,})")
    print(f"{'='*60}")

    # Warn about any mapped source columns that are missing
    missing = [v for k, v in FIELD_MAP.items() if v not in gdf.columns]
    if missing:
        print(
            f"\nWARNING: {len(missing)} mapped source column(s) not found in the data:\n"
            + "\n".join(f"  {c}" for c in missing)
            + "\nUpdate FIELD_MAP above to match the actual column names printed above."
        )

    # Keep only columns that exist and rename to standardized names
    present_map = {raw: std for std, raw in FIELD_MAP.items() if raw in gdf.columns}
    keep_raw = list(present_map.keys()) + (["geometry"] if "geometry" in gdf.columns else [])
    out = gdf[keep_raw].rename(columns=present_map)

    out["is_vacant"] = _derive_is_vacant(out)

    # Cast numeric columns (gracefully handle string values)
    num_cols = [
        "land_area_sqft", "bldg_area_finished_sqft", "bldg_year_built",
        "bldg_condition_num", "bldg_quality_num", "bldg_rooms_bed",
        "bldg_rooms_bath", "bldg_stories", "assr_land_value",
        "assr_impr_value", "assr_market_value",
    ]
    for col in num_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    print(f"\nOutput columns ({len(out.columns)}): {list(out.columns)}")
    print(f"Writing {len(out):,} rows to:\n  {OUT_PATH}")
    out.to_parquet(OUT_PATH, index=False)
    print("Done.")


if __name__ == "__main__":
    main()
