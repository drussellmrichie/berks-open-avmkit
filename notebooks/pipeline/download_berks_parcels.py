"""
Download Berks County parcel polygons + CAMA assessment attributes and write
berks_parcels.parquet.

Data sources
------------
1. Parcel geometry + base attributes
   Berks County GIS — ParcelSearchTable MapServer, Layer 0 (Parcels)
   https://gis.co.berks.pa.us/arcgis/rest/services/Assess/ParcelSearchTable/MapServer/0
   Fields confirmed: PROPID (parcel ID), ACREAGE, CITYNAME (municipality), PIN,
   NAME1 (owner), PROPERTY_LOCATION, ZIP, SCHOOL, geometry (EPSG:3857)

2. CAMA assessment attributes
   Berks County GIS — ParcelSearchTable MapServer, Layer 3 (CAMA_Master table)
   https://gis.co.berks.pa.us/arcgis/rest/services/Assess/ParcelSearchTable/MapServer/3
   Fields: PARID, TAX_DIST, ACCOUNT, DESCR1-4, NAME1
   Join key: PARID == PROPID

   NOTE: The full CAMA export files (Master, Residential, Commercial) are also
   available on the Berks County Open Data Hub:
     https://opendata.berkspa.gov
   These flat-file exports contain the richest attribute data (land value, building
   value, year built, bedrooms, bathrooms, stories, condition, quality, use code).
   CAMA data dictionary:
     https://www.berkspa.gov/getmedia/d40d71e9-35cd-4a09-9300-6cf0281a6bff/Berks-CAMA-Exports-Metadata.pdf

Usage
-----
Run this script first. It will:
  1. Download parcel geometry + base attributes from the GIS server
  2. Attempt to join CAMA_Master attributes
  3. Print all raw column names so you can update FIELD_MAP and settings.json

Then update FIELD_MAP below (and the right-hand side of settings.json data.load)
to match the actual field names from the printed output, and re-run.

Expected output
---------------
  data/us-pa-berks/in/berks_parcels.parquet

Required standardized columns:
  key, land_area_sqft, bldg_area_finished_sqft, bldg_year_built,
  bldg_condition_num, bldg_quality_num, bldg_rooms_bed, bldg_rooms_bath,
  bldg_stories, bldg_type, category_code, zoning, neighborhood,
  census_tract, assr_land_value, assr_impr_value, assr_market_value,
  is_vacant, geometry (polygon, EPSG:4326)
"""

import math
import os
import time

import geopandas as gpd
import pandas as pd
import requests
from pathlib import Path
from shapely.geometry import shape

os.environ["PYTHONIOENCODING"] = "utf-8"

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Berks County GIS — ParcelSearchTable service
GIS_BASE    = "https://gis.co.berks.pa.us/arcgis/rest/services/Assess/ParcelSearchTable/MapServer"
PARCEL_LAYER = 0   # Parcels (polygon, EPSG:3857)
CAMA_LAYER   = 3   # CAMA_Master (table, no geometry)

PAGE_SIZE = 1000   # server max records per request

# The GIS server uses a User-Agent check; send a browser-like header
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

OUT_DIR  = Path(__file__).parent / "data" / "us-pa-berks" / "in"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH = OUT_DIR / "berks_parcels.parquet"

# ---------------------------------------------------------------------------
# FIELD MAPPING
# ---------------------------------------------------------------------------
# Keys   = standardized openavmkit field names (must match settings.json data.load)
# Values = raw column names from the combined parcel + CAMA download
#
# These are best-guess names based on confirmed GIS metadata and typical Berks
# CAMA export field names. Run once, check the printed column list, then update
# values here and in settings.json data.load to match.
#
# Fields marked REQUIRED must resolve; the others are optional but used if present.
FIELD_MAP = {
    # REQUIRED — from parcel layer
    "key":                     "PROPID",            # unique parcel identifier (UPI)
    # From parcel layer (confirmed fields)
    "land_area_sqft":          "ACREAGE",            # NOTE: in acres — converted below
    "neighborhood":            "CITYNAME",           # municipality (44 in Berks)
    # From CAMA Master or Residential export (typical field names — verify against PDF)
    "category_code":           "CLASSCODE",          # PA standard property class code
    "assr_land_value":         "LNDVAL",             # assessed land value
    "assr_impr_value":         "IMPVAL",             # assessed improvement value
    "assr_market_value":       "APRTOT",             # total appraised/market value
    "bldg_area_finished_sqft": "SFLA",               # finished living area sq ft
    "bldg_year_built":         "YRBLT",              # year built
    "bldg_condition_num":      "CONDITION",          # numeric condition rating
    "bldg_quality_num":        "GRADE",              # numeric quality/grade rating
    "bldg_rooms_bed":          "RMBED",              # bedrooms
    "bldg_rooms_bath":         "FIXBATH",            # full bathrooms
    "bldg_stories":            "STORY",              # number of stories
    "bldg_type":               "STYLE",              # building style/type description
    "zoning":                  "ZONING",             # zoning code (if available)
    "census_tract":            "CENSUS_TRACT",       # census tract (if available)
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_record_count(base: str, layer: int, where: str = "1=1") -> int:
    url = f"{base}/{layer}/query"
    resp = requests.get(
        url,
        headers=HEADERS,
        params={"where": where, "returnCountOnly": "true", "f": "json"},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"ArcGIS error (count): {data['error']}")
    return int(data.get("count", 0))


def _query_page_geojson(base: str, layer: int, offset: int, count: int) -> list:
    """Query a feature layer (with geometry) returning GeoJSON features."""
    url = f"{base}/{layer}/query"
    resp = requests.get(
        url,
        headers=HEADERS,
        params={
            "where": "1=1",
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
        raise RuntimeError(f"ArcGIS error (page offset={offset}): {data['error']}")
    return data.get("features", [])


def _query_page_table(base: str, layer: int, offset: int, count: int) -> list:
    """Query a table layer (no geometry) returning JSON records."""
    url = f"{base}/{layer}/query"
    resp = requests.get(
        url,
        headers=HEADERS,
        params={
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "false",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": count,
        },
        timeout=120,
    )
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"ArcGIS error (table page offset={offset}): {data['error']}")
    return data.get("features", [])


def _paginate(base: str, layer: int, *, has_geometry: bool) -> list:
    total = _get_record_count(base, layer)
    print(f"  Total records: {total:,}")
    if total == 0:
        raise RuntimeError(
            f"No records returned from layer {layer}. "
            f"Inspect the service at {base}/{layer}?f=json."
        )
    pages = math.ceil(total / PAGE_SIZE)
    features = []
    query_fn = _query_page_geojson if has_geometry else _query_page_table
    for i in range(pages):
        offset = i * PAGE_SIZE
        print(f"  Page {i + 1}/{pages}  (offset {offset:,}) ...", end="", flush=True)
        for attempt in range(4):
            try:
                page = query_fn(base, layer, offset, PAGE_SIZE)
                break
            except Exception as exc:
                if attempt == 3:
                    raise
                wait = 2 ** (attempt + 1)
                print(f" retry in {wait}s ({exc}) ...", end="", flush=True)
                time.sleep(wait)
        features.extend(page)
        print(f" {len(page)} records")
        time.sleep(0.25)
    return features


def _features_to_gdf(features: list) -> gpd.GeoDataFrame:
    """Convert GeoJSON feature list to GeoDataFrame (EPSG:4326)."""
    rows = []
    geoms = []
    for f in features:
        rows.append(f.get("properties", {}))
        geoms.append(shape(f["geometry"]) if f.get("geometry") else None)
    gdf = gpd.GeoDataFrame(rows, geometry=geoms, crs="EPSG:4326")
    return gdf


def _features_to_df(features: list) -> pd.DataFrame:
    """Convert table feature list (no geometry) to DataFrame."""
    return pd.DataFrame([f.get("attributes", {}) for f in features])


def _derive_is_vacant(df: pd.DataFrame) -> pd.Series:
    """
    Derive is_vacant from category_code and finished sq ft.

    PA standard codes: 700-799 = Vacant land. Also treat parcels with zero
    finished area and non-commercial codes as effectively vacant.
    Adjust once actual codes are confirmed in the data.
    """
    cat = df.get("category_code", pd.Series("", index=df.index))
    cat = cat.astype(str).str.strip().str.zfill(3)
    sqft = pd.to_numeric(
        df.get("bldg_area_finished_sqft", pd.Series(0, index=df.index)),
        errors="coerce",
    ).fillna(0)
    return (cat.str.startswith("7") | (sqft == 0)).astype(bool)


def _print_columns(df: pd.DataFrame, label: str) -> None:
    print(f"\n{'='*60}")
    print(f"{label} — {len(df.columns)} columns, {len(df):,} rows")
    print(f"{'='*60}")
    for col in sorted(df.columns):
        non_null = df[col].notna().sum() if hasattr(df[col], "notna") else "?"
        print(f"  {col:<45}  ({non_null:,} non-null)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # -----------------------------------------------------------------------
    # Step 1: Download parcel geometry
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print("Step 1: Download parcel polygons (Layer 0)")
    print(f"  {GIS_BASE}/{PARCEL_LAYER}")
    print(f"{'='*60}")
    parcel_features = _paginate(GIS_BASE, PARCEL_LAYER, has_geometry=True)
    parcels = _features_to_gdf(parcel_features)
    _print_columns(parcels, "Parcel layer (raw)")

    # -----------------------------------------------------------------------
    # Step 2: Download CAMA_Master table
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print("Step 2: Download CAMA_Master table (Layer 3)")
    print(f"  {GIS_BASE}/{CAMA_LAYER}")
    print(f"{'='*60}")
    try:
        cama_features = _paginate(GIS_BASE, CAMA_LAYER, has_geometry=False)
        cama = _features_to_df(cama_features)
        _print_columns(cama, "CAMA_Master table (raw)")

        # Join CAMA onto parcels by parcel ID
        # Parcel layer uses PROPID; CAMA table uses PARID
        join_left  = "PROPID"
        join_right = "PARID"
        if join_left in parcels.columns and join_right in cama.columns:
            print(f"\nJoining CAMA on {join_right} -> {join_left} ...")
            # Avoid column name collisions — suffix CAMA-only columns
            cama_cols = [c for c in cama.columns if c != join_right and c not in parcels.columns]
            parcels = parcels.merge(
                cama[[join_right] + cama_cols],
                left_on=join_left, right_on=join_right,
                how="left",
            )
            print(f"  After join: {len(parcels):,} rows, {len(parcels.columns)} cols")
        else:
            print(
                f"\nWARNING: Join columns not found "
                f"(looking for '{join_left}' in parcels, '{join_right}' in CAMA). "
                f"Skipping join — update join_left/join_right if column names differ."
            )
    except Exception as exc:
        print(f"\nWARNING: CAMA_Master download failed ({exc}). Continuing with parcel-only data.")

    # -----------------------------------------------------------------------
    # Step 3: Apply field mapping
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print("Step 3: Apply FIELD_MAP")
    print(f"{'='*60}")

    missing = [raw for std, raw in FIELD_MAP.items() if raw not in parcels.columns]
    if missing:
        print(
            f"WARNING: {len(missing)} mapped source column(s) not found:\n"
            + "\n".join(f"  {c}" for c in missing)
            + "\nUpdate FIELD_MAP above (and settings.json data.load) to match the "
            + "column names printed above."
        )

    present_map = {raw: std for std, raw in FIELD_MAP.items() if raw in parcels.columns}
    keep_raw = list(present_map.keys()) + (["geometry"] if "geometry" in parcels.columns else [])
    out = parcels[keep_raw].rename(columns=present_map)

    # ACREAGE -> sqft conversion (1 acre = 43,560 sq ft)
    if "land_area_sqft" in out.columns:
        out["land_area_sqft"] = pd.to_numeric(out["land_area_sqft"], errors="coerce") * 43_560.0

    # Cast all numeric columns
    num_cols = [
        "land_area_sqft", "bldg_area_finished_sqft", "bldg_year_built",
        "bldg_condition_num", "bldg_quality_num", "bldg_rooms_bed",
        "bldg_rooms_bath", "bldg_stories", "assr_land_value",
        "assr_impr_value", "assr_market_value",
    ]
    for col in num_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    out["is_vacant"] = _derive_is_vacant(out)

    # -----------------------------------------------------------------------
    # Step 4: Write output
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print("Step 4: Write output")
    print(f"{'='*60}")
    print(f"Output columns ({len(out.columns)}): {list(out.columns)}")
    print(f"Writing {len(out):,} rows to:\n  {OUT_PATH}")
    out.to_parquet(OUT_PATH, index=False)
    print("Done.")
    print(
        "\nNext steps:\n"
        "  1. Compare the column names printed above against FIELD_MAP.\n"
        "  2. Update FIELD_MAP values (and settings.json data.load right-hand side)\n"
        "     to match the actual column names from the printed output.\n"
        "  3. Download the full CAMA Residential/Commercial flat-file exports from\n"
        "     https://opendata.berkspa.gov for richer building attributes,\n"
        "     then merge on PARID and extend FIELD_MAP accordingly.\n"
        "  4. Review the CAMA data dictionary PDF for exact field definitions:\n"
        "     https://www.berkspa.gov/getmedia/d40d71e9-35cd-4a09-9300-6cf0281a6bff/Berks-CAMA-Exports-Metadata.pdf"
    )


if __name__ == "__main__":
    main()
