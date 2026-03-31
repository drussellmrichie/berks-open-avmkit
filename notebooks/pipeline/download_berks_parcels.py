"""
Download Berks County parcel data and write:
  - berks_parcels.parquet  (universe of all parcels)
  - sales.parquet          (sales extracted from CAMA_Master history)

Data sources (all downloaded programmatically via ArcGIS REST API)
------------------------------------------------------------------
1. Parcel geometry + base attributes
   Berks County GIS — ParcelSearchTable MapServer, Layer 0
   https://gis.co.berks.pa.us/arcgis/rest/services/Assess/ParcelSearchTable/MapServer/0
   Fields used: PROPID, ACREAGE, MUNICIPALNAME, CLASS

2. CAMA_Master assessment data
   Berks County GIS — ParcelSearchTable MapServer, Layer 3
   Fields used: PARID, LAND_VALUE, BLDG_VALUE, TOTAL_VALUE

3. CAMA Residential building attributes + sale history
   Berks County ArcGIS Online FeatureServer, Layer 15
   https://services3.arcgis.com/dGYe1jDYrTw1wwpc/arcgis/rest/services/
       Berks_Assessment_CAMA_Residential_File/FeatureServer/15
   Fields used: PARID, SFLA, YRBLT, PHYCOND, BEDROOMS, FULLBATHS, HALFBATHS,
                STORIES, STYLE, EXTWALL, BSMT, BASE_GARAGE,
                WBFP_OPENINGS, MET_FIREPL,
                PRICE, SALEDT, SALEYR1-3, SALEMTH1-3, SALEPR1-3

Field names confirmed against Berks CAMA Exports Metadata PDF (1/6/2023).

CLASS (Assessed Class) codes: R=Residential, A=Apartment, C=Commercial,
  I=Industrial, F=Farm, E=Exempt, FC=Farm Commercial, UE=Public Utility Exempt,
  UT=Public Utility Taxable

PHYCOND (Physical Condition) codes: VG=Very Good, GD=Good, AV=Average,
  FR=Fair, PR=Poor, US=Unsound  →  mapped to 6/5/4/3/2/1

NOTE: No quality/grade field exists in CAMA Residential.
  bldg_quality_num is therefore not produced.

Usage
-----
Run from notebooks/pipeline/:
    python download_berks_parcels.py

Expected outputs
----------------
  data/us-pa-berks/in/berks_parcels.parquet
  data/us-pa-berks/in/sales.parquet
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

# Berks County GIS server
GIS_BASE     = "https://gis.co.berks.pa.us/arcgis/rest/services/Assess/ParcelSearchTable/MapServer"
PARCEL_LAYER = 0   # Parcels (polygon, EPSG:3857)
CAMA_LAYER   = 3   # CAMA_Master (table, no geometry)

# Berks County ArcGIS Online — CAMA Residential
CAMA_RES_BASE  = "https://services3.arcgis.com/dGYe1jDYrTw1wwpc/arcgis/rest/services/Berks_Assessment_CAMA_Residential_File/FeatureServer"
CAMA_RES_LAYER = 15

PAGE_SIZE = 1000   # records per request

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

OUT_DIR      = Path(__file__).parent / "data" / "us-pa-berks" / "in"
OUT_DIR.mkdir(parents=True, exist_ok=True)
PARCELS_PATH = OUT_DIR / "berks_parcels.parquet"
SALES_PATH   = OUT_DIR / "sales.parquet"

# ---------------------------------------------------------------------------
# Field mapping  (source column → standardized name)
# ---------------------------------------------------------------------------
# All source field names confirmed against CAMA Exports Metadata PDF.

FIELD_MAP = {
    # Parcel layer (Layer 0)
    "key":            "PROPID",        # Unique Parcel ID (UPI)
    "land_area_sqft": "ACREAGE",       # Acres — converted to sqft below
    "neighborhood":   "MUNICIPALNAME", # Municipality name (44 in Berks)
    "category_code":  "CLASS",         # Assessed class: R/A/C/I/F/E/UE/UT
    "school_district": "SCHOOL",       # School district code (01–20)
    # CAMA_Master (Layer 3)
    "assr_land_value":   "LAND_VALUE",  # Land Total
    "assr_impr_value":   "BLDG_VALUE",  # Building Total
    "assr_market_value": "TOTAL_VALUE", # Assessed Total
    # CAMA Residential (FeatureServer/15)
    "bldg_area_finished_sqft": "SFLA",       # Total Sq Ft Living Area
    "bldg_year_built":         "YRBLT",      # Year Built
    "bldg_condition_num":      "PHYCOND",    # Physical Condition (mapped → numeric below)
    "bldg_rooms_bed":          "BEDROOMS",   # Number of Bedrooms
    "bldg_rooms_bath":         "FULLBATHS",  # Number of Full Baths
    "bldg_rooms_bath_half":    "HALFBATHS",  # Number of Half Baths
    "bldg_stories":            "STORIES",    # Story Height
    "bldg_type":               "STYLE",      # Architectural Style code
    "bldg_ext_wall":           "EXTWALL",    # Exterior Wall type code
    "bldg_bsmt_type":          "BSMT",       # Basement / Lower Level code
    "bldg_garage_cars":        "BASE_GARAGE",# Basement Garage — Number of Cars
    # bldg_fireplaces: derived below from WBFP_OPENINGS + MET_FIREPL
    # bldg_quality_num: not available — no grade/quality field in CAMA Residential
    # census_tract: added by openavmkit Census enrichment
    # is_vacant: derived below from SFLA and CLASS
}

# Physical condition text codes → numeric (1=Unsound … 6=Very Good)
PHYCOND_MAP = {"US": 1, "PR": 2, "FR": 3, "AV": 4, "GD": 5, "VG": 6}

# Earliest sale year to include in sales.parquet
SALES_MIN_YEAR = 2018

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _get(url: str, params: dict) -> dict:
    resp = requests.get(url, headers=HEADERS, params=params, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"ArcGIS error: {data['error']}  url={url}")
    return data


def _get_record_count(base: str, layer: int, where: str = "1=1") -> int:
    data = _get(
        f"{base}/{layer}/query",
        {"where": where, "returnCountOnly": "true", "f": "json"},
    )
    return int(data.get("count", 0))


def _query_page_geojson(base: str, layer: int, offset: int, count: int) -> list:
    data = _get(
        f"{base}/{layer}/query",
        {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true",
            "outSR": "4326",
            "f": "geojson",
            "resultOffset": offset,
            "resultRecordCount": count,
        },
    )
    return data.get("features", [])


def _query_page_table(base: str, layer: int, offset: int, count: int) -> list:
    data = _get(
        f"{base}/{layer}/query",
        {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "false",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": count,
        },
    )
    return data.get("features", [])


def _paginate(base: str, layer: int, *, has_geometry: bool, label: str = "") -> list:
    total = _get_record_count(base, layer)
    print(f"  {label or f'Layer {layer}'}: {total:,} records")
    if total == 0:
        raise RuntimeError(f"No records from {base}/{layer}. Check service availability.")
    pages = math.ceil(total / PAGE_SIZE)
    features = []
    fn = _query_page_geojson if has_geometry else _query_page_table
    for i in range(pages):
        offset = i * PAGE_SIZE
        print(f"  Page {i+1}/{pages} (offset {offset:,}) ...", end="", flush=True)
        for attempt in range(4):
            try:
                page = fn(base, layer, offset, PAGE_SIZE)
                break
            except Exception as exc:
                if attempt == 3:
                    raise
                wait = 2 ** (attempt + 1)
                print(f" retry in {wait}s ({exc}) ...", end="", flush=True)
                time.sleep(wait)
        features.extend(page)
        print(f" {len(page)}")
        time.sleep(0.2)
    return features

# ---------------------------------------------------------------------------
# Conversion helpers
# ---------------------------------------------------------------------------

def _features_to_gdf(features: list) -> gpd.GeoDataFrame:
    rows, geoms = [], []
    for f in features:
        rows.append(f.get("properties", {}))
        geoms.append(shape(f["geometry"]) if f.get("geometry") else None)
    return gpd.GeoDataFrame(rows, geometry=geoms, crs="EPSG:4326")


def _features_to_df(features: list) -> pd.DataFrame:
    return pd.DataFrame([f.get("attributes", {}) for f in features])


def _safe_float(val) -> float | None:
    try:
        f = float(val)
        return f if f == f else None  # NaN check
    except (TypeError, ValueError):
        return None

# ---------------------------------------------------------------------------
# Sales extraction from CAMA_Master
# ---------------------------------------------------------------------------

def _extract_sales(cama: pd.DataFrame) -> pd.DataFrame:
    """
    Build a sales table from CAMA Residential sale history fields.

    Each CAMA Residential record can have:
      - Most recent sale: PRICE (NUMBER), SALEDT (DATE as ms-epoch)
      - Historical sales: SALEYR1-3 (TEXT year), SALEMTH1-3 (TEXT month), SALEPR1-3 (NUMBER)

    Returns a DataFrame with columns:
      key_sale, key, sale_date, sale_price, valid_sale, vacant_sale
    """
    records = []

    for _, row in cama.iterrows():
        parid = str(row.get("PARID") or "").strip()
        if not parid:
            continue

        # Most recent sale — SALEDT is stored as milliseconds since epoch
        price0 = _safe_float(row.get("PRICE"))
        saledt = row.get("SALEDT")
        if price0 and price0 > 0 and saledt is not None:
            try:
                if isinstance(saledt, (int, float)):
                    date0 = pd.to_datetime(saledt, unit="ms", utc=True).tz_localize(None)
                else:
                    date0 = pd.to_datetime(saledt)
                if pd.notna(date0) and date0.year >= SALES_MIN_YEAR:
                    records.append({
                        "key_sale":   f"{parid}_{date0.year}_{date0.month:02d}_0",
                        "key":        parid,
                        "sale_date":  date0.normalize(),
                        "sale_price": price0,
                    })
            except Exception:
                pass

        # Historical sales (SALEYR1-3 / SALEMTH1-3 / SALEPR1-3)
        for i in range(1, 4):
            yr  = str(row.get(f"SALEYR{i}")  or "").strip()
            mth = str(row.get(f"SALEMTH{i}") or "").strip().zfill(2)
            pr  = _safe_float(row.get(f"SALEPR{i}"))
            if len(yr) == 4 and mth.isdigit() and pr and pr > 0:
                try:
                    yr_int = int(yr)
                    if yr_int < SALES_MIN_YEAR:
                        continue
                    date_i = pd.Timestamp(f"{yr}-{mth}-01")
                    records.append({
                        "key_sale":   f"{parid}_{yr}_{mth}_{i}",
                        "key":        parid,
                        "sale_date":  date_i,
                        "sale_price": pr,
                    })
                except Exception:
                    pass

    if not records:
        print("  WARNING: No sales records extracted from CAMA_Master.")
        return pd.DataFrame(
            columns=["key_sale", "key", "sale_date", "sale_price", "valid_sale", "vacant_sale"]
        )

    sales = pd.DataFrame(records)
    # Drop exact duplicate key_sale entries
    sales = sales.drop_duplicates(subset=["key_sale"])
    # Drop duplicate (key, sale_date, sale_price) — avoids PRICE/SALEYR1 overlap
    sales = sales.drop_duplicates(subset=["key", "sale_date", "sale_price"])
    # Arm's-length heuristic: price >= $10,000
    sales["valid_sale"]  = sales["sale_price"] >= 10_000
    # vacant_sale: filled in after join with parcel is_vacant
    sales["vacant_sale"] = False
    return sales.reset_index(drop=True)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # -----------------------------------------------------------------------
    # Step 1: Download parcel polygons (Layer 0)
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print("Step 1: Parcel polygons (Layer 0)")
    parcel_features = _paginate(GIS_BASE, PARCEL_LAYER, has_geometry=True, label="Parcels")
    parcels = _features_to_gdf(parcel_features)
    print(f"  Columns: {sorted(parcels.columns.tolist())}")

    # -----------------------------------------------------------------------
    # Step 2: Download CAMA_Master table (Layer 3)
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print("Step 2: CAMA_Master table (Layer 3)")
    cama_features = _paginate(GIS_BASE, CAMA_LAYER, has_geometry=False, label="CAMA_Master")
    cama = _features_to_df(cama_features)
    print(f"  Columns: {sorted(cama.columns.tolist())}")

    # -----------------------------------------------------------------------
    # Step 3: Download CAMA Residential (FeatureServer/15)
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print("Step 3: CAMA Residential (FeatureServer/15)")
    res_features = _paginate(CAMA_RES_BASE, CAMA_RES_LAYER, has_geometry=False, label="CAMA Residential")
    cama_res = _features_to_df(res_features)
    print(f"  Columns: {sorted(cama_res.columns.tolist())}")

    # -----------------------------------------------------------------------
    # Step 4: Extract sales from CAMA Residential (before dedup)
    # -----------------------------------------------------------------------
    # Sale history fields (PRICE/SALEDT/SALEYR1-3/etc.) live in CAMA Residential,
    # not CAMA_Master. Extract from all cards BEFORE deduplicating so that sale
    # history on secondary cards (lower SFLA) is not lost.
    print(f"\n{'='*60}")
    print("Step 4: Extract sales from CAMA Residential (before dedup)")
    sales = _extract_sales(cama_res)
    print(f"  {len(sales):,} sale records (year >= {SALES_MIN_YEAR})")

    # For parcels with multiple cards (TOTCARDS > 1), keep the card with the
    # largest finished area (primary structure).
    if "PARID" in cama_res.columns and "SFLA" in cama_res.columns:
        cama_res["SFLA"] = pd.to_numeric(cama_res["SFLA"], errors="coerce").fillna(0)
        cama_res = (
            cama_res.sort_values("SFLA", ascending=False)
                    .drop_duplicates(subset=["PARID"], keep="first")
        )
        print(f"  After dedup on PARID (keep max SFLA): {len(cama_res):,} rows")
    print(f"  Valid (>= $10k): {sales['valid_sale'].sum():,}")

    # -----------------------------------------------------------------------
    # Step 5: Join CAMA_Master onto parcels
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print("Step 5: Join CAMA_Master -> parcels (PROPID == PARID)")
    if "PROPID" in parcels.columns and "PARID" in cama.columns:
        cama_cols = [c for c in cama.columns if c not in parcels.columns or c == "PARID"]
        parcels = parcels.merge(
            cama[list(dict.fromkeys(["PARID"] + cama_cols))],
            left_on="PROPID", right_on="PARID",
            how="left",
        )
        print(f"  After join: {len(parcels):,} rows, {len(parcels.columns)} cols")
    else:
        missing = []
        if "PROPID" not in parcels.columns: missing.append("PROPID in parcels")
        if "PARID" not in cama.columns:     missing.append("PARID in cama")
        print(f"  WARNING: Join skipped — missing {missing}")

    # -----------------------------------------------------------------------
    # Step 6: Join CAMA Residential onto parcels
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print("Step 6: Join CAMA Residential -> parcels (PROPID == PARID)")
    if "PROPID" in parcels.columns and "PARID" in cama_res.columns:
        res_cols = [c for c in cama_res.columns
                    if c not in parcels.columns or c == "PARID"]
        parcels = parcels.merge(
            cama_res[list(dict.fromkeys(["PARID"] + res_cols))],
            left_on="PROPID", right_on="PARID",
            how="left",
            suffixes=("", "_res"),
        )
        print(f"  After join: {len(parcels):,} rows, {len(parcels.columns)} cols")
    else:
        missing = []
        if "PROPID" not in parcels.columns:  missing.append("PROPID in parcels")
        if "PARID" not in cama_res.columns:  missing.append("PARID in cama_res")
        print(f"  WARNING: Join skipped — missing {missing}")

    # -----------------------------------------------------------------------
    # Step 7: Apply field mapping
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print("Step 7: Apply FIELD_MAP")
    missing = [raw for std, raw in FIELD_MAP.items() if raw not in parcels.columns]
    if missing:
        print(
            f"  WARNING: {len(missing)} source column(s) not found in combined data:\n"
            + "\n".join(f"    {c}" for c in missing)
        )

    present_map = {raw: std for std, raw in FIELD_MAP.items() if raw in parcels.columns}
    keep_raw    = list(present_map.keys()) + (["geometry"] if "geometry" in parcels.columns else [])
    out = parcels[keep_raw].rename(columns=present_map)

    # ACREAGE → sqft
    if "land_area_sqft" in out.columns:
        out["land_area_sqft"] = pd.to_numeric(out["land_area_sqft"], errors="coerce") * 43_560.0

    # Cast numeric columns
    num_cols = [
        "land_area_sqft", "bldg_area_finished_sqft", "bldg_year_built",
        "bldg_condition_num", "bldg_rooms_bed", "bldg_rooms_bath",
        "bldg_rooms_bath_half", "bldg_stories", "bldg_garage_cars",
        "assr_land_value", "assr_impr_value", "assr_market_value",
    ]
    for col in num_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    # Derive bldg_fireplaces = wood-burning openings + prefab fireplaces
    wbfp = pd.to_numeric(parcels.get("WBFP_OPENINGS", pd.Series(0, index=parcels.index)), errors="coerce").fillna(0)
    met_fp = pd.to_numeric(parcels.get("MET_FIREPL", pd.Series(0, index=parcels.index)), errors="coerce").fillna(0)
    out["bldg_fireplaces"] = (wbfp + met_fp).astype("float64")

    # Map PHYCOND text → numeric condition rating
    if "bldg_condition_num" in out.columns:
        out["bldg_condition_num"] = (
            out["bldg_condition_num"]
            .astype(str)
            .str.strip()
            .str.upper()
            .map(PHYCOND_MAP)
        )
        out["bldg_condition_num"] = pd.to_numeric(out["bldg_condition_num"], errors="coerce")

    # Derive is_vacant: no finished sqft AND no building value
    sfla = pd.to_numeric(out.get("bldg_area_finished_sqft", 0), errors="coerce").fillna(0)
    bldg_val = pd.to_numeric(out.get("assr_impr_value", 0), errors="coerce").fillna(0)
    out["is_vacant"] = ((sfla == 0) & (bldg_val == 0)).astype(bool)

    # -----------------------------------------------------------------------
    # Step 8: Fill vacant_sale in sales using parcel is_vacant
    # -----------------------------------------------------------------------
    if len(sales) > 0 and "key" in out.columns:
        vacant_lookup = out.set_index("key")["is_vacant"].to_dict()
        sales["vacant_sale"] = sales["key"].map(vacant_lookup).fillna(False).astype(bool)

    # -----------------------------------------------------------------------
    # Step 9: Write outputs
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print("Step 9: Write outputs")

    print(f"  berks_parcels.parquet — {len(out):,} rows, cols: {list(out.columns)}")
    out.to_parquet(PARCELS_PATH, index=False)
    print(f"  Written: {PARCELS_PATH}")

    print(f"  sales.parquet — {len(sales):,} rows")
    sales.to_parquet(SALES_PATH, index=False)
    print(f"  Written: {SALES_PATH}")

    print("\nDone.")
    print(
        "\nNotes:\n"
        "  - category_code values are CLASS letter codes: R/A/C/I/F/E/UE/UT\n"
        "  - bldg_condition_num mapped from PHYCOND: 1=Unsound … 6=Very Good\n"
        "  - bldg_quality_num not available (no grade field in CAMA Residential)\n"
        "  - sales.parquet uses arm's-length heuristic: valid_sale = price >= $10k\n"
        "    Review and supplement with RTT data if available.\n"
        "  - Verify CLASS code distribution against model_groups in settings.json"
    )


if __name__ == "__main__":
    main()
