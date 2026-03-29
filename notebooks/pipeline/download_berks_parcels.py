"""
Download Berks County parcel polygons and write berks_parcels.parquet.

TODO: Complete this script once data sources are confirmed. Options:

1. PASDA (PA Spatial Data Access) — Berks County Parcels 2026:
   https://www.pasda.psu.edu/
   REST endpoint (may vary by year):
   https://apps.pasda.psu.edu/arcgis/rest/services/PA_Parcels/MapServer

2. Berks County Open Data Hub:
   https://opendata.berkspa.gov
   Check for a parcel layer with polygon geometry + assessment attributes.

3. Direct GIS request:
   Contact Berks County GIS office (gis@berkspa.gov) for a full parcel extract
   in GeoJSON, Shapefile, or Parquet format.

Expected output:
   data/us-pa-berks/in/berks_parcels.parquet
   Required columns (standardized names — see settings.json data.load section):
     key, land_area_sqft, bldg_area_finished_sqft, bldg_year_built,
     bldg_condition_num, bldg_quality_num, bldg_rooms_bed, bldg_rooms_bath,
     bldg_stories, bldg_type, category_code, zoning, neighborhood,
     census_tract, assr_land_value, assr_impr_value, assr_market_value,
     is_vacant, geometry (polygon, EPSG:4326)

Notes:
  - Berks County uses PA standardized property class codes
    (e.g. 101=Single Family, 210=Multi-Family, 400s=Commercial, 600s=Ag, 700s=Vacant)
    Verify the actual codes in the first rows of the data before finalizing
    model_groups filters in settings.json.
  - Quality/condition may already be numeric (unlike OPA's A/B/C letter grades).
  - For the neighborhood proxy, use municipality code, zip code, or census tract
    depending on what's available.
"""

import os
os.environ["PYTHONIOENCODING"] = "utf-8"

# import requests
# import pandas as pd
# import geopandas as gpd
# from pathlib import Path
#
# OUT_DIR = Path(__file__).parent / "data" / "us-pa-berks" / "in"
# OUT_DIR.mkdir(parents=True, exist_ok=True)
#
# # TODO: implement download + field mapping
# # parcels.to_parquet(OUT_DIR / "berks_parcels.parquet", index=False)

raise NotImplementedError(
    "download_berks_parcels.py is not yet implemented. "
    "See the TODO comments above for data source options."
)
