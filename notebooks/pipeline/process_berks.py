"""
Process raw Berks County assessment + sales data into the two input parquets
expected by the pipeline:

  data/us-pa-berks/in/berks_parcels.parquet   (universe — all parcels)
  data/us-pa-berks/in/sales.parquet           (arm's-length sales)

TODO: Complete this script once data sources and column schemas are confirmed.

----------------------------------------------------------------------------
PARCELS
----------------------------------------------------------------------------
Input:  raw assessment extract from Berks County Assessment Office or PASDA
Output: berks_parcels.parquet

Key transformations (adapt from Philadelphia's process_opa.py):
  - Map raw Berks column names to standardized openavmkit field names
    (update settings.json data.load section to match)
  - Quality/condition: Berks likely uses numeric ratings directly (no
    letter-grade mapping needed, unlike OPA's A+/A/B/C... grades)
  - category_code: verify PA standard codes in the actual data
    (101=SF, 102=Mobile, 210=MF, 400s=Commercial, 600s=Ag, 700s=Vacant)
  - is_vacant: derive from category_code or zero livable area
  - neighborhood proxy: use municipality code, zip, or census tract
    (44 municipalities in Berks County; municipality is the strongest
    geographic categorical predictor to start with)
  - geometry: polygon in EPSG:4326 required by openavmkit

----------------------------------------------------------------------------
SALES
----------------------------------------------------------------------------
Input:  RTT (Realty Transfer Tax) records from Berks County Recorder of Deeds
        or PA Department of Revenue bulk extract
Output: sales.parquet

Key transformations (adapt from Philadelphia's process_sales.py):
  - Rename fields: key_sale, key, sale_date, sale_price, valid_sale, vacant_sale
  - valid_sale: arm's-length deeds only, sale_price >= $10,000
  - vacant_sale: join is_vacant from parcels
  - sale_date: parse to YYYY-MM-DD string
  - Drop rows with null key_sale, key, sale_date, or sale_price

Notes:
  - Berks has ~430k population vs. Philly ~1.5M — expect ~10-15k sales/year.
    Consider using 2023+ rather than 2022+ if sales volume is thin.
  - If RTT data is not publicly available in bulk, contact:
      Berks County Recorder of Deeds: https://www.berkspa.gov/departments/recorder-of-deeds
      PA Dept of Revenue RETR data (Realty Transfer Certificate of Return)
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
# # TODO: implement parcel processing
# # TODO: implement sales processing

raise NotImplementedError(
    "process_berks.py is not yet implemented. "
    "See the TODO comments above for field mapping guidance."
)
