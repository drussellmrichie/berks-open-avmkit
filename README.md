# Berks Open AVMKit

Berks County, PA property valuation models using [OpenAVMKit](https://github.com/larsiusprime/openavmkit).

> **Disclaimer:** This is independent research in progress. All models, findings, and estimates are provided for research and educational purposes only. No warranty is made regarding accuracy, completeness, or fitness for any particular purpose. This work does not constitute professional appraisal, legal, tax, or financial advice. Results should not be relied upon for any official, legal, or financial decision. Use at your own risk.

## Overview

This project builds automated valuation models (AVMs) for Berks County properties using publicly available assessment and sales data. The goals are to analyze:

1. The impact of Berks County's decades-old assessments on assessment fairness.
2. A revenue-neutral shift from property tax to land value tax, conditional on updated reassessments based on recent sales.

County seat: Reading, PA. FIPS: 42011.

## Key Findings (Valuation Date 2026-01-01)

### Model Performance

Three model groups, evaluated on the 1-year study period (2025-01-01 – 2026-01-01):

| Model | Study-period sales (trimmed) | Median Ratio | COD (trimmed) | Notes |
|---|---|---|---|---|
| Residential SF (improved) | 4,536 | 1.02 | 12.6 | Excellent — meets IAAO standards |
| Residential SF (vacant lots) | 215 | 1.39 | 77.2 | R-category vacant; calibration challenging |
| Commercial/Industrial/Apartment (vacant) | 29 | 4.31 | 146.3 | Data-limited; sparse sales |
| Vacant (standalone cross-cutting model) | 51 | 1.79 | 91.3 | All vacant parcels across CLASS codes |

*Median ratio = predicted ÷ time-adjusted sale price. COD = Coefficient of Dispersion (lower is better; IAAO standard ≤ 15 for residential improved).*

The residential SF improved model meets IAAO standards (median ratio 1.02, COD 12.6). Vacant and commercial models remain challenging due to sparse arm's-length sales and high price heterogeneity — the commercial group had only 1 improved test sale in the study period.

A hedonic model is also trained per model group to decompose value into land and improvement components. The `hedonic_full` sub-model (predicting total market value) matches main model accuracy; the `hedonic_land` sub-model (predicting land-only value) is currently unreliable for improved parcels — it over-predicts land values by inflating the land fraction due to location–quality correlation. See `CLAUDE.md` for details.

Model inputs for residential SF (31 features): building area, land area, condition, age, bedrooms, bathrooms (full + half), stories, garage cars, fireplaces, exterior wall type, basement type, architectural style, distance to Reading City Hall, lat/lon, polar coordinates, parcel aspect ratio, municipality, school district, distances to parks/water/schools/highway on-ramps/groceries/shopping/medical (OSM), median household income, median home value, and commercial fields (land use code, commercial building area, structure type, parking spaces, living units). Training uses sales from 2019 onward.

### Assessment Ratios (Current vs. Market)

A striking finding from comparing current county assessments to modeled market values:

| Property Type | Median Assessment Ratio |
|---|---|
| Residential (SF) | **30.7%** of market value |
| Commercial/Industrial | **34.1%** of market value |
| Vacant Land | **3.2%** of market value |

Berks County's last general reassessment was in 1994. Properties are assessed at roughly a third of market value — and vacant/undeveloped land at barely 4 cents on the dollar — reflecting decades of appreciation that the assessment rolls have never captured. This severe underassessment of land relative to improvements is a key input to the land value tax distributional analysis.

## Setup

Requires Python 3.11+ and [OpenAVMKit](https://github.com/larsiusprime/openavmkit):

```bash
pip install openavmkit python-dotenv geopandas shapely pyarrow requests
```

Several bugs in the installed `openavmkit` library require patching before the pipeline will run. See `CLAUDE.md` for the full list of patched files and functions.

## Pipeline

Located in `notebooks/pipeline/`. Run scripts in order from that directory:

```bash
cd notebooks/pipeline

python download_berks_parcels.py   # Download parcels + CAMA from Berks County GIS
python process_berks.py            # Validate downloaded files (schema, nulls, coverage)
python run_01_assemble.py          # Assemble parcels + sales; enrich with census/OSM
python run_02_clean.py             # Sales scrutiny and data cleaning
python run_03_model.py             # Train valuation models and generate ratio studies
```

| Script | Status |
|---|---|
| `download_berks_parcels.py` | Complete — downloads parcel geometry, CAMA_Master (assessment values), CAMA Residential (building attributes + sale history), and CAMA Commercial (commercial building attributes + sale history) |
| `process_berks.py` | Complete — validates `berks_parcels.parquet` + `sales.parquet` schema, key uniqueness, null rates, and coverage stats; run after download to confirm files are pipeline-ready |
| `run_01_assemble.py` | Ready |
| `run_02_clean.py` | Ready |
| `run_03_model.py` | Ready |

## Data

Input data (`notebooks/pipeline/data/us-pa-berks/`) is not tracked in git due to size. Run `download_berks_parcels.py` to fetch it from Berks County's public ArcGIS REST APIs.

**Sources:**
- Parcel geometry + CLASS/ACREAGE/MUNICIPALNAME: [ParcelSearchTable MapServer, Layer 0](https://gis.co.berks.pa.us/arcgis/rest/services/Assess/ParcelSearchTable/MapServer/0)
- Assessment values (LAND_VALUE, BLDG_VALUE, TOTAL_VALUE): [CAMA_Master, Layer 3](https://gis.co.berks.pa.us/arcgis/rest/services/Assess/ParcelSearchTable/MapServer/3)
- Residential building attributes + sale history: [CAMA Residential FeatureServer/15](https://services3.arcgis.com/dGYe1jDYrTw1wwpc/arcgis/rest/services/Berks_Assessment_CAMA_Residential_File/FeatureServer/15)
- Commercial building attributes + sale history: [CAMA Commercial FeatureServer/13](https://services3.arcgis.com/dGYe1jDYrTw1wwpc/arcgis/rest/services/Berks_Assessment_CAMA_Commercial_File/FeatureServer/13)
- Census enrichment: ACS 5-year estimates at census block group level (median income, median home value, median rent, owner-occupancy rate) via Census Bureau API
- OSM enrichment: distances to parks, water bodies, schools, highway on-ramps, groceries, shopping centers, and medical facilities via OpenStreetMap/Overpass

**Universe:** 156,778 parcels — 133,855 residential (CLASS=R), 9,174 commercial/industrial (CLASS=C/I), 256 apartment (CLASS=A), 6,188 farm/exempt (CLASS=F/E), remainder other/unknown.

**Sales:** extracted from CAMA Residential + Commercial + Master history fields (2019–present); deduped and filtered for arm's-length transactions (≥ $10k, not bulk/portfolio sales).
