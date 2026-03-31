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

| Model Group | Test Sales | Median Ratio | MAPE (trimmed) |
|---|---|---|---|
| Residential (SF) | 4,401 | 1.00 | 11.9% |
| Commercial/Industrial | 18 | 4.84 | — |
| Vacant Land | 37 | 1.59 | — |

*Median ratio = predicted ÷ time-adjusted sale price. Trimmed = ratios between 0.6–1.4. MAPE omitted where fewer than 15 trimmed sales.*

The residential model is well-calibrated (median ratio ~1.0, MAPE ~12%), comparable to professional mass appraisal standards. Commercial and vacant are undertrained due to low sales volume — only 18 and 37 test sales respectively, with very few falling in the trimmed window.

Model inputs for residential SF: building area, land area, condition, age, bedrooms, bathrooms (full + half), stories, garage cars, fireplaces, exterior wall type, basement type, building style (architectural type), distance to Reading City Hall, lat/lon, polar coordinates, parcel aspect ratio, municipality, and school district. Training uses sales from 2021 onward.

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
pip install openavmkit geopandas shapely pyarrow requests
```

Several bugs in the installed `openavmkit` library require patching before the pipeline will run. See `CLAUDE.md` for the full list of patched files and functions.

## Pipeline

Located in `notebooks/pipeline/`. Run scripts in order from that directory:

```bash
cd notebooks/pipeline

python download_berks_parcels.py   # Download parcels + CAMA from Berks County GIS
python run_01_assemble.py          # Assemble parcels + sales; enrich with census/OSM
python run_02_clean.py             # Sales scrutiny and data cleaning
python run_03_model.py             # Train valuation models and generate ratio studies
```

| Script | Status |
|---|---|
| `download_berks_parcels.py` | Complete — downloads parcel geometry, CAMA_Master (values), CAMA Residential (building attributes + sale history) |
| `process_berks.py` | Data validation — checks schema, key uniqueness, null rates, CLASS distribution, sales volume |
| `run_01_assemble.py` | Ready |
| `run_02_clean.py` | Ready |
| `run_03_model.py` | Ready |

## Data

Input data (`notebooks/pipeline/data/us-pa-berks/`) is not tracked in git due to size. Run `download_berks_parcels.py` to fetch it from Berks County's public ArcGIS REST APIs.

**Sources:**
- Parcel geometry + CLASS/ACREAGE/MUNICIPALNAME: [ParcelSearchTable MapServer, Layer 0](https://gis.co.berks.pa.us/arcgis/rest/services/Assess/ParcelSearchTable/MapServer/0)
- Assessment values (LAND_VALUE, BLDG_VALUE, TOTAL_VALUE): [CAMA_Master, Layer 3](https://gis.co.berks.pa.us/arcgis/rest/services/Assess/ParcelSearchTable/MapServer/3)
- Building attributes + sale history: [CAMA Residential FeatureServer/15](https://services3.arcgis.com/dGYe1jDYrTw1wwpc/arcgis/rest/services/Berks_Assessment_CAMA_Residential_File/FeatureServer/15)

**Universe:** 156,469 parcels — 133,855 residential (CLASS=R), 9,174 commercial/industrial, 6,996 vacant, 256 apartment, 6,188 farm/exempt.

**Sales:** 78,258 extracted from CAMA Residential history fields (2018–present); 60,058 valid (≥ $10k); 18,905 retained after sales scrutiny.
