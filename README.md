# Berks Open AVMKit

Berks County, PA property valuation models using [OpenAVMKit](https://github.com/larsiusprime/openavmkit).

## Overview

This project builds automated valuation models (AVMs) for Berks County properties using publicly available assessment and sales data. The goal is to support a distributional analysis of the impact of replacing local wage/earned income taxes with a land value tax.

County seat: Reading, PA. FIPS: 42011.

## Setup

Requires Python 3.11+ and [OpenAVMKit](https://github.com/larsiusprime/openavmkit):

```bash
pip install openavmkit geopandas shapely pyarrow requests
```

## Pipeline

Located in `notebooks/pipeline/`. Run scripts in order from that directory:

```bash
cd notebooks/pipeline

python download_berks_parcels.py   # Download parcels + CAMA from Berks County GIS
python process_berks.py            # Process sales into sales.parquet  (TODO)
python run_01_assemble.py          # Assemble parcels + sales; enrich with census/OSM
python run_02_clean.py             # Sales scrutiny and data cleaning
python run_03_model.py             # Train valuation models and generate ratio studies
```

| Script | Status |
|---|---|
| `download_berks_parcels.py` | Partial — downloads geometry + assessed values; building attributes pending CAMA Residential join |
| `process_berks.py` | TODO — sales/RTT data not yet acquired |
| `run_01_assemble.py` | Ready |
| `run_02_clean.py` | Ready |
| `run_03_model.py` | Ready |

## Data

Input data (`notebooks/pipeline/data/us-pa-berks/`) is not tracked in git due to size.

**Parcel data** is downloaded from the Berks County GIS server via `download_berks_parcels.py`.

**Sales data** requires a separate RTT extract from the Berks County Recorder of Deeds or PA Department of Revenue — see `process_berks.py` for details.
