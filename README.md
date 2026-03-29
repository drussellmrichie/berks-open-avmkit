# Berks Open AVMKit

Berks County, PA property valuation models using [OpenAVMKit](https://github.com/larsiusprime/openavmkit).

## Overview

This project builds automated valuation models (AVMs) for Berks County properties using publicly available assessment and sales data. The goal is to support a distributional analysis of the impact of replacing local wage/earned income taxes with a land value tax.

County seat: Reading, PA. FIPS: 42011.

## Setup

Requires Python 3.11+ and [OpenAVMKit](https://github.com/larsiusprime/openavmkit):

```bash
pip install openavmkit
```

## Notebooks

Located in `notebooks/pipeline/`:

- `00-download.ipynb` — Download and cache source data
- `01-assemble.ipynb` — Assemble parcels and sales into a unified dataset
- `02-clean.ipynb` — Sales scrutiny and data cleaning
- `03-model.ipynb` — Train valuation models and generate ratio studies
- `assessment_quality.ipynb` — Assessment quality analysis

## Data

Input data (`notebooks/pipeline/data/us-pa-berks/`) is not tracked in git due to size.

See `download_berks_parcels.py` and `process_berks.py` for data acquisition scripts (stubs — to be completed once data sources are confirmed).
