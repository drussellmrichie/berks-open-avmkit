# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Berks County, PA property automated valuation models (AVMs), built on the [`openavmkit`](https://github.com/larsiusprime/openavmkit) library. The goal is distributional analysis of replacing local earned income taxes with a land value tax. County seat: Reading, PA. FIPS: 42011.

Ported from `philly_open_avmkit` — see that repo for reference. The pipeline architecture, `openavmkit` library patches, checkpoint system, and modeling logic are identical. Only the data acquisition scripts, field mappings, CBD coordinates, and category codes differ.

## Running the Pipeline

**Python interpreter:** Always use `C:/Users/druss/miniconda3/python.exe` (not the system `python`, which redirects to the Microsoft Store stub).

Scripts must be run from `notebooks/pipeline/`:

```bash
cd C:/projects/berks_open_avmkit/notebooks/pipeline

# Stage 1: Assemble raw data into a SalesUniversePair
C:/Users/druss/miniconda3/python.exe run_01_assemble.py

# Stage 2: Clean data, run sales scrutiny, build equity clusters
C:/Users/druss/miniconda3/python.exe run_02_clean.py

# Stage 3: Feature engineering + model training + SHAP output (main loop)
C:/Users/druss/miniconda3/python.exe run_03_model.py
```

`openavmkit` uses a checkpoint system (`out/checkpoints/`). Each `run_03_model.py` call clears the `3-model` checkpoints at startup (except the spatial-lag checkpoint `3-model-00-enrich-spatial-lag`, which is expensive to recompute). If you need a full fresh run, delete all files in `out/checkpoints/` manually.

## Architecture

### Pipeline Stages

| Script | Purpose |
|---|---|
| `download_berks_parcels.py` | Download Berks County parcel polygons + assessment attributes |
| `process_berks.py` | Process parcels + sales into `berks_parcels.parquet` + `sales.parquet` |
| `run_01_assemble.py` | Merge parcels + sales; tag model groups; enrich with census/OSM |
| `run_02_clean.py` | Horizontal equity clustering; sales scrutiny; null-fill |
| `run_03_model.py` | Berks enrichment + LightGBM training + SHAP writing |

### Configuration: `settings.json`

All model configuration lives in `notebooks/pipeline/data/us-pa-berks/in/settings.json`. Key sections:

- **`data.load`** — maps raw Berks column names to standardized field names. **The right-hand side (source column names) are placeholders from Philadelphia** — update these once Berks data is acquired and column names are known.
- **`data.process`** — enrichment sources (census FIPS 42011, OSM), null-fill prescriptions per model group
- **`modeling.metadata`** — valuation date (`2026-01-01`), sales lookback (`use_sales_from: 2022`). If Berks sales volume is thin (~10-15k/year), consider changing to 2023.
- **`modeling.model_groups`** — PA standard property class codes (101=SF, 210/220=MF, 400-405=Commercial, vacant). **Verify these against actual Berks data before running the pipeline.**
- **`modeling.models.main.default.ind_vars`** — 15 features (Philadelphia's OPA-specific fields removed). Add Berks-specific fields here once data is confirmed.

### Berks-Specific Enrichment in `run_03_model.py`

1. **`add_dist_to_cbd()`** — Haversine distance (miles) from each parcel centroid to Reading City Hall (40.3356°N, 75.9269°W); stored as `dist_to_cbd`.
2. **`fill_universe_nulls()`** — median-imputes `bldg_quality_num`, `bldg_condition_num`, `bldg_stories`, `bldg_rooms_bath`, `bldg_rooms_bed` per model group (improved parcels only).
3. **TODO block** — placeholder for joining additional Berks assessment fields (equivalent to Philadelphia's OPA join for frontage, garage, fireplaces, etc.) once data sources are confirmed.

### `openavmkit` Library Patches

The same patches applied to `philly_open_avmkit` are required here. See `philly_open_avmkit/CLAUDE.md` for the full patch table. Patched files in `C:/Users/druss/miniconda3/Lib/site-packages/openavmkit/`:

| File | Functions patched |
|---|---|
| `utilities/stats.py` | `calc_elastic_net_regularization`, `calc_p_values_recursive_drop`, `calc_t_values_recursive_drop`, `calc_vif_recursive_drop` |
| `shap_analysis.py` | `make_shap_table` |
| `modeling.py` | `_contrib_to_unit_values`, `_add_prediction_to_contribution` |
| `pipeline.py` | `finalize_models` |

### Data Acquisition (TODO)

**This is the critical blocker before the pipeline can run.**

1. **Parcel data** — see `download_berks_parcels.py` for source options (PASDA, opendata.berkspa.gov, or direct GIS request to gis@berkspa.gov)
2. **Sales/RTT data** — contact Berks County Recorder of Deeds or PA Dept of Revenue for bulk RTT extract. No public bulk API has been confirmed yet.
3. **Once data is in hand:** update `data.load` field mappings in `settings.json` to match actual Berks column names.

### Key Pathing Note

`openavmkit.pipeline.init_notebook(locality)` changes `os.getcwd()` to `data/us-pa-berks`. All relative paths in pipeline code break after this call. Use `pathlib.Path(__file__).parent` to anchor paths to the script's directory.

## Data Files (gitignored)

```
notebooks/pipeline/data/us-pa-berks/
├── in/
│   ├── settings.json          # tracked
│   ├── berks_parcels.parquet  # gitignored (to be created by download/process scripts)
│   └── sales.parquet          # gitignored
├── out/
│   ├── checkpoints/           # pipeline state (pickle files)
│   ├── models/                # SHAP CSVs, ratio study outputs
│   └── look/                  # diagnostic parquets
└── cache/                     # geometry cache
```
