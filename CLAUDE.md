# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Berks County, PA property automated valuation models (AVMs), built on the [`openavmkit`](https://github.com/larsiusprime/openavmkit) library. The goal is distributional analysis of replacing local earned income taxes with a land value tax. County seat: Reading, PA. FIPS: 42011.

Ported from `philly_open_avmkit` — see that repo for reference. The pipeline architecture, `openavmkit` library patches, checkpoint system, and modeling logic are identical. Only the data acquisition scripts, field mappings, CBD coordinates, and category codes differ.

## Running the Pipeline

Scripts must be run from `notebooks/pipeline/`:

```bash
cd notebooks/pipeline

# Stage 1: Assemble raw data into a SalesUniversePair
python run_01_assemble.py

# Stage 2: Clean data, run sales scrutiny, build equity clusters
python run_02_clean.py

# Stage 3: Feature engineering + model training + SHAP output (main loop)
python run_03_model.py
```

**Note (original dev environment):** The project was developed on Windows with `C:/Users/druss/miniconda3/python.exe`. On that system, replace `python` with that full path to avoid the Microsoft Store Python stub.

`openavmkit` uses a checkpoint system (`out/checkpoints/`). Each `run_03_model.py` call clears the `3-model` checkpoints at startup (except the spatial-lag checkpoint `3-model-00-enrich-spatial-lag`, which is expensive to recompute). If you need a full fresh run, delete all files in `out/checkpoints/` manually.

## Architecture

### Repository Layout

```
berks-open-avmkit/
├── CLAUDE.md
├── README.md
├── .gitignore
└── notebooks/pipeline/
    ├── init_notebooks.py             # Environment setup utility (used internally)
    ├── download_berks_parcels.py     # Data acquisition — downloads parcels, CAMA, and sales via ArcGIS REST
    ├── process_berks.py              # Data processing stub — NOT YET IMPLEMENTED
    ├── run_01_assemble.py            # Pipeline stage 1: assemble
    ├── run_02_clean.py               # Pipeline stage 2: clean
    ├── run_03_model.py               # Pipeline stage 3: model
    └── data/us-pa-berks/
        └── in/
            └── settings.json        # Tracked — all model configuration
```

### Pipeline Stages

| Script | Status | Purpose |
|---|---|---|
| `download_berks_parcels.py` | **Partial** | Downloads parcel geometry + assessed values from Berks County GIS; building attributes pending CAMA Residential join |
| `process_berks.py` | **TODO** | Process sales/RTT data into `sales.parquet` (parcel download now handled by `download_berks_parcels.py`) |
| `run_01_assemble.py` | Ready | Merge parcels + sales; tag model groups; enrich with census/OSM |
| `run_02_clean.py` | Ready | Horizontal equity clustering; sales scrutiny; null-fill |
| `run_03_model.py` | Ready | Berks enrichment + LightGBM training + SHAP writing |

#### Stage 1 steps (`run_01_assemble.py`)
1. `init_notebook(locality)` — sets `os.getcwd()` to `data/us-pa-berks` (see pathing note below)
2. `load_settings()` — reads `settings.json`
3. `load_dataframes(settings)` — loads `berks_parcels.parquet` + `sales.parquet`
4. `process_dataframes(...)` — merges, enriches with Census (FIPS 42011) + OSM
5. `tag_model_groups_sup(...)` — assigns `model_group` per parcel based on `category_code`/`is_vacant`
6. `write_notebook_output_sup(sup, "1-assemble")` — writes pickle + parquet to `out/`

#### Stage 2 steps (`run_02_clean.py`)
1. `init_notebook` + `delete_checkpoints("2-clean")`
2. `read_pickle("out/1-assemble-sup")` — loads stage 1 output
3. `examine_sup(...)` — diagnostic summary
4. `fill_unknown_values_sup(...)` — null-fill per `settings.json data.process.fill`
5. `mark_horizontal_equity_clusters_per_model_group_sup(...)` — cached as `2-clean-00-horizontal-equity`
6. `process_sales(...)` — cached as `2-clean-01-process_sales`
7. `run_sales_scrutiny(...)` — drops cluster + heuristic outliers; cached as `2-clean-02-sales-scrutiny`
8. `write_notebook_output_sup(sup, "2-clean")`

#### Stage 3 steps (`run_03_model.py`)
1. `init_notebook` + `delete_checkpoints("3-model")` (spatial-lag checkpoint preserved)
2. `load_settings()` + `load_cleaned_data_for_modeling(settings)`
3. `examine_sup(...)` + `write_canonical_splits(...)`
4. `enrich_sup_spatial_lag(...)` — cached as `3-model-00-enrich-spatial-lag` (expensive)
5. **Berks-specific enrichment** (outside openavmkit):
   - `add_dist_to_cbd()` — haversine distance to Reading City Hall
   - `fill_universe_nulls()` — median-impute building fields per model group
   - **TODO block** — join additional Berks assessment fields (garage, fireplaces, etc.)
6. `try_variables(...)` — variable selection
7. `try_models(...)` — LightGBM training (main + ensemble only; vacant/hedonic disabled)
8. `identify_outliers(...)` — wrapped in try/except; skipped if optional column missing
9. `finalize_models(...)` — cached as `3-model-02-finalize-models`
10. `run_and_write_ratio_study_breakdowns(...)` — ratio study reports

### Configuration: `settings.json`

Location: `notebooks/pipeline/data/us-pa-berks/in/settings.json`

#### `data.load`
`download_berks_parcels.py` outputs `berks_parcels.parquet` with standardized column names already applied, so `settings.json` uses identity mappings. The source → standardized mapping lives entirely in `FIELD_MAP` in the download script.

Confirmed source columns (from Berks County GIS server, verified against live data):

| Standardized field | Source column | Origin | Status |
|---|---|---|---|
| `key` | `PROPID` | Parcel layer (Layer 0) | Confirmed |
| `land_area_sqft` | `ACREAGE` × 43,560 | Parcel layer | Confirmed |
| `neighborhood` | `MUNICIPALNAME` | Parcel layer | Confirmed |
| `category_code` | `CLASS` | Parcel layer | Confirmed — verify actual code values |
| `assr_land_value` | `LAND_VALUE` | CAMA_Master (Layer 3) | Confirmed |
| `assr_impr_value` | `BLDG_VALUE` | CAMA_Master | Confirmed |
| `assr_market_value` | `TOT_VALUE` | CAMA_Master | Confirmed |
| `bldg_area_finished_sqft` | `SFLA` | CAMA Residential (pending) | Placeholder |
| `bldg_year_built` | `YRBLT` | CAMA Residential (pending) | Placeholder |
| `bldg_condition_num` | `CONDITION` | CAMA Residential (pending) | Placeholder |
| `bldg_quality_num` | `GRADE` | CAMA Residential (pending) | Placeholder |
| `bldg_rooms_bed` | `RMBED` | CAMA Residential (pending) | Placeholder |
| `bldg_rooms_bath` | `FIXBATH` | CAMA Residential (pending) | Placeholder |
| `bldg_stories` | `STORY` | CAMA Residential (pending) | Placeholder |
| `bldg_type` | `STYLE` | CAMA Residential (pending) | Placeholder |
| `census_tract` | `census_tract` | Added by openavmkit Census enrichment | Auto |
| `is_vacant` | Derived | From `CLASS` prefix "7" or zero sqft | Derived |

**Zoning is not available in the Berks GIS data and has been removed from the model.**

CAMA Residential FeatureServer: `https://services3.arcgis.com/dGYe1jDYrTw1wwpc/arcgis/rest/services/Berks_Assessment_CAMA_Residential_File/FeatureServer/15`
CAMA data dictionary PDF: in `data/us-pa-berks/in/` (downloaded from opendata.berkspa.gov)

#### `data.process`
- **Census enrichment:** FIPS `42011`
- **OSM enrichment:** enabled
- **Null-fill:** `median_impr` for 5 building fields; `zero` for `bldg_area_finished_sqft`
- **Dupe handling:** drop on `key` (parcels), drop on `key_sale` (sales)

#### `modeling.metadata`
- `valuation_date`: `2026-01-01`
- `use_sales_from`: `2022` — consider changing to `2023` if Berks sales volume is thin (~10–15k/year)
- `modeler`: `MUSA` / `musa`

#### `modeling.model_groups`
Four groups, filtered by `category_code` or `is_vacant`:

| Group | Filter |
|---|---|
| `residential_sf` | `category_code == "101"` |
| `residential_mf` | `category_code in ["210", "220"]` |
| `commercial` | `category_code in ["400"–"405"]` |
| `vacant` | `is_vacant == true` |

**Verify these codes against actual Berks data before running.**

#### `modeling.models` — independent variables
- **main** (15 features): `bldg_area_finished_sqft`, `land_area_sqft`, `bldg_quality_num`, `bldg_condition_num`, `bldg_age_years`, `bldg_rooms_bed`, `bldg_rooms_bath`, `bldg_stories`, `dist_to_cbd`, `latitude_norm`, `longitude_norm`, `polar_radius`, `polar_angle`, `geom_aspect_ratio`, `neighborhood`
- **vacant** (9 features): `land_area_sqft`, `land_area_sqft_log`, `latitude_norm`, `longitude_norm`, `polar_angle`, `polar_radius`, `geom_rectangularity_num`, `dist_to_cbd`, `neighborhood`
- **hedonic** (10 features): disabled (`"run": []` in instructions)

#### `modeling.instructions`
- Main + vacant: `["lightgbm"]`; hedonic: `[]` (disabled)
- Time adjustment: quarterly (`"period": "Q"`)
- Ensemble: `[]` (auto)

#### `analysis.ratio_study`
1-year lookback, breakdowns by: sale price, building area, building age (10-yr slices), land area, quality (5 quantiles), condition (5 quantiles), neighborhood, market area (census tract).

#### `field_classification`
- `loc_neighborhood` → `neighborhood`; `loc_market_area` → `census_tract`
- Land numeric: `land_area_sqft`; land categorical: `zoning`, `neighborhood`, `census_tract`
- Improvement numeric: area, age, condition, quality, rooms, stories; categorical: `bldg_type`

### Berks-Specific Enrichment in `run_03_model.py`

1. **`add_dist_to_cbd(df)`** — Haversine distance (miles) from each parcel centroid to Reading City Hall (`40.3356°N, 75.9269°W`); stored as `dist_to_cbd`. Applied after spatial-lag checkpoint restore so it always runs on fresh data.
2. **`fill_universe_nulls(universe)`** — Median-imputes the 5 `_IMPR_FILL_MEDIAN` fields (`bldg_quality_num`, `bldg_condition_num`, `bldg_stories`, `bldg_rooms_bath`, `bldg_rooms_bed`) per model group on improved parcels. Falls back to global improved median if a group has no data. Also zero-fills `bldg_area_finished_sqft`.
3. **TODO block** — Placeholder for joining additional Berks assessment fields (equivalent to Philadelphia's OPA join for frontage, garage spaces, fireplaces, basements, construction type). Once added, include those fields in `ind_vars` in `settings.json` and append them to `_IMPR_FILL_MEDIAN`.

### `openavmkit` Library Patches

The same patches applied to `philly_open_avmkit` are required here. See `philly_open_avmkit/CLAUDE.md` for full details. Patched files in the openavmkit install (`site-packages/openavmkit/`):

| File | Functions/locations patched | Problem fixed |
|---|---|---|
| `utilities/stats.py` | `calc_elastic_net_regularization`, `calc_p_values_recursive_drop`, `calc_t_values_recursive_drop`, `calc_vif_recursive_drop` | Median-impute NaN before sklearn/statsmodels fits |
| `shap_analysis.py` | `make_shap_table` | `if list_keys_sale` → `_has_sale_keys` to avoid numpy array truth-value error |
| `modeling.py` | `_contrib_to_unit_values`, `_add_prediction_to_contribution` | Positional concat instead of key-based merge to prevent OOM cartesian product |
| `pipeline.py` | `finalize_models` | Added `run_main/vacant/hedonic/ensemble` params (were hardcoded `True`) |
| `data.py` | `_handle_duplicated_rows` | Default `sort_by` had key `"asc"` instead of `"ascendings"` — KeyError when parcels have duplicate keys |
| `data.py` | `_basic_geo_enrichment` | `land_area_sqft` cast to int64 then float GIS values assigned into it — newer pandas raises; fix: round+astype(int) the RHS |
| `utilities/cache.py` | `write_cached_df` | `ArrowExtensionArray` has no `.sum()` — wrap in `pd.Series()` |

### Data Acquisition Status

**Parcel data** — `download_berks_parcels.py` is fully implemented:
- Downloads 156,778 parcel polygons from Layer 0 (geometry + CLASS/ACREAGE/MUNICIPALNAME)
- Joins CAMA_Master (Layer 3) for LAND_VALUE, BLDG_VALUE, TOTAL_VALUE
- Joins CAMA Residential (FeatureServer/15) for SFLA, YRBLT, PHYCOND, BEDROOMS, FULLBATHS, STORIES, STYLE
- Extracts sales from CAMA Residential history fields (PRICE/SALEDT + SALEYR1-3/SALEMTH1-3/SALEPR1-3) — **before** SFLA-dedup so all cards contribute sales
- Outputs: `berks_parcels.parquet` (156,469 rows after dedup) and `sales.parquet` (78,258 records, 60,058 valid ≥$10k)

**CLASS codes confirmed** from live data: R=Residential (133,855), Commercial/Industrial (9,174), Vacant (6,996 by `is_vacant` flag), Apartment/CLASS=A (256). 6,188 UNKNOWN parcels (likely F=Farm, E=Exempt).

**Sales/RTT data** — sales are extracted from CAMA Residential history fields. `process_berks.py` remains unimplemented (was intended for external RTT data, not currently needed).

### Required Input Data Schemas

**`berks_parcels.parquet`** — all parcels (universe):

| Field | Type | Notes |
|---|---|---|
| `key` | str | Unique parcel identifier |
| `land_area_sqft` | float | |
| `bldg_area_finished_sqft` | float | |
| `bldg_year_built` | float | |
| `bldg_condition_num` | float | Numeric rating |
| `bldg_quality_num` | float | Numeric rating |
| `bldg_rooms_bed` | float | |
| `bldg_rooms_bath` | float | |
| `bldg_stories` | float | |
| `bldg_type` | str | |
| `category_code` | str | PA standard codes |
| `zoning` | str | |
| `neighborhood` | str | Municipality, zip, or census tract proxy |
| `census_tract` | str | |
| `assr_land_value` | float | |
| `assr_impr_value` | float | |
| `assr_market_value` | float | |
| `is_vacant` | bool | |
| `geometry` | polygon | EPSG:4326 |

**`sales.parquet`** — arm's-length sales:

| Field | Type | Notes |
|---|---|---|
| `key_sale` | str | Unique sale identifier |
| `key` | str | Parcel reference |
| `sale_date` | datetime | YYYY-MM-DD format |
| `sale_price` | float | Must be ≥ $10,000 for `valid_sale` |
| `valid_sale` | bool | Arm's-length deed flag |
| `vacant_sale` | bool | Joined from parcel `is_vacant` |

### Key Pathing Note

`openavmkit.pipeline.init_notebook(locality)` changes `os.getcwd()` to `data/us-pa-berks`. All relative paths in pipeline code break after this call. Use `pathlib.Path(__file__).parent` to anchor paths to the script's directory, not relative paths.

### Environment Notes

- Python 3.11+ required
- Install openavmkit: `pip install openavmkit`
- No `requirements.txt` or `pyproject.toml` in the repo; only external dependency is `openavmkit`
- `matplotlib` must use `"Agg"` backend (set in `run_03_model.py`) to prevent GUI hangs in subprocess execution
- `PYTHONIOENCODING=utf-8` and `PYTHONUNBUFFERED=1` set in all pipeline scripts

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
