"""
Run the 03-model notebook logic as a plain Python script.
Execute from the notebooks/pipeline/ directory.
"""
import os, sys
import matplotlib
matplotlib.use("Agg")  # non-interactive backend — prevents GUI hang in subprocesses
import numpy as np
import pandas as pd
os.environ["PYTHONUNBUFFERED"] = "1"  # force unbuffered stdout so progress appears in real time
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)  # must be called on the live stream; os.environ["PYTHONIOENCODING"] has no effect after Python starts

repo_root = os.path.abspath("../..")
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# ---------------------------------------------------------------------------
# Preprocessing helpers (Berks County-specific, outside OpenAVMKit)
# ---------------------------------------------------------------------------
from berks_helpers import (
    CITY_HALL_LAT, CITY_HALL_LON, _IMPR_FILL_MEDIAN,
    add_dist_to_cbd, add_land_shape_features, add_census_derived_features, fill_universe_nulls,
)

locality = "us-pa-berks"
verbose = True

from openavmkit.pipeline import (
    init_notebook,
    from_checkpoint,
    delete_checkpoints,
    load_settings,
    load_cleaned_data_for_modeling,
    examine_sup,
    write_canonical_splits,
    try_variables,
    try_models,
    finalize_models,
    run_and_write_ratio_study_breakdowns,
    enrich_sup_spatial_lag,
    identify_outliers,
    write_parquet,
)

print(f"\n{'='*60}")
print(f"Step 1: init_notebook + clear checkpoints")
print(f"{'='*60}")
init_notebook(locality)
delete_checkpoints("3-model")

print(f"\n{'='*60}")
print(f"Step 2: load_settings")
print(f"{'='*60}")
settings = load_settings()

print(f"\n{'='*60}")
print(f"Step 3: load_cleaned_data_for_modeling")
print(f"{'='*60}")
sup = load_cleaned_data_for_modeling(settings)
print(f"  universe: {len(sup.universe):,} rows")
print(f"  sales:    {len(sup.sales):,} rows")

print(f"\n{'='*60}")
print(f"Step 4: examine_sup")
print(f"{'='*60}")
examine_sup(sup, load_settings())

print(f"\n{'='*60}")
print(f"Step 5: write_canonical_splits")
print(f"{'='*60}")
write_canonical_splits(sup, load_settings(), verbose=verbose)

print(f"\n{'='*60}")
print(f"Step 6: enrich_sup_spatial_lag")
print(f"{'='*60}")
sup = from_checkpoint(
    "3-model-00-enrich-spatial-lag",
    enrich_sup_spatial_lag,
    {"sup": sup, "settings": load_settings(), "verbose": verbose},
)
write_parquet(sup.universe, "out/look/3-spatial-lag-universe.parquet")
write_parquet(sup.sales,    "out/look/3-spatial-lag-sales.parquet")

# --- Berks County-specific enrichment (outside OpenAVMKit) ---
# Applied after checkpoint load so it survives the spatial-lag cache restore.
print("  Adding dist_to_cbd from Reading City Hall coordinates ...")
sup.universe = add_dist_to_cbd(sup.universe)

print("  Computing derived census features (median_home_value sentinel cleanup, pct_owner_occupied) ...")
sup.universe = add_census_derived_features(sup.universe)

print("  Computing lot compactness (perimeter / sqrt(area), UTM Zone 18N) ...")
sup.universe = add_land_shape_features(sup.universe)
if "land_compactness" in sup.universe.columns:
    vals = sup.universe["land_compactness"].dropna()
    print(f"    land_compactness: median={vals.median():.2f}, min={vals.min():.2f}, max={vals.max():.2f}")
print(f"  dist_to_cbd: min={sup.universe['dist_to_cbd'].min():.3f} mi, "
      f"max={sup.universe['dist_to_cbd'].max():.3f} mi")

# Berks-specific fields (bldg_garage_cars, bldg_fireplaces, bldg_ext_wall,
# bldg_bsmt_type, bldg_rooms_bath_half) are sourced from CAMA Residential and
# embedded in berks_parcels.parquet by download_berks_parcels.py — no
# additional join is needed here. They flow through settings.json load/fill
# and are included in ind_vars for the main model.

# Add binary category_code indicator columns for the vacant model.
# Raw category_code is a string and gets dropped by variable-selection steps
# that require numeric input. Binary float columns are always numeric and
# give LightGBM a clear signal to separate residential lots from commercial/
# industrial/farm vacant land (very different price ranges).
print("  Adding category_code binary indicators ...")
for col, codes in [
    ("cat_is_residential", ["R"]),
    ("cat_is_commercial",  ["C"]),
    ("cat_is_industrial",  ["I"]),
    ("cat_is_farm",        ["F"]),
]:
    sup.universe[col] = sup.universe["category_code"].isin(codes).astype("float64")
    n = int(sup.universe[col].sum())
    print(f"    {col}: {n:,} parcels = 1")

print("  Filling NaN building characteristics with per-model-group medians ...")
sup.universe = fill_universe_nulls(sup.universe)
for col in _IMPR_FILL_MEDIAN:
    if col in sup.universe.columns:
        n_null = sup.universe[col].isna().sum()
        print(f"    {col}: {n_null} remaining nulls after fill")

print(f"\n{'='*60}")
print(f"Step 7: try_variables")
print(f"{'='*60}")
try_variables(sup, load_settings(), verbose, plot=False)

print(f"\n{'='*60}")
print(f"Step 8: try_models")
print(f"{'='*60}")
try_models(
    sup=sup,
    settings=load_settings(),
    save_params=True,
    verbose=verbose,
    run_main=True,
    run_vacant=True,
    run_hedonic=True,
    run_ensemble=True,
    do_shaps=False,
    do_plots=False,
)

print(f"\n{'='*60}")
print(f"Step 9: identify_outliers")
print(f"{'='*60}")
try:
    identify_outliers(sup=sup, settings=load_settings())
except Exception as e:
    print(f"  identify_outliers skipped (missing optional column): {e}")

print(f"\n{'='*60}")
print(f"Step 10: finalize_models")
print(f"{'='*60}")
results = from_checkpoint(
    "3-model-02-finalize-models",
    finalize_models,
    {
        "sup": sup,
        "settings": load_settings(),
        "save_params": True,
        "use_saved_params": True,
        "verbose": verbose,
        "run_main": True,
        "run_vacant": True,
        "run_hedonic": True,
        "run_ensemble": True,
    },
)

print(f"\n{'='*60}")
print(f"Step 11: ratio study reports")
print(f"{'='*60}")
run_and_write_ratio_study_breakdowns(load_settings())

print("Done!")
