"""
Run the 03-model notebook logic as a plain Python script.
Execute from the notebooks/pipeline/ directory.
"""
import os, sys
import matplotlib
matplotlib.use("Agg")  # non-interactive backend — prevents GUI hang in subprocesses
import numpy as np
import pandas as pd
os.environ["PYTHONIOENCODING"] = "utf-8"
os.environ["PYTHONUNBUFFERED"] = "1"  # force unbuffered stdout so progress appears in real time
sys.stdout.reconfigure(line_buffering=True)

repo_root = os.path.abspath("../..")
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)


# ---------------------------------------------------------------------------
# Preprocessing helpers (Berks County-specific, outside OpenAVMKit)
# ---------------------------------------------------------------------------
CITY_HALL_LAT = 40.3356   # Reading, PA City Hall
CITY_HALL_LON = -75.9269

# Improved-parcel building fields to fill with per-model-group median
_IMPR_FILL_MEDIAN = [
    "bldg_condition_num",
    "bldg_stories",
    "bldg_rooms_bath",
    "bldg_rooms_bed",
]


def fill_universe_nulls(universe):
    """Fill NaN building characteristics in the universe with per-model-group medians
    (computed on improved parcels only). Mirrors settings fill.median_impr prescription."""
    df = universe.copy()
    # Treat is_vacant as a plain numpy bool to avoid pandas nullable-boolean issues
    is_vacant = df["is_vacant"].to_numpy(dtype=bool, na_value=True)
    model_group = df["model_group"].values

    for col in _IMPR_FILL_MEDIAN:
        if col not in df.columns:
            continue
        col_vals = df[col].to_numpy(dtype=float, na_value=np.nan).copy()
        null_mask = np.isnan(col_vals)
        if not null_mask.any():
            continue
        # Compute per-model-group medians on improved parcels
        mg_medians = {}
        for mg in np.unique(model_group[~pd.isnull(model_group)]):
            mg_improved_mask = (model_group == mg) & (~is_vacant)
            vals_mg = col_vals[mg_improved_mask]
            vals_mg = vals_mg[~np.isnan(vals_mg)]
            if len(vals_mg) > 0:
                mg_medians[mg] = float(np.median(vals_mg))
        # Fill nulls per group
        for mg, med in mg_medians.items():
            fill_mask = null_mask & (model_group == mg)
            col_vals[fill_mask] = med
        # Any remaining nulls (no group median): global improved median
        still_null = np.isnan(col_vals)
        if still_null.any():
            global_vals = col_vals[~is_vacant & ~np.isnan(col_vals)]
            if len(global_vals) > 0:
                col_vals[still_null] = float(np.median(global_vals))
        df[col] = col_vals

    # bldg_area_finished_sqft: fill all remaining nulls with 0
    if "bldg_area_finished_sqft" in df.columns:
        df["bldg_area_finished_sqft"] = df["bldg_area_finished_sqft"].fillna(0.0)

    return df

def add_dist_to_cbd(df):
    """Compute haversine distance (miles) from each parcel centroid to City Hall
    and add as 'dist_to_cbd'. Works on any DataFrame with 'latitude'/'longitude'."""
    R = 3958.8
    lat1 = np.radians(df["latitude"].values)
    lon1 = np.radians(df["longitude"].values)
    lat2 = np.radians(CITY_HALL_LAT)
    lon2 = np.radians(CITY_HALL_LON)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    df = df.copy()
    df["dist_to_cbd"] = (2 * R * np.arcsin(np.sqrt(a))).astype("float64")
    return df

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
print(f"  dist_to_cbd: min={sup.universe['dist_to_cbd'].min():.3f} mi, "
      f"max={sup.universe['dist_to_cbd'].max():.3f} mi")

# TODO: Add Berks-specific field join here once data sources are confirmed.
# For Philadelphia, this block joined frontage, garage_spaces, fireplaces,
# basements, and general_construction from the raw OPA parquet. Add equivalent
# Berks fields here if the assessment data includes them, then re-add those
# fields to ind_vars in settings.json and _IMPR_FILL_MEDIAN above.

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
    run_vacant=False,
    run_hedonic=False,
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
        "run_vacant": False,
        "run_hedonic": False,
        "run_ensemble": True,
    },
)

print(f"\n{'='*60}")
print(f"Step 11: ratio study reports")
print(f"{'='*60}")
run_and_write_ratio_study_breakdowns(load_settings())

print("Done!")
