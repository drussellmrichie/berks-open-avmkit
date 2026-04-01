"""
Berks County-specific enrichment helpers used in run_03_model.py.

Extracted into a separate module so they can be imported and unit-tested
without triggering the openavmkit pipeline side-effects in run_03_model.py.
"""

import numpy as np
import pandas as pd

CITY_HALL_LAT = 40.3356   # Reading, PA City Hall
CITY_HALL_LON = -75.9269

# Improved-parcel building fields to fill with per-model-group median
_IMPR_FILL_MEDIAN = [
    "bldg_condition_num",
    "bldg_stories",
    "bldg_rooms_bath",
    "bldg_rooms_bath_half",
    "bldg_rooms_bed",
    "bldg_garage_cars",
    "bldg_fireplaces",
]


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
