"""
Check that Stage 1 produced census_tract, then patch settings.json to add it
to modeling.models.main and modeling.models.hedonic ind_vars.

Run from notebooks/pipeline/ after run_01_assemble.py completes:
    python check_census_and_patch.py
"""
import json
import pathlib
import sys

import pandas as pd

SCRIPT_DIR = pathlib.Path(__file__).parent
DATA_DIR   = SCRIPT_DIR / "data" / "us-pa-berks"
PARQUET    = DATA_DIR / "out" / "look" / "1-assemble-universe.parquet"
SETTINGS   = DATA_DIR / "in" / "settings.json"

MIN_FILL_RATE = 0.80   # require census_tract on at least 80% of parcels

# ---------------------------------------------------------------------------
# 1. Load Stage 1 output
# ---------------------------------------------------------------------------
if not PARQUET.exists():
    sys.exit(f"ERROR: {PARQUET} not found — run run_01_assemble.py first.")

print(f"Reading {PARQUET} ...")
df = pd.read_parquet(PARQUET)
print(f"  {len(df):,} rows, {len(df.columns)} columns")

# ---------------------------------------------------------------------------
# 2. Check census_tract
# ---------------------------------------------------------------------------
if "census_tract" not in df.columns:
    sys.exit(
        "FAIL: census_tract column is absent from Stage 1 output.\n"
        "      Check that CENSUS_API_KEY is set in notebooks/.env and re-run Stage 1."
    )

n_total    = len(df)
n_non_null = df["census_tract"].notna().sum()
fill_rate  = n_non_null / n_total
sample     = df["census_tract"].dropna().unique()[:5].tolist()

print(f"\ncensus_tract fill rate: {n_non_null:,} / {n_total:,} = {fill_rate:.1%}")
print(f"  Sample values: {sample}")

if fill_rate < MIN_FILL_RATE:
    sys.exit(
        f"FAIL: census_tract fill rate {fill_rate:.1%} is below threshold {MIN_FILL_RATE:.0%}.\n"
        "      Census enrichment may have partially failed. Check Stage 1 logs."
    )

print(f"PASS: census_tract fill rate {fill_rate:.1%} >= {MIN_FILL_RATE:.0%}")

# ---------------------------------------------------------------------------
# 3. Patch settings.json
# ---------------------------------------------------------------------------
with open(SETTINGS, "r", encoding="utf-8") as f:
    settings = json.load(f)

MODELS_TO_PATCH = ["main", "hedonic"]
patched = []

for model_name in MODELS_TO_PATCH:
    ind_vars = settings["modeling"]["models"][model_name]["default"]["ind_vars"]
    if "census_tract" not in ind_vars:
        # Insert before "neighborhood" so location vars stay grouped together
        try:
            idx = ind_vars.index("neighborhood")
        except ValueError:
            idx = len(ind_vars)
        ind_vars.insert(idx, "census_tract")
        patched.append(model_name)

if not patched:
    print("\nSettings already up to date — census_tract already in all ind_vars. Nothing to do.")
    sys.exit(0)

with open(SETTINGS, "w", encoding="utf-8") as f:
    json.dump(settings, f, indent=2)
    f.write("\n")

print(f"\nPatched settings.json — added census_tract to ind_vars for: {', '.join(patched)}")
print("Next steps:")
print("  1. Delete out/checkpoints/ (or at least 2-clean and 3-model checkpoints)")
print("  2. Re-run run_02_clean.py and run_03_model.py")
