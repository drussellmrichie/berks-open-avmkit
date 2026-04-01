"""
Run the 01-assemble notebook logic as a plain Python script.
Execute from the notebooks/pipeline/ directory.
"""
import os, sys
os.environ["PYTHONIOENCODING"] = "utf-8"

# Load .env from notebooks/ (contains CENSUS_API_KEY, etc.)
import pathlib
_env_file = pathlib.Path(__file__).parent.parent / ".env"
if _env_file.exists():
    from dotenv import load_dotenv
    load_dotenv(_env_file)

# Add repo root to path (same as init_notebooks.setup_environment)
repo_root = os.path.abspath("../..")
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

locality = "us-pa-berks"
verbose = True

from openavmkit.pipeline import (
    init_notebook,
    load_settings,
    load_dataframes,
    process_dataframes,
    tag_model_groups_sup,
    write_notebook_output_sup,
)

print(f"\n{'='*60}")
print(f"Step 1: init_notebook")
print(f"{'='*60}")
init_notebook(locality)

print(f"\n{'='*60}")
print(f"Step 2: load_settings")
print(f"{'='*60}")
settings = load_settings()
print("Settings loaded OK")
print(f"  locality: {settings.get('locality', {}).get('name')}")
print(f"  valuation_date: {settings.get('modeling', {}).get('metadata', {}).get('valuation_date')}")

print(f"\n{'='*60}")
print(f"Step 3: load_dataframes")
print(f"{'='*60}")
dataframes = load_dataframes(settings=settings, verbose=verbose)
for k, df in dataframes.items():
    print(f"  {k}: {len(df):,} rows, {len(df.columns)} cols")

print(f"\n{'='*60}")
print(f"Step 4: process_dataframes (merge + enrich)")
print(f"{'='*60}")
sup = process_dataframes(dataframes=dataframes, settings=settings, verbose=verbose)
print(f"\nSalesUniversePair created:")
print(f"  universe: {len(sup.universe):,} rows")
print(f"  sales:    {len(sup.sales):,} rows")

print(f"\n{'='*60}")
print(f"Step 5: tag_model_groups_sup")
print(f"{'='*60}")
sup = tag_model_groups_sup(sup=sup, settings=settings, verbose=verbose)
print("\nModel group distribution (universe):")
print(sup.universe["model_group"].value_counts(dropna=False).to_string())

print(f"\n{'='*60}")
print(f"Step 6: write output")
print(f"{'='*60}")
write_notebook_output_sup(sup, "1-assemble", parquet=True, gpkg=False, shp=False)
print("Done! Output written to out/")
