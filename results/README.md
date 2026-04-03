# Results

Published outputs from the Berks County AVM pipeline. Manually copied from `notebooks/pipeline/data/us-pa-berks/out/` after a meaningful pipeline run.

## ratio_study/

Ratio study reports by model group. Each subdirectory contains:
- `ratio_study.html` — full ratio study with breakdowns by sale price decile, building area, age, land area, condition, neighborhood, and school district
- `ratio_study.md` — plain-text version of the same report

| Group | Description |
|---|---|
| `residential_sf/` | CLASS=R parcels (133,855 parcels); median ratio 1.02, COD 12.6 (trimmed) |
| `commercial/` | CLASS=C, I, and A parcels (apartments merged in — 256 CLASS=A parcels have too few sales to model separately); median ratio 3.03 improved (1 test sale — statistically meaningless) |
| `vacant/` | Cross-cutting vacant parcels; median ratio 1.79, COD 91.3 (trimmed) |

Vacant lot valuation within residential_sf uses a separate sub-model (median ratio 1.39, COD 77.2 trimmed). See the main README for full model performance table.

**Run date:** 2026-04-03
