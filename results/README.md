# Results

Published outputs from the Berks County AVM pipeline. Manually copied from `notebooks/pipeline/data/us-pa-berks/out/` after a meaningful pipeline run.

## ratio_study/

Ratio study reports by model group. Each subdirectory contains:
- `ratio_study.html` — full ratio study with breakdowns by sale price decile, building area, age, land area, condition, neighborhood, and school district
- `ratio_study.md` — plain-text version of the same report

| Group | Description |
|---|---|
| `residential_sf/` | CLASS=R parcels (133,855 parcels); main model MAPE 11.4% |
| `residential_mf/` | CLASS=A apartment parcels (256 parcels); very few sales |
| `commercial/` | CLASS=C and I parcels (9,174 parcels); high price variance |
| `vacant/` | Farm/Exempt/unknown vacant parcels (6,996 parcels) |

Vacant lot valuation within residential_sf uses a separate sub-model (ratio 1.01, MAPE 44.9%). See the main README for full model performance table.

**Run date:** 2026-03-31
