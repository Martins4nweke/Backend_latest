# NCD Allocation Backend v6.3

This version adds an evidence-aware retrieval waterfall for missing parameters.

## What v6.3 adds

- Uploaded values remain first priority
- Public-source retrieval before imputation
- Parameter-level provenance for population, prevalence, cost, and DALY
- Stronger population-basis checks to reduce prevalence double counting
- Scenario trade-off outputs: best efficiency, best equity, best balanced
- Expanded Word report with executive summary, comparative tables, and provenance preview

## Runtime evidence strategy

For each missing value, the backend uses this order:

1. Uploaded value
2. Country evidence pack if configured
3. Public official source where available
4. Proxy or bundle match
5. Imputation only as the last resort

## Supported endpoints

- `POST /api/v1/allocate`
- `POST /api/v1/compare-scenarios`
- `POST /api/v1/export-word`
- `POST /api/v1/evidence-fill-preview`
- `GET /api/v1/source-readiness`

## Optional environment variables

- `FRONTEND_ORIGINS`
- `LIVE_SOURCE_TIMEOUT_SECONDS`
- `COUNTRY_EVIDENCE_PACK_DIR`
- `WORLD_BANK_API_URL`
- `GBD_CSV_URL`
- `WHO_CHOICE_CSV_URL`
- `WHO_GHO_CSV_URL`

## Evidence pack convention

If `COUNTRY_EVIDENCE_PACK_DIR` is set, the backend can read these files when present:

- `country_metrics.csv`
- `gbd_latest.csv`
- `who_choice_costs.csv`

These local packs let you stabilise the runtime even when public sites change format.
