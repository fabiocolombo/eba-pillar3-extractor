# EBA Pillar 3 Data Hub Extractor (`eba-p3dh`)

A Python CLI pipeline to extract **normalized quantitative Pillar 3 disclosures** from the European Banking Authority's [Pillar 3 Data Hub (P3DH)](https://www.eba.europa.eu/risk-and-data-analysis/pillar-3-data-hub) via the public [European Data Access Portal (EDAP)](https://edap-public.eba.europa.eu/).

## Why this architecture?

EDAP exposes the Pillar 3 Data Hub through public Power BI Embedded reports (whose UI exports are capped at 30,000 CSV / 150,000 XLSX rows). `eba-p3dh` connects directly to the public EDAP session and queries the underlying Power BI semantic model with cursor-based `RestartTokens` pagination — extracting clean, typed, normalized data points per submission with:

- **Configurable filtering**: by country, institution LEI, entity name, institution type, Pillar 3 module (`CODIS`, `FINDIS`, `ESGDIS`, `IRRBBDIS`, `MRELTLACDIS`, `REMDIS`, `GSIIDIS`), template code (e.g. `K_01.00`, `K_41.00`, `K_68.00`), and reference dates.
- **Incremental sync**: tracks every extracted `instance_id` and its `reception_date`/`integration_datetime` in `sync_state`, skipping unchanged submissions on subsequent runs and automatically re-syncing resubmissions.
- **Dual local database support**: stores data in a normalized star schema in **DuckDB** (default) or **SQLite**, with a ready-to-query analytical view `v_pillar3_datapoints`.
- **Parquet & CSV exports**: export either the flat joined analytical view (`--layout flat`) or all normalized star-schema tables (`--layout star`).

## Setup

Requires Python 3.11+ and [`uv`](https://docs.astral.sh/uv/).

```bash
cd eba-pillar3-extractor
uv sync
```

## Quick Start

### 1. Inspect the live catalog (without downloading facts)

```bash
# View live counts by module, reference date, and country
uv run eba-p3dh catalog

# Filter the catalog preview
uv run eba-p3dh catalog --country Germany --module CODIS --ref-date 2025-12-31
```

### 2. Run an incremental sync

```bash
# Sync all latest accepted quantitative submissions (DuckDB default: data/eba_pillar3.duckdb)
uv run eba-p3dh sync

# Sync specific countries/modules/dates and export to Parquet + CSV
uv run eba-p3dh sync \
  --country Germany --country France \
  --module CODIS --module ESGDIS \
  --ref-date 2025-12-31 --ref-date 2026-06-30 \
  --export --format parquet --format csv

# Sync using SQLite instead of DuckDB
uv run eba-p3dh sync --backend sqlite --module IRRBBDIS --limit 10

# Sync using a declarative TOML config file
cp config.example.toml config.toml
uv run eba-p3dh sync --config config.toml --export
```

Re-running `eba-p3dh sync` with the same database only fetches newly published or updated submissions. Pass `--force` to re-extract already-synced submissions.

### 3. Check local coverage, export, and query with SQL

```bash
# Show local DB metrics and module breakdown
uv run eba-p3dh status

# Export flat analytical dataset or full star schema to Parquet/CSV
uv run eba-p3dh export --format parquet --format csv --layout flat
uv run eba-p3dh export --format parquet --layout star --export-dir data/exports_star

# Query locally with SQL (outputs table, csv, or json)
uv run eba-p3dh query "
SELECT lei, entity_name, country, reference_date, template_code,
       row_code, row_label, col_code, currency_code, quant_value_eur
FROM v_pillar3_datapoints
WHERE template_code = 'K_01.00'
LIMIT 20
"
```

## Local Database Schema

The pipeline stores normalized tables mirroring the EBA Data Point Model (DPM):

| Table / View | Primary Key | Description |
| --- | --- | --- |
| `dim_entities` | `entity_key` | EEA institutions (LEI, official name, country, entity type, institution size classification, listing/G-SII flags) |
| `dim_modules` | `module_vid` | Pillar 3 modules (`CODIS`, `FINDIS`, `ESGDIS`, `IRRBBDIS`, `MRELTLACDIS`, `REMDIS`, `GSIIDIS`) across DPM versions |
| `dim_templates` | `template_id` | Pillar 3 disclosure templates (`K_01.00`, `K_41.00`, `K_68.00`, ...) |
| `dim_tables` | `table_vid` | Concrete DPM tables belonging to each template |
| `dim_variables` | `variable_id` | DPM variable metadata (`main_property`, `data_type`) |
| `dim_submissions` | `instance_id` | Submission catalog (`reference_date`, `status`, `latest_submission`, `reception_date`, `report_file_link`) |
| `fact_datapoints` | *(indexed by `instance_id`, `template_code`)* | Quantitative data points with `sheet_code`/`row_code`/`col_code` + labels, `key_descriptor` (for open-row/open-sheet tables), `currency_code`, `quant_value_rep` (reported units), and `quant_value_eur` (EUR-converted by EBA) |
| `sync_state` | `instance_id` | Tracks incremental sync status, timestamps, row counts, and per-submission errors |
| `v_pillar3_datapoints` | *(view)* | Denormalized analytical view joining `fact_datapoints` with all dimension tables |

### Units & Currency Notes

Per EBA ITS (EU) 2024/3172 and the EBA Pillar 3 Data Hub FAQs:
- Monetary figures are stored in **currency units** (not truncated to millions).
- `quant_value_rep` holds the value in the institution's original reporting currency (`currency_code`), while `quant_value_eur` holds the EBA's EUR-converted value (using ECB reference exchange rates).
- Percentages are stored as ratios per unit (e.g. `0.145` = `14.5%`).

## Development & Tests

```bash
uv run pytest -v
```
