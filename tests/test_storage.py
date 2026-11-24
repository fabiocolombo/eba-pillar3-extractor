"""Tests for DuckDB and SQLite storage backends, views, and Parquet/CSV exports."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import pyarrow.parquet as pq
import pytest

from eba_p3dh.storage import Pillar3Store


@pytest.mark.parametrize("backend", ["duckdb", "sqlite"])
def test_store_roundtrip_view_and_exports(tmp_path: Path, backend: Literal["duckdb", "sqlite"]) -> None:
    db_path = tmp_path / f"test.{backend}"
    export_dir = tmp_path / f"exports_{backend}"

    with Pillar3Store(db_path, backend=backend) as store:
        store.upsert_dimension(
            "dim_modules",
            [
                {
                    "module_vid": 451,
                    "module_id": 10,
                    "module_code": "IRRBBDIS",
                    "module_name": "IRRBB disclosures",
                    "module_version_code": "IRRBBDIS",
                    "module_version_name": "IRRBB 4.1",
                    "framework_id": 41,
                    "from_reference_date": "2025-06-30",
                    "to_reference_date": None,
                }
            ],
        )
        store.upsert_dimension(
            "dim_entities",
            [
                {
                    "entity_key": "266753_1",
                    "entity_id": "266753",
                    "entity_version": 1,
                    "lei": "J48C8PCSJVUBR8KCW529",
                    "entity_name": "Example Italian Bank S.p.A.",
                    "short_name": "Example Bank",
                    "country": "Italy",
                    "entity_type": "CRD Credit Institution",
                    "institution_type": "Large highest EEA",
                    "authority": "BI",
                    "is_highest_level_eea": "Yes",
                    "is_listed": "Yes",
                    "is_osii_gsii": "Yes",
                    "is_snci": "No",
                    "financial_year_end": "12-31",
                }
            ],
        )
        store.upsert_dimension(
            "dim_templates",
            [
                {
                    "template_id": 1951,
                    "template_code": "K_68.00",
                    "template_name": "EU IRRBB1 – Interest rate risks of non-trading book activities",
                    "is_abstract": 0,
                    "has_open_rows": 0,
                    "has_open_sheets": 0,
                }
            ],
        )
        store.upsert_dimension(
            "dim_tables",
            [
                {
                    "table_vid": 6178,
                    "table_code": "K_68.00",
                    "table_name": "EU IRRBB1",
                    "template_id": 1951,
                    "has_open_rows": 0,
                    "has_open_sheets": 0,
                }
            ],
        )
        store.upsert_dimension(
            "dim_variables",
            [
                {
                    "variable_id": 3525475,
                    "variable_type": "fact",
                    "main_property": "Change in EVE",
                    "data_type": "monetary",
                }
            ],
        )
        store.upsert_dimension(
            "dim_submissions",
            [
                {
                    "instance_id": 3325837,
                    "entity_key": "266753_1",
                    "entity_id": "266753",
                    "module_vid": 451,
                    "reference_date": "2025-06-30",
                    "is_current": 1,
                    "is_accepted": 1,
                    "latest_submission": "Yes",
                    "status": "Accepted",
                    "reception_date": "2026-01-26T10:53:19+00:00",
                    "integration_datetime": None,
                    "submission_name": "sample_irrbb",
                    "report_file_link": None,
                }
            ],
        )

        # Includes all-None columns (sheet_code, sheet_label, key_descriptor) to verify Arrow/DuckDB handling
        sample_facts = [
            {
                "instance_id": 3325837,
                "table_vid": 6178,
                "template_id": 1951,
                "template_code": "K_68.00",
                "table_code": "K_68.00",
                "variable_id": 3525475,
                "cell_id": "350363",
                "sheet_code": None,
                "sheet_label": None,
                "row_code": "0010",
                "row_label": "1. Parallel up",
                "col_code": "0010",
                "col_label": "a. Current period",
                "table_key_num": 0,
                "unique_key_id": "",
                "key_descriptor": None,
                "data_type": "monetary",
                "currency_code": "EUR",
                "quant_value_rep": -221532000.0,
                "quant_value_eur": -221532000.0,
            }
        ]
        written = store.replace_submission_datapoints(
            3325837,
            sample_facts,
            reception_date="2026-01-26T10:53:19+00:00",
            integration_datetime=None,
        )
        assert written == 1

        # Idempotent replace should not duplicate rows
        store.replace_submission_datapoints(
            3325837,
            sample_facts,
            reception_date="2026-01-26T10:53:19+00:00",
            integration_datetime=None,
        )
        cols, rows = store.query_sql("SELECT lei, country, module_code, quant_value_eur FROM v_pillar3_datapoints")
        assert cols == ["lei", "country", "module_code", "quant_value_eur"]
        assert rows == [("J48C8PCSJVUBR8KCW529", "Italy", "IRRBBDIS", -221532000.0)]

        summary = store.get_status_summary()
        assert summary["synced_submissions"] == 1
        assert summary["total_datapoints"] == 1
        assert summary["reference_dates"] == ["2025-06-30"]

        # Test both flat and star exports in Parquet + CSV
        flat_files = store.export_datasets(export_dir / "flat", ["parquet", "csv"], layout="flat")
        assert len(flat_files) == 2
        pq_tbl = pq.read_table(export_dir / "flat" / "pillar3_datapoints.parquet")
        assert pq_tbl.num_rows == 1
        assert pq_tbl.column("lei").to_pylist() == ["J48C8PCSJVUBR8KCW529"]

        star_files = store.export_datasets(export_dir / "star", ["parquet"], layout="star")
        assert len(star_files) == 7
