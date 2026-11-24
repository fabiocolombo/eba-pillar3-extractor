"""Tests for filtering and incremental sync behavior in Pillar3Pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eba_p3dh.config import ExtractorConfig, StorageConfig, SyncFilter
from eba_p3dh.pipeline import Pillar3Pipeline
from eba_p3dh.storage import Pillar3Store


class FakeEDAPClient:
    """In-memory mock of EDAPClient for deterministic pipeline testing."""

    def __init__(self) -> None:
        self.datapoint_calls: list[int] = []
        self.submissions: list[dict[str, Any]] = [
            {
                "instance_id": 1001,
                "entity_key": "E1_1",
                "entity_id": "E1",
                "module_vid": 443,
                "reference_date": "2025-12-31",
                "is_current": 1,
                "is_accepted": 1,
                "latest_submission": "Yes",
                "status": "Accepted",
                "reception_date": "2026-03-01T10:00:00+00:00",
                "integration_datetime": None,
                "submission_name": "bank_de_codis",
                "report_file_link": None,
            },
            {
                "instance_id": 1002,
                "entity_key": "E2_1",
                "entity_id": "E2",
                "module_vid": 449,
                "reference_date": "2025-12-31",
                "is_current": 1,
                "is_accepted": 1,
                "latest_submission": "Yes",
                "status": "Accepted",
                "reception_date": "2026-03-02T10:00:00+00:00",
                "integration_datetime": None,
                "submission_name": "bank_fr_esgdis",
                "report_file_link": None,
            },
            {
                "instance_id": 1003,
                "entity_key": "E1_1",
                "entity_id": "E1",
                "module_vid": 441,  # PDF-only module -> excluded when quantitative_only=True
                "reference_date": "2025-12-31",
                "is_current": 1,
                "is_accepted": 1,
                "latest_submission": "Yes",
                "status": "Accepted",
                "reception_date": "2026-03-01T10:00:00+00:00",
                "integration_datetime": None,
                "submission_name": "bank_de_pdf",
                "report_file_link": None,
            },
        ]

    def fetch_modules(self) -> list[dict[str, Any]]:
        return [
            {
                "module_vid": 443,
                "module_id": 1,
                "module_code": "CODIS",
                "module_name": "Common disclosures",
                "module_version_code": "CODIS",
                "module_version_name": "CODIS 4.1",
                "framework_id": 41,
                "from_reference_date": "2025-06-30",
                "to_reference_date": None,
            },
            {
                "module_vid": 449,
                "module_id": 2,
                "module_code": "ESGDIS",
                "module_name": "ESG disclosures",
                "module_version_code": "ESGDIS",
                "module_version_name": "ESGDIS 4.1",
                "framework_id": 41,
                "from_reference_date": "2025-06-30",
                "to_reference_date": None,
            },
            {
                "module_vid": 441,
                "module_id": 3,
                "module_code": "P3_DIS_DOCS",
                "module_name": "PDF docs",
                "module_version_code": "P3_DIS_DOCS",
                "module_version_name": "PDF",
                "framework_id": 41,
                "from_reference_date": "2025-06-30",
                "to_reference_date": None,
            },
        ]

    def fetch_entities(self) -> list[dict[str, Any]]:
        return [
            {
                "entity_key": "E1_1",
                "entity_id": "E1",
                "entity_version": 1,
                "lei": "LEI_GERMANY_00000001",
                "entity_name": "Deutsche Musterbank AG",
                "short_name": "Musterbank",
                "country": "Germany",
                "entity_type": "CRD Credit Institution",
                "institution_type": "Large highest EEA",
                "authority": "BaFin",
                "is_highest_level_eea": "Yes",
                "is_listed": "Yes",
                "is_osii_gsii": "Yes",
                "is_snci": "No",
                "financial_year_end": "12-31",
            },
            {
                "entity_key": "E2_1",
                "entity_id": "E2",
                "entity_version": 1,
                "lei": "LEI_FRANCE_000000002",
                "entity_name": "Banque Exemple SA",
                "short_name": "Exemple",
                "country": "France",
                "entity_type": "CRD Credit Institution",
                "institution_type": "Large highest EEA",
                "authority": "ACPR",
                "is_highest_level_eea": "Yes",
                "is_listed": "Yes",
                "is_osii_gsii": "Yes",
                "is_snci": "No",
                "financial_year_end": "12-31",
            },
        ]

    def fetch_templates(self) -> list[dict[str, Any]]:
        return [
            {
                "template_id": 100,
                "template_code": "K_01.00",
                "template_name": "EU KM1 - Key metrics",
                "is_abstract": 0,
                "has_open_rows": 0,
                "has_open_sheets": 0,
            }
        ]

    def fetch_tables(self) -> list[dict[str, Any]]:
        return [
            {
                "table_vid": 200,
                "table_code": "K_01.00",
                "table_name": "EU KM1",
                "template_id": 100,
                "has_open_rows": 0,
                "has_open_sheets": 0,
            }
        ]

    def fetch_variables(self) -> list[dict[str, Any]]:
        return [
            {
                "variable_id": 300,
                "variable_type": "fact",
                "main_property": "CET1 capital ratio",
                "data_type": "percentage",
            }
        ]

    def fetch_submissions(self, *, accepted_only: bool = True, latest_only: bool = True) -> list[dict[str, Any]]:
        return list(self.submissions)

    def fetch_submission_datapoints(
        self,
        instance_id: int,
        *,
        templates: list[str] | None = None,
        quantitative_only: bool = True,
    ) -> list[dict[str, Any]]:
        self.datapoint_calls.append(instance_id)
        return [
            {
                "instance_id": instance_id,
                "table_vid": 200,
                "template_id": 100,
                "template_code": "K_01.00",
                "table_code": "K_01.00",
                "variable_id": 300,
                "cell_id": f"cell_{instance_id}",
                "sheet_code": None,
                "sheet_label": None,
                "row_code": "0050",
                "row_label": "Common Equity Tier 1 (CET1) ratio (%)",
                "col_code": "0010",
                "col_label": "T",
                "table_key_num": 0,
                "unique_key_id": "",
                "key_descriptor": None,
                "data_type": "percentage",
                "currency_code": None,
                "quant_value_rep": 0.145,
                "quant_value_eur": 0.145,
            }
        ]


def test_pipeline_filters_and_incremental_sync(tmp_path: Path) -> None:
    cfg = ExtractorConfig(
        filters=SyncFilter(countries=["Germany"]),
        storage=StorageConfig(backend="duckdb", db_path=tmp_path / "p3.duckdb", export_dir=tmp_path / "exp"),
    )
    client = FakeEDAPClient()

    with Pillar3Store(cfg.storage.db_path, cfg.storage.backend) as store:
        pipeline = Pillar3Pipeline(cfg, client, store)  # type: ignore[arg-type]

        # Run 1: extracts instance 1001 (Germany + CODIS), ignores 1002 (France) and 1003 (PDF-only)
        s1 = pipeline.run_sync(export=True)
        assert s1.matched_submissions == 1
        assert s1.extracted_submissions == 1
        assert s1.skipped_already_synced == 0
        assert s1.total_datapoints_written == 1
        assert client.datapoint_calls == [1001]
        assert len(s1.exported_files) == 1

        # Run 2: incremental — 1001 already synced with same timestamp -> skipped!
        s2 = pipeline.run_sync()
        assert s2.matched_submissions == 1
        assert s2.skipped_already_synced == 1
        assert s2.extracted_submissions == 0
        assert client.datapoint_calls == [1001]

        # Run 3: reception_date updates -> automatically re-synced!
        client.submissions[0]["reception_date"] = "2026-04-01T10:00:00+00:00"
        s3 = pipeline.run_sync()
        assert s3.extracted_submissions == 1
        assert s3.skipped_already_synced == 0
        assert client.datapoint_calls == [1001, 1001]
