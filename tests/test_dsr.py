"""Unit tests for Power BI DSR decoding and semantic query builders."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from eba_p3dh.dsr import (
    DSRError,
    build_querydata_payload,
    decode_dsr_response,
    ms_to_date,
    ms_to_datetime,
    pbi_literal,
    select_col,
    where_eq,
)


def test_decode_dsr_value_dicts_repeat_and_null_bitmasks() -> None:
    response = {
        "results": [
            {
                "result": {
                    "data": {
                        "descriptor": {
                            "Select": [
                                {"Value": "G0", "Name": "instance_id"},
                                {"Value": "G1", "Name": "template_code"},
                                {"Value": "G2", "Name": "sheet_code"},
                                {"Value": "G3", "Name": "quant_value_eur"},
                            ]
                        },
                        "dsr": {
                            "DS": [
                                {
                                    "ValueDicts": {"D0": ["K_68.00", "K_01.00"]},
                                    "RT": [["3325837L", "'K_01.00'", None, "200D"]],
                                    "PH": [
                                        {
                                            "DM0": [
                                                {
                                                    "S": [
                                                        {"N": "G0", "T": 4},
                                                        {"N": "G1", "T": 1, "DN": "D0"},
                                                        {"N": "G2", "T": 1},
                                                        {"N": "G3", "T": 3},
                                                    ],
                                                    "C": [3325837, 0, 100.5],
                                                    "Ø": 4,  # bit 2 (sheet_code) is NULL
                                                },
                                                {
                                                    "C": [200.0],
                                                    "R": 3,  # bits 0 & 1 repeated
                                                    "Ø": 4,  # bit 2 NULL
                                                },
                                                {
                                                    "C": [1, "S1", 300.0],
                                                    "R": 1,  # bit 0 repeated
                                                },
                                            ]
                                        }
                                    ],
                                }
                            ]
                        },
                    }
                }
            }
        ]
    }

    page = decode_dsr_response(response)
    assert len(page.rows) == 3
    assert page.rows[0] == {
        "instance_id": 3325837,
        "template_code": "K_68.00",
        "sheet_code": None,
        "quant_value_eur": 100.5,
    }
    assert page.rows[1] == {
        "instance_id": 3325837,
        "template_code": "K_68.00",
        "sheet_code": None,
        "quant_value_eur": 200.0,
    }
    assert page.rows[2] == {
        "instance_id": 3325837,
        "template_code": "K_01.00",
        "sheet_code": "S1",
        "quant_value_eur": 300.0,
    }
    assert page.restart_tokens == [["3325837L", "'K_01.00'", None, "200D"]]
    assert page.is_complete is False


def test_decode_dsr_scalar_aggregation() -> None:
    response = {
        "results": [
            {
                "result": {
                    "data": {
                        "descriptor": {"Select": [{"Value": "M0", "Name": "cnt"}]},
                        "dsr": {"DS": [{"PH": [{"DM0": [{"M0": 7234}]}], "IC": True}]},
                    }
                }
            }
        ]
    }
    page = decode_dsr_response(response)
    assert page.rows == [{"cnt": 7234}]
    assert page.is_complete is True


def test_decode_dsr_single_column_with_schema() -> None:
    response = {
        "results": [
            {
                "result": {
                    "data": {
                        "descriptor": {"Select": [{"Value": "G0", "Name": "last_refreshed_date"}]},
                        "dsr": {
                            "DS": [
                                {
                                    "PH": [{"DM0": [{"S": [{"N": "G0", "T": 7}], "G0": 1763990400000}]}],
                                    "IC": True,
                                }
                            ]
                        },
                    }
                }
            }
        ]
    }
    page = decode_dsr_response(response)
    assert page.rows == [{"last_refreshed_date": 1763990400000}]


def test_decode_dsr_raises_on_error() -> None:
    with pytest.raises(DSRError):
        decode_dsr_response({"results": [{"result": {"error": {"code": "BadQuery"}}}]})


def test_ms_date_conversions() -> None:
    assert ms_to_date(1751241600000) == date(2025, 6, 30)
    assert ms_to_date("2025-12-31") == date(2025, 12, 31)
    assert ms_to_date(None) is None
    assert ms_to_datetime(1751241600000) == datetime(2025, 6, 30, 0, 0, tzinfo=UTC)


def test_pbi_literal_and_payload_builder() -> None:
    assert pbi_literal("O'Bank") == {"Literal": {"Value": "'O''Bank'"}}
    assert pbi_literal(42) == {"Literal": {"Value": "42L"}}
    payload = build_querydata_payload(
        model_id=123,
        dataset_id="ds-1",
        report_id="rep-1",
        from_entities=[("m", "dm_Module")],
        selects=[select_col("m", "ModuleCode", "module_code")],
        where=[where_eq("m", "ModuleCode", "CODIS")],
        count=100,
    )
    assert payload["modelId"] == 123
    cmd = payload["queries"][0]["Query"]["Commands"][0]["SemanticQueryDataShapeCommand"]
    assert cmd["Binding"]["DataReduction"]["Primary"]["Window"]["Count"] == 100
