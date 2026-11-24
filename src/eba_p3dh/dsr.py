"""Power BI Semantic Query builder and DSR (Data Shape Result) decoder."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any


class DSRError(RuntimeError):
    """Raised when a Power BI querydata response contains a DSR or query error."""


@dataclass(frozen=True)
class DSRPage:
    """Decoded page from a Power BI querydata response."""

    rows: list[dict[str, Any]]
    restart_tokens: list[list[Any]] | None
    is_complete: bool


def ms_to_date(value: Any) -> date | None:
    """Convert Power BI Unix epoch milliseconds (or ISO string) to a Python date."""
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000.0, tz=UTC).date()
    if isinstance(value, str):
        return date.fromisoformat(value[:10])
    return None


def ms_to_datetime(value: Any) -> datetime | None:
    """Convert Power BI Unix epoch milliseconds (or ISO string) to UTC datetime."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000.0, tz=UTC)
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return None


def column_expr(source: str, prop: str) -> dict[str, Any]:
    """Build a Power BI semantic query column expression."""
    return {"Column": {"Expression": {"SourceRef": {"Source": source}}, "Property": prop}}


def select_col(source: str, prop: str, alias: str | None = None) -> dict[str, Any]:
    """Build a Power BI semantic query Select entry for a column."""
    return {
        "Column": {"Expression": {"SourceRef": {"Source": source}}, "Property": prop},
        "Name": alias or prop,
    }


def select_count(source: str, prop: str, alias: str = "cnt") -> dict[str, Any]:
    """Build a Power BI semantic query Select entry for CountNonNull (Function=5)."""
    return {
        "Aggregation": {
            "Expression": column_expr(source, prop),
            "Function": 5,
        },
        "Name": alias,
    }


def pbi_literal(value: Any) -> dict[str, Any]:
    """Encode a Python value as a Power BI semantic query literal."""
    if isinstance(value, bool):
        return {"Literal": {"Value": "true" if value else "false"}}
    if isinstance(value, int):
        return {"Literal": {"Value": f"{value}L"}}
    if isinstance(value, float):
        return {"Literal": {"Value": f"{value}D"}}
    if isinstance(value, date) and not isinstance(value, datetime):
        return {"Literal": {"Value": f"datetime'{value.isoformat()}T00:00:00'"}}
    if isinstance(value, datetime):
        return {"Literal": {"Value": f"datetime'{value.strftime('%Y-%m-%dT%H:%M:%S')}'"}}
    escaped = str(value).replace("'", "''")
    return {"Literal": {"Value": f"'{escaped}'"}}


def where_eq(source: str, prop: str, value: Any) -> dict[str, Any]:
    """Build a Where equality condition."""
    return {
        "Condition": {
            "Comparison": {
                "ComparisonKind": 0,
                "Left": column_expr(source, prop),
                "Right": pbi_literal(value),
            }
        }
    }


def where_not_null(source: str, prop: str) -> dict[str, Any]:
    """Build a Where IS NOT NULL condition."""
    return {
        "Condition": {
            "Not": {
                "Expression": {
                    "Comparison": {
                        "ComparisonKind": 0,
                        "Left": column_expr(source, prop),
                        "Right": {"Literal": {"Value": "null"}},
                    }
                }
            }
        }
    }


def where_in(source: str, prop: str, values: list[Any]) -> dict[str, Any]:
    """Build a Where IN (...) condition."""
    return {
        "Condition": {
            "In": {
                "Expressions": [column_expr(source, prop)],
                "Values": [[pbi_literal(v)] for v in values],
            }
        }
    }


def where_or(left_cond: dict[str, Any], right_cond: dict[str, Any]) -> dict[str, Any]:
    """Combine two inner Condition dicts with logical OR."""
    return {
        "Condition": {
            "Or": {
                "Left": left_cond["Condition"],
                "Right": right_cond["Condition"],
            }
        }
    }


def build_querydata_payload(
    *,
    model_id: int,
    dataset_id: str,
    report_id: str,
    from_entities: list[tuple[str, str]],
    selects: list[dict[str, Any]],
    where: list[dict[str, Any]] | None = None,
    order_by: list[tuple[str, str, int]] | None = None,
    count: int = 5000,
    restart_tokens: list[list[Any]] | None = None,
    is_aggregation: bool = False,
) -> dict[str, Any]:
    """Construct a complete Power BI /explore/querydata request body."""
    query: dict[str, Any] = {
        "Version": 2,
        "From": [{"Name": alias, "Entity": entity, "Type": 0} for alias, entity in from_entities],
        "Select": selects,
    }
    if where:
        query["Where"] = where
    if order_by:
        query["OrderBy"] = [
            {"Direction": direction, "Expression": column_expr(src, prop)}
            for src, prop, direction in order_by
        ]

    binding: dict[str, Any] = {
        "Primary": {"Groupings": [{"Projections": list(range(len(selects)))}]},
        "Version": 1,
    }
    if not is_aggregation:
        window: dict[str, Any] = {"Count": count}
        if restart_tokens:
            window["RestartTokens"] = restart_tokens
        binding["DataReduction"] = {"DataVolume": 4, "Primary": {"Window": window}}

    return {
        "version": "1.0.0",
        "queries": [
            {
                "Query": {
                    "Commands": [
                        {
                            "SemanticQueryDataShapeCommand": {
                                "Query": query,
                                "Binding": binding,
                                "ExecutionMetricsKind": 1,
                            }
                        }
                    ]
                },
                "QueryId": "",
                "ApplicationContext": {
                    "DatasetId": dataset_id,
                    "Sources": [{"ReportId": report_id}],
                },
            }
        ],
        "cancelQueries": [],
        "modelId": model_id,
    }


def decode_dsr_response(response_json: dict[str, Any]) -> DSRPage:
    """Decode a Power BI querydata JSON payload into typed Python dicts + restart tokens.

    Handles:
    - Dictionary-encoded columns via ``ValueDicts`` (``DN`` attribute in schema ``S``)
    - Run-length repeated values via the ``R`` bitmask
    - Explicit nulls via the ``Ø`` bitmask
    - Scalar aggregation responses (e.g. ``{"M0": 123}`` without ``C``/``S``)
    - Explicit DSR errors
    """
    results = response_json.get("results")
    if not results:
        raise DSRError(f"Missing 'results' in querydata response: {response_json!r}")

    res0 = results[0].get("result", {})
    if "error" in res0:
        raise DSRError(f"Power BI query error: {res0['error']}")

    data = res0.get("data")
    if not data:
        raise DSRError(f"Missing 'data' in querydata result: {res0!r}")

    dsr = data.get("dsr", {})
    if "DataShapes" in dsr:
        for shape in dsr["DataShapes"]:
            if "odata.error" in shape:
                raise DSRError(f"Power BI DSR DataShape error: {shape['odata.error']}")

    ds_list = dsr.get("DS")
    if not ds_list:
        raise DSRError(f"Missing 'DS' in DSR payload: {dsr!r}")

    ds = ds_list[0]
    if "odata.error" in ds:
        raise DSRError(f"Power BI DS error: {ds['odata.error']}")

    vdicts: dict[str, list[Any]] = ds.get("ValueDicts", {})
    ph = ds.get("PH", [])
    dm0 = ph[0].get("DM0", []) if ph else []
    restart_tokens = ds.get("RT")
    is_complete = bool(ds.get("IC", restart_tokens is None))

    if not dm0:
        return DSRPage(rows=[], restart_tokens=None, is_complete=True)

    selects = data.get("descriptor", {}).get("Select", [])
    val_to_name = {s["Value"]: s["Name"] for s in selects if "Value" in s and "Name" in s}

    first = dm0[0]
    # Scalar aggregation row without columnar C/S (e.g., {"M0": 12345})
    if "C" not in first and "S" not in first:
        rows = [
            {val_to_name.get(k, k): v for k, v in item.items() if k not in ("R", "Ø")}
            for item in dm0
        ]
        return DSRPage(rows=rows, restart_tokens=restart_tokens, is_complete=is_complete)

    schema = first.get("S", [])
    col_names = [val_to_name.get(col["N"], col["N"]) for col in schema]

    rows: list[dict[str, Any]] = []
    prev: list[Any] = [None] * len(schema)

    for item in dm0:
        # Handle inline schema updates if Power BI emits an updated S mid-stream
        if "S" in item and item is not first:
            schema = item["S"]
            col_names = [val_to_name.get(col["N"], col["N"]) for col in schema]
            if len(prev) != len(schema):
                prev = [None] * len(schema)

        c_vals = item.get("C")
        r_mask = item.get("R", 0)
        o_mask = item.get("Ø", 0)

        row_vals: list[Any] = []
        ci = 0
        for idx, col in enumerate(schema):
            bit = 1 << idx
            if o_mask & bit:
                val = None
            elif r_mask & bit:
                val = prev[idx]
            else:
                if c_vals is not None:
                    raw = c_vals[ci]
                    ci += 1
                else:
                    raw = item.get(col["N"])
                dn = col.get("DN")
                if dn and dn in vdicts and isinstance(raw, int):
                    val = vdicts[dn][raw]
                else:
                    val = raw
            row_vals.append(val)

        prev = row_vals
        rows.append(dict(zip(col_names, row_vals, strict=False)))

    return DSRPage(rows=rows, restart_tokens=restart_tokens, is_complete=is_complete)
