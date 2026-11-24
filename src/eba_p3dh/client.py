"""HTTP client for querying the public EBA Pillar 3 Data Hub (EDAP) Power BI model."""

from __future__ import annotations

import base64
import json
import re
import time
import urllib.parse
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from eba_p3dh.config import ClientConfig
from eba_p3dh.dsr import (
    DSRError,
    build_querydata_payload,
    decode_dsr_response,
    ms_to_date,
    ms_to_datetime,
    select_col,
    where_eq,
    where_in,
    where_not_null,
    where_or,
)

_RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
_TOKEN_REFRESH_MARGIN = timedelta(minutes=3)


class EDAPClientError(RuntimeError):
    """Raised when EDAP session initialization or Power BI querying fails."""


@dataclass
class EmbedSession:
    """Public Power BI Embedded session metadata obtained from EDAP."""

    report_id: str
    embed_token: str
    cluster_url: str
    model_id: int
    dataset_id: str
    expires_at: datetime


class EDAPClient:
    """Public client for the EBA Pillar 3 Data Hub hosted on EDAP."""

    def __init__(self, config: ClientConfig | None = None) -> None:
        self.config = config or ClientConfig()
        self._http = httpx.Client(
            follow_redirects=True,
            timeout=self.config.timeout_seconds,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (compatible; eba-pillar3-extractor/0.1; "
                    "+https://www.eba.europa.eu/risk-and-data-analysis/pillar-3-data-hub)"
                )
            },
        )
        self._session: EmbedSession | None = None

    def close(self) -> None:
        """Close the underlying HTTP connection pool."""
        self._http.close()

    def __enter__(self) -> EDAPClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def ensure_session(self, *, force_refresh: bool = False) -> EmbedSession:
        """Obtain or refresh the public Power BI Embedded session from EDAP."""
        now = datetime.now(tz=UTC)
        if (
            not force_refresh
            and self._session is not None
            and now + _TOKEN_REFRESH_MARGIN < self._session.expires_at
        ):
            return self._session

        base = self.config.edap_base_url.rstrip("/")
        # Step 1: Initialize public session cookies on EDAP
        home_resp = self._http.get(f"{base}/")
        home_resp.raise_for_status()

        # Step 2: Load the Pillar 3 report page to obtain the public embed config
        report_url = f"{base}/Report/index/{self.config.report_nav_code}"
        report_resp = self._http.get(report_url)
        report_resp.raise_for_status()
        html = report_resp.text

        report_id, embed_token, cluster_url, expires_at = _parse_edap_embed_html(html)

        # Step 3: Resolve Power BI semantic model ID and dataset ID
        me_url = f"{cluster_url}/explore/reports/{report_id}/modelsAndExploration?preferReadOnlySession=true"
        me_resp = self._http.get(
            me_url,
            headers={"Authorization": f"EmbedToken {embed_token}", "Accept": "application/json"},
        )
        me_resp.raise_for_status()
        me_data = me_resp.json()
        models = me_data.get("models") or []
        if not models:
            raise EDAPClientError("No Power BI semantic models found in EDAP report response.")

        model_id = int(models[0]["id"])
        dataset_id = str(models[0]["dbName"])

        self._session = EmbedSession(
            report_id=report_id,
            embed_token=embed_token,
            cluster_url=cluster_url,
            model_id=model_id,
            dataset_id=dataset_id,
            expires_at=expires_at,
        )
        return self._session

    def query_page(
        self,
        *,
        from_entities: list[tuple[str, str]],
        selects: list[dict[str, Any]],
        where: list[dict[str, Any]] | None = None,
        order_by: list[tuple[str, str, int]] | None = None,
        count: int | None = None,
        restart_tokens: list[list[Any]] | None = None,
        is_aggregation: bool = False,
    ) -> tuple[list[dict[str, Any]], list[list[Any]] | None]:
        """Execute a single Power BI querydata request with token refresh and retries."""
        page_size = count or self.config.page_size
        attempt = 0
        refreshed_auth = False

        while True:
            session = self.ensure_session()
            payload = build_querydata_payload(
                model_id=session.model_id,
                dataset_id=session.dataset_id,
                report_id=session.report_id,
                from_entities=from_entities,
                selects=selects,
                where=where,
                order_by=order_by,
                count=page_size,
                restart_tokens=restart_tokens,
                is_aggregation=is_aggregation,
            )
            url = f"{session.cluster_url}/explore/querydata?synchronous=true"
            try:
                resp = self._http.post(
                    url,
                    headers={
                        "Authorization": f"EmbedToken {session.embed_token}",
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                    },
                    json=payload,
                )
            except httpx.TransportError as exc:
                if attempt >= self.config.max_retries:
                    raise EDAPClientError(f"Network error querying EDAP Power BI: {exc}") from exc
                attempt += 1
                time.sleep(min(2.0**attempt, 16.0))
                continue

            if resp.status_code in (401, 403) and not refreshed_auth:
                refreshed_auth = True
                self.ensure_session(force_refresh=True)
                continue

            if resp.status_code in _RETRYABLE_STATUS_CODES and attempt < self.config.max_retries:
                attempt += 1
                retry_after = resp.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else min(2.0**attempt, 16.0)
                time.sleep(delay)
                continue

            resp.raise_for_status()
            try:
                page = decode_dsr_response(resp.json())
            except DSRError as exc:
                raise EDAPClientError(f"Failed to decode Power BI response: {exc}") from exc

            return page.rows, page.restart_tokens

    def query_all(
        self,
        *,
        from_entities: list[tuple[str, str]],
        selects: list[dict[str, Any]],
        where: list[dict[str, Any]] | None = None,
        order_by: list[tuple[str, str, int]] | None = None,
        page_size: int | None = None,
    ) -> list[dict[str, Any]]:
        """Paginate through all rows of a Power BI semantic query using RestartTokens."""
        limit = page_size or self.config.page_size
        all_rows: list[dict[str, Any]] = []
        restart_tokens: list[list[Any]] | None = None
        seen_tokens: set[str] = set()

        while True:
            rows, next_tokens = self.query_page(
                from_entities=from_entities,
                selects=selects,
                where=where,
                order_by=order_by,
                count=limit,
                restart_tokens=restart_tokens,
            )
            if not rows:
                break

            # Power BI RestartTokens are inclusive of the previous page's boundary row.
            if restart_tokens is not None and all_rows and rows[0] == all_rows[-1]:
                rows = rows[1:]

            all_rows.extend(rows)

            if not next_tokens:
                break

            token_sig = json.dumps(next_tokens, sort_keys=True)
            if token_sig in seen_tokens:
                break
            seen_tokens.add(token_sig)
            restart_tokens = next_tokens

            if self.config.request_delay_seconds > 0:
                time.sleep(self.config.request_delay_seconds)

        return all_rows

    def fetch_last_refresh_date(self) -> datetime | None:
        """Fetch the timestamp when the EBA last refreshed the EDAP semantic model."""
        rows, _ = self.query_page(
            from_entities=[("r", "LastRefreshDate")],
            selects=[select_col("r", "Last Refreshed Date", "last_refreshed_date")],
            count=1,
        )
        if not rows:
            return None
        return ms_to_datetime(rows[0].get("last_refreshed_date"))

    def fetch_modules(self) -> list[dict[str, Any]]:
        """Fetch all Pillar 3 modules from dm_Module."""
        rows = self.query_all(
            from_entities=[("m", "dm_Module")],
            selects=[
                select_col("m", "ModuleVID", "module_vid"),
                select_col("m", "ModuleID", "module_id"),
                select_col("m", "ModuleCode", "module_code"),
                select_col("m", "ModuleName", "module_name"),
                select_col("m", "ModuleVersionCode", "module_version_code"),
                select_col("m", "ModuleVersionName", "module_version_name"),
                select_col("m", "FrameworkID", "framework_id"),
                select_col("m", "FromReferenceDate", "from_reference_date"),
                select_col("m", "ToReferenceDate", "to_reference_date"),
            ],
        )
        return [
            {
                "module_vid": _to_int(r["module_vid"]),
                "module_id": _to_int(r["module_id"]),
                "module_code": _to_str(r["module_code"]),
                "module_name": _to_str(r["module_name"]),
                "module_version_code": _to_str(r["module_version_code"]),
                "module_version_name": _to_str(r["module_version_name"]),
                "framework_id": _to_int(r["framework_id"]),
                "from_reference_date": _iso_date(ms_to_date(r["from_reference_date"])),
                "to_reference_date": _iso_date(ms_to_date(r["to_reference_date"])),
            }
            for r in rows
            if r.get("module_vid") is not None
        ]

    def fetch_entities(self) -> list[dict[str, Any]]:
        """Fetch all reporting entities and entity versions from dm_Entity."""
        rows = self.query_all(
            from_entities=[("e", "dm_Entity")],
            selects=[
                select_col("e", "EntityKey", "entity_key"),
                select_col("e", "EntityID", "entity_id"),
                select_col("e", "EntityVersion", "entity_version"),
                select_col("e", "EntityCode", "lei"),
                select_col("e", "ENT_NAM", "entity_name"),
                select_col("e", "ENT_NAM_SHO", "short_name"),
                select_col("e", "Country", "country"),
                select_col("e", "EntityType", "entity_type"),
                select_col("e", "InstitutionType", "institution_type"),
                select_col("e", "ENT_AUT", "authority"),
                select_col("e", "IS_HIG_LEV_EEA", "is_highest_level_eea"),
                select_col("e", "IS_ITS_LIS", "is_listed"),
                select_col("e", "IS_OSI_GSI_INS", "is_osii_gsii"),
                select_col("e", "SMA_NON_COM_INS", "is_snci"),
                select_col("e", "REP_FIN_YEA_END", "financial_year_end"),
            ],
        )
        return [
            {
                "entity_key": _to_str(r["entity_key"]),
                "entity_id": _to_str(r["entity_id"]),
                "entity_version": _to_int(r["entity_version"]),
                "lei": _to_str(r["lei"]),
                "entity_name": _to_str(r["entity_name"]),
                "short_name": _to_str(r["short_name"]),
                "country": _to_str(r["country"]),
                "entity_type": _to_str(r["entity_type"]),
                "institution_type": _to_str(r["institution_type"]),
                "authority": _to_str(r["authority"]),
                "is_highest_level_eea": _to_str(r["is_highest_level_eea"]),
                "is_listed": _to_str(r["is_listed"]),
                "is_osii_gsii": _to_str(r["is_osii_gsii"]),
                "is_snci": _to_str(r["is_snci"]),
                "financial_year_end": _to_str(r["financial_year_end"]),
            }
            for r in rows
            if r.get("entity_key") is not None
        ]

    def fetch_templates(self) -> list[dict[str, Any]]:
        """Fetch all Pillar 3 disclosure templates from dm_Template."""
        rows = self.query_all(
            from_entities=[("t", "dm_Template")],
            selects=[
                select_col("t", "TemplateID", "template_id"),
                select_col("t", "TemplateCode", "template_code"),
                select_col("t", "TemplateName", "template_name"),
                select_col("t", "IsAbstract", "is_abstract"),
                select_col("t", "HasOpenRows", "has_open_rows"),
                select_col("t", "HasOpenSheets", "has_open_sheets"),
            ],
        )
        return [
            {
                "template_id": _to_int(r["template_id"]),
                "template_code": _to_str(r["template_code"]),
                "template_name": _to_str(r["template_name"]),
                "is_abstract": _to_int(r["is_abstract"]),
                "has_open_rows": _to_int(r["has_open_rows"]),
                "has_open_sheets": _to_int(r["has_open_sheets"]),
            }
            for r in rows
            if r.get("template_id") is not None
        ]

    def fetch_tables(self) -> list[dict[str, Any]]:
        """Fetch all Pillar 3 disclosure tables from dm_Table."""
        rows = self.query_all(
            from_entities=[("t", "dm_Table")],
            selects=[
                select_col("t", "TableVID", "table_vid"),
                select_col("t", "TableCode", "table_code"),
                select_col("t", "TableName", "table_name"),
                select_col("t", "TemplateID", "template_id"),
                select_col("t", "HasOpenRows", "has_open_rows"),
                select_col("t", "HasOpenSheets", "has_open_sheets"),
            ],
        )
        return [
            {
                "table_vid": _to_int(r["table_vid"]),
                "table_code": _to_str(r["table_code"]),
                "table_name": _to_str(r["table_name"]),
                "template_id": _to_int(r["template_id"]),
                "has_open_rows": _to_int(r["has_open_rows"]),
                "has_open_sheets": _to_int(r["has_open_sheets"]),
            }
            for r in rows
            if r.get("table_vid") is not None
        ]

    def fetch_variables(self) -> list[dict[str, Any]]:
        """Fetch all DPM variable definitions from dm_Variable."""
        rows = self.query_all(
            from_entities=[("v", "dm_Variable")],
            selects=[
                select_col("v", "VariableID", "variable_id"),
                select_col("v", "Type", "variable_type"),
                select_col("v", "MainProperty", "main_property"),
                select_col("v", "DataType", "data_type"),
            ],
        )
        return [
            {
                "variable_id": _to_int(r["variable_id"]),
                "variable_type": _to_str(r["variable_type"]),
                "main_property": _to_str(r["main_property"]),
                "data_type": _to_str(r["data_type"]),
            }
            for r in rows
            if r.get("variable_id") is not None
        ]

    def fetch_submissions(
        self,
        *,
        accepted_only: bool = True,
        latest_only: bool = True,
    ) -> list[dict[str, Any]]:
        """Fetch Pillar 3 submission catalog from dm_ReportInstance."""
        where: list[dict[str, Any]] = []
        if accepted_only:
            where.append(where_eq("r", "Status", "Accepted"))
        if latest_only:
            where.append(where_eq("r", "Latest Submission", "Yes"))

        rows = self.query_all(
            from_entities=[("r", "dm_ReportInstance")],
            selects=[
                select_col("r", "InstanceID", "instance_id"),
                select_col("r", "EntityKey", "entity_key"),
                select_col("r", "EntityID", "entity_id"),
                select_col("r", "ModuleVID", "module_vid"),
                select_col("r", "ReferenceDate", "reference_date"),
                select_col("r", "IsCurrent", "is_current"),
                select_col("r", "IsAccepted", "is_accepted"),
                select_col("r", "Latest Submission", "latest_submission"),
                select_col("r", "Status", "status"),
                select_col("r", "ReceptionDate", "reception_date"),
                select_col("r", "IntegrationDateTime", "integration_datetime"),
                select_col("r", "Name", "submission_name"),
                select_col("r", "Report File Link", "report_file_link"),
            ],
            where=where or None,
        )
        return [
            {
                "instance_id": _to_int(r["instance_id"]),
                "entity_key": _to_str(r["entity_key"]),
                "entity_id": _to_str(r["entity_id"]),
                "module_vid": _to_int(r["module_vid"]),
                "reference_date": _iso_date(ms_to_date(r["reference_date"])),
                "is_current": _to_int(r["is_current"]),
                "is_accepted": _to_int(r["is_accepted"]),
                "latest_submission": _to_str(r["latest_submission"]),
                "status": _to_str(r["status"]),
                "reception_date": _iso_dt(ms_to_datetime(r["reception_date"])),
                "integration_datetime": _iso_dt(ms_to_datetime(r["integration_datetime"])),
                "submission_name": _to_str(r["submission_name"]),
                "report_file_link": _to_str(r["report_file_link"]),
            }
            for r in rows
            if r.get("instance_id") is not None
        ]

    def fetch_submission_datapoints(
        self,
        instance_id: int,
        *,
        templates: list[str] | None = None,
        quantitative_only: bool = True,
    ) -> list[dict[str, Any]]:
        """Fetch normalized quantitative data points for a single submission instance."""
        where: list[dict[str, Any]] = [where_eq("r", "InstanceID", str(instance_id))]
        if quantitative_only:
            where.append(
                where_or(
                    where_not_null("f", "QuantValueRep"),
                    where_not_null("f", "QuantValueConv"),
                )
            )
        if templates:
            where.append(where_in("tp", "TemplateCode", templates))

        rows = self.query_all(
            from_entities=[
                ("f", "fact_Value"),
                ("r", "dm_ReportInstance"),
                ("t", "dm_Table"),
                ("tp", "dm_Template"),
                ("rw", "dm_Row"),
                ("cl", "dm_Column"),
                ("sh", "dm_Sheet"),
                ("v", "dm_Variable"),
                ("tk", "dm_TableKey"),
            ],
            selects=[
                select_col("f", "InstanceID", "instance_id"),
                select_col("f", "TableVID", "table_vid"),
                select_col("tp", "TemplateID", "template_id"),
                select_col("tp", "TemplateCode", "template_code"),
                select_col("t", "TableCode", "table_code"),
                select_col("f", "VariableID", "variable_id"),
                select_col("f", "CellID", "cell_id"),
                select_col("sh", "HeaderCode", "sheet_code"),
                select_col("sh", "HeaderLabel", "sheet_label"),
                select_col("rw", "HeaderCode", "row_code"),
                select_col("rw", "HeaderLabel", "row_label"),
                select_col("cl", "HeaderCode", "col_code"),
                select_col("cl", "HeaderLabel", "col_label"),
                select_col("f", "TableKeyNum", "table_key_num"),
                select_col("f", "UniqueKeyID", "unique_key_id"),
                select_col("tk", "KeyDescriptor", "key_descriptor"),
                select_col("v", "DataType", "data_type"),
                select_col("f", "CurrencyCode", "currency_code"),
                select_col("f", "QuantValueRep", "quant_value_rep"),
                select_col("f", "QuantValueConv", "quant_value_eur"),
            ],
            where=where,
        )

        normalized: list[dict[str, Any]] = []
        for r in rows:
            normalized.append(
                {
                    "instance_id": _to_int(r["instance_id"]) or instance_id,
                    "table_vid": _to_int(r["table_vid"]),
                    "template_id": _to_int(r["template_id"]),
                    "template_code": _to_str(r["template_code"]),
                    "table_code": _to_str(r["table_code"]),
                    "variable_id": _to_int(r["variable_id"]),
                    "cell_id": _to_str(r["cell_id"]) or "",
                    "sheet_code": _to_str(r["sheet_code"]),
                    "sheet_label": _to_str(r["sheet_label"]),
                    "row_code": _to_str(r["row_code"]),
                    "row_label": _to_str(r["row_label"]),
                    "col_code": _to_str(r["col_code"]),
                    "col_label": _to_str(r["col_label"]),
                    "table_key_num": _to_int(r["table_key_num"]) or 0,
                    "unique_key_id": _to_str(r["unique_key_id"]) or "",
                    "key_descriptor": _to_str(r["key_descriptor"]),
                    "data_type": _to_str(r["data_type"]),
                    "currency_code": _to_str(r["currency_code"]),
                    "quant_value_rep": _to_float(r["quant_value_rep"]),
                    "quant_value_eur": _to_float(r["quant_value_eur"]),
                }
            )
        return normalized


def _parse_edap_embed_html(html: str) -> tuple[str, str, str, datetime]:
    """Extract Power BI Embedded parameters from an EDAP report HTML page."""
    match = re.search(r"loadReport\s*\((.*?)\);", html, re.S)
    if not match:
        raise EDAPClientError("Could not locate loadReport(...) embed initialization on EDAP page.")

    raw_args = [line.strip().lstrip(",").strip() for line in match.group(1).splitlines() if line.strip()]
    if len(raw_args) < 5:
        raise EDAPClientError(f"Unexpected loadReport(...) argument count: {len(raw_args)}")

    report_id = raw_args[2].strip("\"'")
    embed_token = raw_args[3].strip("\"'")
    embed_url = raw_args[4].strip("\"'")

    qs = urllib.parse.parse_qs(urllib.parse.urlparse(embed_url).query)
    if "config" not in qs:
        raise EDAPClientError("Missing 'config' query parameter in EDAP Power BI embedUrl.")

    cfg_b64 = qs["config"][0]
    # Add base64 padding if stripped
    cfg_b64 += "=" * (-len(cfg_b64) % 4)
    cfg = json.loads(base64.b64decode(cfg_b64).decode("utf-8"))
    cluster_url = (cfg.get("clusterUrl") or cfg.get("clusterUri") or "").rstrip("/")
    if not cluster_url:
        raise EDAPClientError(f"Missing clusterUrl in Power BI embed config: {cfg!r}")

    exp_match = re.search(r'id="EmbedTokenExpiration"\s+value=[\'"]([^\'"]+)[\'"]', html)
    if exp_match:
        expires_at = datetime.fromisoformat(exp_match.group(1).replace("Z", "+00:00"))
    else:
        expires_at = datetime.now(tz=UTC) + timedelta(minutes=25)

    return report_id, embed_token, cluster_url, expires_at


def _to_int(val: Any) -> int | None:
    if val is None or val == "":
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _to_float(val: Any) -> float | None:
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _to_str(val: Any) -> str | None:
    if val is None:
        return None
    return str(val)


def _iso_date(val: Any) -> str | None:
    return val.isoformat() if val is not None else None


def _iso_dt(val: Any) -> str | None:
    return val.isoformat() if val is not None else None
