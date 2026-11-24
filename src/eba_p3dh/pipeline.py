"""Incremental sync orchestrator for the EBA Pillar 3 Data Hub."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from eba_p3dh.client import EDAPClient
from eba_p3dh.config import (
    NON_QUANTITATIVE_MODULE_CODES,
    ExtractorConfig,
    SyncFilter,
)
from eba_p3dh.storage import Pillar3Store


@dataclass
class CatalogSnapshot:
    """In-memory snapshot of EBA Pillar 3 dimensions and submission catalog."""

    modules: list[dict[str, Any]]
    entities: list[dict[str, Any]]
    templates: list[dict[str, Any]]
    tables: list[dict[str, Any]]
    variables: list[dict[str, Any]]
    submissions: list[dict[str, Any]]


@dataclass
class SyncSummary:
    """Metrics returned at the end of a pipeline sync run."""

    catalog_submissions: int = 0
    matched_submissions: int = 0
    skipped_already_synced: int = 0
    extracted_submissions: int = 0
    failed_submissions: int = 0
    total_datapoints_written: int = 0
    exported_files: list[Path] = field(default_factory=list)
    errors: list[tuple[int, str]] = field(default_factory=list)


ProgressCallback = Callable[[int, int, dict[str, Any], int | None, str | None], None]


def filter_submissions(
    submissions: list[dict[str, Any]],
    *,
    entities_by_key: dict[str, dict[str, Any]],
    modules_by_vid: dict[int, dict[str, Any]],
    filters: SyncFilter,
) -> list[dict[str, Any]]:
    """Apply all configured SyncFilter predicates to the submission catalog."""
    countries_lc = {c.lower() for c in filters.countries}
    leis_uc = {lei.upper() for lei in filters.leis}
    inst_types_lc = {t.lower() for t in filters.institution_types}
    modules_uc = {m.upper() for m in filters.modules}
    ref_dates_set = set(filters.reference_dates)
    name_substr = filters.entity_name_contains.lower() if filters.entity_name_contains else None

    matched: list[dict[str, Any]] = []
    for sub in submissions:
        if filters.accepted_only and sub.get("status") != "Accepted":
            continue
        if filters.latest_only and sub.get("latest_submission") != "Yes":
            continue

        mod = modules_by_vid.get(sub.get("module_vid") or -1, {})
        mod_code = (mod.get("module_code") or "").upper()
        if filters.quantitative_only and mod_code in NON_QUANTITATIVE_MODULE_CODES:
            continue
        if modules_uc and mod_code not in modules_uc:
            continue

        ref_date = sub.get("reference_date")
        if ref_dates_set and ref_date not in ref_dates_set:
            continue
        if filters.reference_date_from and (not ref_date or ref_date < filters.reference_date_from):
            continue
        if filters.reference_date_to and (not ref_date or ref_date > filters.reference_date_to):
            continue

        ent = entities_by_key.get(sub.get("entity_key") or "", {})
        if countries_lc and (ent.get("country") or "").lower() not in countries_lc:
            continue
        if leis_uc and (ent.get("lei") or "").upper() not in leis_uc:
            continue
        if inst_types_lc and (ent.get("institution_type") or "").lower() not in inst_types_lc:
            continue
        if name_substr:
            haystack = f"{ent.get('entity_name') or ''} {ent.get('short_name') or ''}".lower()
            if name_substr not in haystack:
                continue

        enriched = dict(sub)
        enriched["_entity_name"] = ent.get("entity_name") or ent.get("lei") or sub.get("entity_id")
        enriched["_lei"] = ent.get("lei")
        enriched["_country"] = ent.get("country")
        enriched["_module_code"] = mod_code
        matched.append(enriched)

    # Stable deterministic ordering: newest reference date first, then instance_id
    matched.sort(key=lambda s: (s.get("reference_date") or "", s.get("instance_id") or 0), reverse=True)
    return matched


def needs_sync(
    sub: dict[str, Any],
    existing_state: dict[int, dict[str, Any]],
    *,
    force: bool = False,
) -> bool:
    """Return True if a submission has not been synced yet or has a newer reception/integration timestamp."""
    if force:
        return True
    inst_id = int(sub["instance_id"])
    state = existing_state.get(inst_id)
    if not state or state.get("status") != "synced":
        return True
    if state.get("reception_date") != sub.get("reception_date"):
        return True
    if state.get("integration_datetime") != sub.get("integration_datetime"):
        return True
    return False


class Pillar3Pipeline:
    """Coordinates dimension refresh, incremental submission extraction, and file exports."""

    def __init__(
        self,
        config: ExtractorConfig,
        client: EDAPClient,
        store: Pillar3Store,
    ) -> None:
        self.config = config
        self.client = client
        self.store = store

    def refresh_catalog(self, *, include_variables: bool = True) -> CatalogSnapshot:
        """Fetch EBA Pillar 3 dimensions and submission catalog and upsert into local storage."""
        modules = self.client.fetch_modules()
        entities = self.client.fetch_entities()
        templates = self.client.fetch_templates()
        tables = self.client.fetch_tables()
        variables = self.client.fetch_variables() if include_variables else []
        submissions = self.client.fetch_submissions(
            accepted_only=self.config.filters.accepted_only,
            latest_only=self.config.filters.latest_only,
        )

        self.store.upsert_dimension("dim_modules", modules)
        self.store.upsert_dimension("dim_entities", entities)
        self.store.upsert_dimension("dim_templates", templates)
        self.store.upsert_dimension("dim_tables", tables)
        if variables:
            self.store.upsert_dimension("dim_variables", variables)
        self.store.upsert_dimension("dim_submissions", submissions)

        return CatalogSnapshot(
            modules=modules,
            entities=entities,
            templates=templates,
            tables=tables,
            variables=variables,
            submissions=submissions,
        )

    def run_sync(
        self,
        *,
        force: bool = False,
        export: bool = False,
        export_layout: Literal["flat", "star"] = "flat",
        progress_cb: ProgressCallback | None = None,
    ) -> SyncSummary:
        """Run an end-to-end incremental sync according to config.filters."""
        snapshot = self.refresh_catalog(include_variables=True)
        entities_by_key = {e["entity_key"]: e for e in snapshot.entities}
        modules_by_vid = {int(m["module_vid"]): m for m in snapshot.modules}

        matched = filter_submissions(
            snapshot.submissions,
            entities_by_key=entities_by_key,
            modules_by_vid=modules_by_vid,
            filters=self.config.filters,
        )

        existing_state = self.store.get_sync_state()
        pending = [sub for sub in matched if needs_sync(sub, existing_state, force=force)]
        skipped = len(matched) - len(pending)

        if self.config.filters.limit_submissions is not None:
            pending = pending[: self.config.filters.limit_submissions]

        summary = SyncSummary(
            catalog_submissions=len(snapshot.submissions),
            matched_submissions=len(matched),
            skipped_already_synced=skipped,
        )

        total_pending = len(pending)
        templates_filter = self.config.filters.templates or None

        for idx, sub in enumerate(pending, start=1):
            instance_id = int(sub["instance_id"])
            try:
                rows = self.client.fetch_submission_datapoints(
                    instance_id,
                    templates=templates_filter,
                    quantitative_only=self.config.filters.quantitative_only,
                )
                written = self.store.replace_submission_datapoints(
                    instance_id,
                    rows,
                    reception_date=sub.get("reception_date"),
                    integration_datetime=sub.get("integration_datetime"),
                )
                summary.extracted_submissions += 1
                summary.total_datapoints_written += written
                if progress_cb:
                    progress_cb(idx, total_pending, sub, written, None)
            except Exception as exc:
                err_msg = str(exc)
                self.store.record_sync_error(
                    instance_id,
                    err_msg,
                    reception_date=sub.get("reception_date"),
                    integration_datetime=sub.get("integration_datetime"),
                )
                summary.failed_submissions += 1
                summary.errors.append((instance_id, err_msg))
                if progress_cb:
                    progress_cb(idx, total_pending, sub, None, err_msg)

        if export and self.config.storage.export_formats:
            summary.exported_files = self.store.export_datasets(
                self.config.storage.export_dir,
                self.config.storage.export_formats,
                layout=export_layout,
            )

        return summary
