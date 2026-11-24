"""Configuration models and TOML loader for the EBA Pillar 3 Data Hub extractor."""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

# Modules in EDAP that carry quantitative XBRL-CSV tables (excluding PDF-only & contact modules)
QUANTITATIVE_MODULE_CODES: tuple[str, ...] = (
    "CODIS",
    "CONDIS",
    "ESGDIS",
    "FINDIS",
    "GSIIDIS",
    "IRRBBDIS",
    "MRELTLACDIS",
    "REMDIS",
)

NON_QUANTITATIVE_MODULE_CODES: frozenset[str] = frozenset(
    {
        "P3_DIS_DOCS",
        "P3_NONREM_DIS_DOCS",
        "P3_REM_DIS_DOCS",
        "P3DH",
    }
)


class SyncFilter(BaseModel):
    """Filter criteria controlling which Pillar 3 submissions and data points are synced."""

    latest_only: bool = Field(
        default=True,
        description="Sync only the latest accepted submission per entity, module, and reference date.",
    )
    accepted_only: bool = Field(
        default=True,
        description="Restrict to submissions with Status = 'Accepted'.",
    )
    quantitative_only: bool = Field(
        default=True,
        description="Exclude PDF-only/contact modules and keep quantitative data points.",
    )
    countries: list[str] = Field(
        default_factory=list,
        description="Optional list of country names (e.g. ['Germany', 'France']) to include.",
    )
    leis: list[str] = Field(
        default_factory=list,
        description="Optional list of entity LEI codes (EntityCode) to include.",
    )
    entity_name_contains: str | None = Field(
        default=None,
        description="Optional case-insensitive substring filter on official entity name.",
    )
    institution_types: list[str] = Field(
        default_factory=list,
        description="Optional institution classifications (e.g. ['Large highest EEA']).",
    )
    modules: list[str] = Field(
        default_factory=list,
        description="Optional Pillar 3 module codes (e.g. ['CODIS', 'ESGDIS', 'FINDIS']).",
    )
    templates: list[str] = Field(
        default_factory=list,
        description="Optional template codes (e.g. ['K_01.00', 'K_41.00', 'K_68.00']).",
    )
    reference_dates: list[str] = Field(
        default_factory=list,
        description="Optional ISO reference dates (YYYY-MM-DD, e.g. ['2025-12-31', '2026-03-31']).",
    )
    reference_date_from: str | None = Field(
        default=None,
        description="Optional inclusive lower bound on reference date (YYYY-MM-DD).",
    )
    reference_date_to: str | None = Field(
        default=None,
        description="Optional inclusive upper bound on reference date (YYYY-MM-DD).",
    )
    limit_submissions: int | None = Field(
        default=None,
        ge=1,
        description="Optional cap on number of submissions to extract in a single run.",
    )


class StorageConfig(BaseModel):
    """Local storage and export settings."""

    backend: Literal["duckdb", "sqlite"] = Field(
        default="duckdb",
        description="Local analytical database backend.",
    )
    db_path: Path = Field(
        default=Path("data/eba_pillar3.duckdb"),
        description="Path to the local DuckDB or SQLite database file.",
    )
    export_dir: Path = Field(
        default=Path("data/exports"),
        description="Directory where Parquet/CSV exports are written.",
    )
    export_formats: list[Literal["parquet", "csv"]] = Field(
        default_factory=lambda: ["parquet"],
        description="Export formats generated after sync or via `export`.",
    )


class ClientConfig(BaseModel):
    """HTTP and Power BI query behavior settings."""

    edap_base_url: str = Field(
        default="https://edap-public.eba.europa.eu",
        description="Base URL of the EBA European Data Access Portal.",
    )
    report_nav_code: str = Field(
        default="MTE2",
        description="Base64 EDAP navigation code for Pillar 3 Data Points Report (116 -> MTE2).",
    )
    page_size: int = Field(
        default=5000,
        ge=100,
        le=30000,
        description="Number of rows requested per Power BI querydata window.",
    )
    timeout_seconds: float = Field(
        default=60.0,
        gt=0,
        description="HTTP request timeout in seconds.",
    )
    max_retries: int = Field(
        default=4,
        ge=0,
        description="Maximum retries for transient network or HTTP 429/5xx failures.",
    )
    request_delay_seconds: float = Field(
        default=0.15,
        ge=0.0,
        description="Polite delay between consecutive Power BI page queries.",
    )


class ExtractorConfig(BaseModel):
    """Top-level project configuration."""

    filters: SyncFilter = Field(default_factory=SyncFilter)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    client: ClientConfig = Field(default_factory=ClientConfig)

    @classmethod
    def from_toml(cls, path: Path) -> ExtractorConfig:
        """Load configuration from a TOML file."""
        with path.open("rb") as f:
            raw = tomllib.load(f)
        return cls.model_validate(raw)
