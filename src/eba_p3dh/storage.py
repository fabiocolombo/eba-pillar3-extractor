"""Normalized DuckDB and SQLite storage backend with Parquet/CSV exports."""

from __future__ import annotations

import csv
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# Column lists for every table (used consistently across DuckDB and SQLite)
TABLE_COLUMNS: dict[str, list[str]] = {
    "dim_modules": [
        "module_vid",
        "module_id",
        "module_code",
        "module_name",
        "module_version_code",
        "module_version_name",
        "framework_id",
        "from_reference_date",
        "to_reference_date",
    ],
    "dim_entities": [
        "entity_key",
        "entity_id",
        "entity_version",
        "lei",
        "entity_name",
        "short_name",
        "country",
        "entity_type",
        "institution_type",
        "authority",
        "is_highest_level_eea",
        "is_listed",
        "is_osii_gsii",
        "is_snci",
        "financial_year_end",
    ],
    "dim_templates": [
        "template_id",
        "template_code",
        "template_name",
        "is_abstract",
        "has_open_rows",
        "has_open_sheets",
    ],
    "dim_tables": [
        "table_vid",
        "table_code",
        "table_name",
        "template_id",
        "has_open_rows",
        "has_open_sheets",
    ],
    "dim_variables": [
        "variable_id",
        "variable_type",
        "main_property",
        "data_type",
    ],
    "dim_submissions": [
        "instance_id",
        "entity_key",
        "entity_id",
        "module_vid",
        "reference_date",
        "is_current",
        "is_accepted",
        "latest_submission",
        "status",
        "reception_date",
        "integration_datetime",
        "submission_name",
        "report_file_link",
    ],
    "fact_datapoints": [
        "instance_id",
        "table_vid",
        "template_id",
        "template_code",
        "table_code",
        "variable_id",
        "cell_id",
        "sheet_code",
        "sheet_label",
        "row_code",
        "row_label",
        "col_code",
        "col_label",
        "table_key_num",
        "unique_key_id",
        "key_descriptor",
        "data_type",
        "currency_code",
        "quant_value_rep",
        "quant_value_eur",
    ],
    "sync_state": [
        "instance_id",
        "reception_date",
        "integration_datetime",
        "row_count",
        "synced_at",
        "status",
        "error_message",
    ],
}

_DDL_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS dim_modules (
        module_vid INTEGER PRIMARY KEY,
        module_id INTEGER,
        module_code TEXT,
        module_name TEXT,
        module_version_code TEXT,
        module_version_name TEXT,
        framework_id INTEGER,
        from_reference_date TEXT,
        to_reference_date TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_entities (
        entity_key TEXT PRIMARY KEY,
        entity_id TEXT,
        entity_version INTEGER,
        lei TEXT,
        entity_name TEXT,
        short_name TEXT,
        country TEXT,
        entity_type TEXT,
        institution_type TEXT,
        authority TEXT,
        is_highest_level_eea TEXT,
        is_listed TEXT,
        is_osii_gsii TEXT,
        is_snci TEXT,
        financial_year_end TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_templates (
        template_id INTEGER PRIMARY KEY,
        template_code TEXT,
        template_name TEXT,
        is_abstract INTEGER,
        has_open_rows INTEGER,
        has_open_sheets INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_tables (
        table_vid INTEGER PRIMARY KEY,
        table_code TEXT,
        table_name TEXT,
        template_id INTEGER,
        has_open_rows INTEGER,
        has_open_sheets INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_variables (
        variable_id INTEGER PRIMARY KEY,
        variable_type TEXT,
        main_property TEXT,
        data_type TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_submissions (
        instance_id BIGINT PRIMARY KEY,
        entity_key TEXT,
        entity_id TEXT,
        module_vid INTEGER,
        reference_date TEXT,
        is_current INTEGER,
        is_accepted INTEGER,
        latest_submission TEXT,
        status TEXT,
        reception_date TEXT,
        integration_datetime TEXT,
        submission_name TEXT,
        report_file_link TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_datapoints (
        instance_id BIGINT NOT NULL,
        table_vid INTEGER,
        template_id INTEGER,
        template_code TEXT,
        table_code TEXT,
        variable_id INTEGER,
        cell_id TEXT,
        sheet_code TEXT,
        sheet_label TEXT,
        row_code TEXT,
        row_label TEXT,
        col_code TEXT,
        col_label TEXT,
        table_key_num INTEGER,
        unique_key_id TEXT,
        key_descriptor TEXT,
        data_type TEXT,
        currency_code TEXT,
        quant_value_rep DOUBLE,
        quant_value_eur DOUBLE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS sync_state (
        instance_id BIGINT PRIMARY KEY,
        reception_date TEXT,
        integration_datetime TEXT,
        row_count INTEGER,
        synced_at TEXT,
        status TEXT,
        error_message TEXT
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fact_instance ON fact_datapoints (instance_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_fact_template ON fact_datapoints (template_code)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_sub_ref_date ON dim_submissions (reference_date)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_ent_lei ON dim_entities (lei)
    """,
)

_VIEW_SQL = """
CREATE VIEW v_pillar3_datapoints AS
SELECT
    s.instance_id,
    s.reference_date,
    s.latest_submission,
    s.status AS submission_status,
    s.reception_date,
    e.lei,
    e.entity_name,
    e.short_name AS entity_short_name,
    e.country,
    e.entity_type,
    e.institution_type,
    m.module_code,
    m.module_name,
    m.framework_id,
    f.template_id,
    COALESCE(f.template_code, tp.template_code) AS template_code,
    tp.template_name,
    f.table_vid,
    COALESCE(f.table_code, tb.table_code) AS table_code,
    tb.table_name,
    f.sheet_code,
    f.sheet_label,
    f.row_code,
    f.row_label,
    f.col_code,
    f.col_label,
    f.table_key_num,
    f.unique_key_id,
    f.key_descriptor,
    f.cell_id,
    f.variable_id,
    v.main_property,
    COALESCE(f.data_type, v.data_type) AS data_type,
    f.currency_code,
    f.quant_value_rep,
    f.quant_value_eur
FROM fact_datapoints f
LEFT JOIN dim_submissions s ON f.instance_id = s.instance_id
LEFT JOIN dim_entities e ON s.entity_key = e.entity_key
LEFT JOIN dim_modules m ON s.module_vid = m.module_vid
LEFT JOIN dim_templates tp ON f.template_id = tp.template_id
LEFT JOIN dim_tables tb ON f.table_vid = tb.table_vid
LEFT JOIN dim_variables v ON f.variable_id = v.variable_id
"""


class Pillar3Store:
    """Unified local analytical database (DuckDB or SQLite) for Pillar 3 data."""

    def __init__(self, db_path: Path, backend: Literal["duckdb", "sqlite"] = "duckdb") -> None:
        self.db_path = Path(db_path)
        self.backend = backend
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        if backend == "duckdb":
            self._conn: Any = duckdb.connect(str(self.db_path))
        elif backend == "sqlite":
            self._conn = sqlite3.connect(str(self.db_path))
        else:
            raise ValueError(f"Unsupported backend: {backend}")
        self._init_schema()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> Pillar3Store:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _init_schema(self) -> None:
        for stmt in _DDL_STATEMENTS:
            self._conn.execute(stmt)
        self._conn.execute("DROP VIEW IF EXISTS v_pillar3_datapoints")
        self._conn.execute(_VIEW_SQL)
        if self.backend == "sqlite":
            self._conn.commit()

    def upsert_dimension(self, table: str, rows: list[dict[str, Any]]) -> int:
        """Upsert rows into a dimension table by primary key."""
        if not rows:
            return 0
        cols = TABLE_COLUMNS[table]
        col_list = ", ".join(cols)
        placeholders = ", ".join(["?"] * len(cols))

        if self.backend == "duckdb":
            arrow_tbl = _rows_to_arrow(rows, cols)
            self._conn.register("_staging_dim", arrow_tbl)
            try:
                self._conn.execute(
                    f"INSERT OR REPLACE INTO {table} ({col_list}) SELECT {col_list} FROM _staging_dim"
                )
            finally:
                self._conn.unregister("_staging_dim")
        else:
            tuples = [tuple(r.get(c) for c in cols) for r in rows]
            self._conn.executemany(
                f"INSERT OR REPLACE INTO {table} ({col_list}) VALUES ({placeholders})",
                tuples,
            )
            self._conn.commit()
        return len(rows)

    def get_sync_state(self) -> dict[int, dict[str, Any]]:
        """Return mapping of instance_id -> sync_state record for successfully synced rows."""
        cur = self._conn.execute(
            "SELECT instance_id, reception_date, integration_datetime, row_count, synced_at, status, error_message "
            "FROM sync_state"
        )
        cols = [d[0] for d in cur.description]
        return {int(row[0]): dict(zip(cols, row, strict=False)) for row in cur.fetchall()}

    def replace_submission_datapoints(
        self,
        instance_id: int,
        rows: list[dict[str, Any]],
        *,
        reception_date: str | None,
        integration_datetime: str | None,
    ) -> int:
        """Atomically replace all fact rows for a submission and mark sync_state as synced."""
        cols = TABLE_COLUMNS["fact_datapoints"]
        col_list = ", ".join(cols)
        synced_at = datetime.now(tz=UTC).isoformat()

        if self.backend == "duckdb":
            self._conn.execute("BEGIN TRANSACTION")
            try:
                self._conn.execute("DELETE FROM fact_datapoints WHERE instance_id = ?", [instance_id])
                if rows:
                    arrow_tbl = _rows_to_arrow(rows, cols)
                    self._conn.register("_staging_facts", arrow_tbl)
                    try:
                        self._conn.execute(
                            f"INSERT INTO fact_datapoints ({col_list}) SELECT {col_list} FROM _staging_facts"
                        )
                    finally:
                        self._conn.unregister("_staging_facts")
                self._conn.execute(
                    "INSERT OR REPLACE INTO sync_state "
                    "(instance_id, reception_date, integration_datetime, row_count, synced_at, status, error_message) "
                    "VALUES (?, ?, ?, ?, ?, 'synced', NULL)",
                    [instance_id, reception_date, integration_datetime, len(rows), synced_at],
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        else:
            placeholders = ", ".join(["?"] * len(cols))
            with self._conn:
                self._conn.execute("DELETE FROM fact_datapoints WHERE instance_id = ?", (instance_id,))
                if rows:
                    tuples = [tuple(r.get(c) for c in cols) for r in rows]
                    self._conn.executemany(
                        f"INSERT INTO fact_datapoints ({col_list}) VALUES ({placeholders})",
                        tuples,
                    )
                self._conn.execute(
                    "INSERT OR REPLACE INTO sync_state "
                    "(instance_id, reception_date, integration_datetime, row_count, synced_at, status, error_message) "
                    "VALUES (?, ?, ?, ?, ?, 'synced', NULL)",
                    (instance_id, reception_date, integration_datetime, len(rows), synced_at),
                )
        return len(rows)

    def record_sync_error(
        self,
        instance_id: int,
        error_message: str,
        *,
        reception_date: str | None,
        integration_datetime: str | None,
    ) -> None:
        """Record a per-submission sync error in sync_state so subsequent runs can retry it."""
        synced_at = datetime.now(tz=UTC).isoformat()
        params = [instance_id, reception_date, integration_datetime, 0, synced_at, "error", error_message[:1000]]
        self._conn.execute(
            "INSERT OR REPLACE INTO sync_state "
            "(instance_id, reception_date, integration_datetime, row_count, synced_at, status, error_message) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            params,
        )
        if self.backend == "sqlite":
            self._conn.commit()

    def query_sql(self, sql: str, params: list[Any] | None = None) -> tuple[list[str], list[tuple[Any, ...]]]:
        """Execute a SQL query and return (column_names, rows)."""
        cur = self._conn.execute(sql, params or [])
        cols = [d[0] for d in (cur.description or [])]
        return cols, cur.fetchall()

    def get_status_summary(self) -> dict[str, Any]:
        """Return high-level counts and coverage metrics for the local database."""
        def _scalar(sql: str) -> Any:
            _, rows = self.query_sql(sql)
            return rows[0][0] if rows else 0

        _, ref_dates = self.query_sql(
            "SELECT DISTINCT s.reference_date FROM fact_datapoints f "
            "JOIN dim_submissions s ON f.instance_id = s.instance_id "
            "WHERE s.reference_date IS NOT NULL ORDER BY s.reference_date"
        )
        _, modules = self.query_sql(
            "SELECT m.module_code, COUNT(DISTINCT f.instance_id) AS submissions, COUNT(*) AS datapoints "
            "FROM fact_datapoints f "
            "JOIN dim_submissions s ON f.instance_id = s.instance_id "
            "JOIN dim_modules m ON s.module_vid = m.module_vid "
            "GROUP BY m.module_code ORDER BY datapoints DESC"
        )
        return {
            "backend": self.backend,
            "db_path": str(self.db_path),
            "db_size_bytes": self.db_path.stat().st_size if self.db_path.exists() else 0,
            "catalog_submissions": _scalar("SELECT COUNT(*) FROM dim_submissions"),
            "catalog_entities": _scalar("SELECT COUNT(DISTINCT lei) FROM dim_entities"),
            "synced_submissions": _scalar("SELECT COUNT(*) FROM sync_state WHERE status = 'synced'"),
            "failed_submissions": _scalar("SELECT COUNT(*) FROM sync_state WHERE status = 'error'"),
            "total_datapoints": _scalar("SELECT COUNT(*) FROM fact_datapoints"),
            "synced_entities": _scalar(
                "SELECT COUNT(DISTINCT e.lei) FROM fact_datapoints f "
                "JOIN dim_submissions s ON f.instance_id = s.instance_id "
                "JOIN dim_entities e ON s.entity_key = e.entity_key"
            ),
            "reference_dates": [r[0] for r in ref_dates],
            "module_breakdown": [
                {"module_code": r[0], "submissions": r[1], "datapoints": r[2]} for r in modules
            ],
        }

    def export_datasets(
        self,
        export_dir: Path,
        formats: list[Literal["parquet", "csv"]],
        *,
        layout: Literal["flat", "star"] = "flat",
    ) -> list[Path]:
        """Export extracted datasets to Parquet and/or CSV."""
        export_dir = Path(export_dir)
        export_dir.mkdir(parents=True, exist_ok=True)
        written: list[Path] = []

        if layout == "flat":
            targets = [("pillar3_datapoints", "SELECT * FROM v_pillar3_datapoints")]
        else:
            targets = [
                (tbl, f"SELECT * FROM {tbl}")
                for tbl in (
                    "dim_entities",
                    "dim_modules",
                    "dim_templates",
                    "dim_tables",
                    "dim_variables",
                    "dim_submissions",
                    "fact_datapoints",
                )
            ]

        for name, sql in targets:
            if "parquet" in formats:
                out_pq = export_dir / f"{name}.parquet"
                self._export_parquet(sql, out_pq)
                written.append(out_pq)
            if "csv" in formats:
                out_csv = export_dir / f"{name}.csv"
                self._export_csv(sql, out_csv)
                written.append(out_csv)

        return written

    def _export_parquet(self, sql: str, out_path: Path) -> None:
        if self.backend == "duckdb":
            escaped = str(out_path).replace("'", "''")
            self._conn.execute(f"COPY ({sql}) TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        else:
            cols, rows = self.query_sql(sql)
            columns_dict = {col: [r[i] for r in rows] for i, col in enumerate(cols)}
            table = pa.table(columns_dict) if cols else pa.table({})
            pq.write_table(table, out_path, compression="zstd")

    def _export_csv(self, sql: str, out_path: Path) -> None:
        if self.backend == "duckdb":
            escaped = str(out_path).replace("'", "''")
            self._conn.execute(f"COPY ({sql}) TO '{escaped}' (FORMAT CSV, HEADER true)")
        else:
            cols, rows = self.query_sql(sql)
            with out_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(cols)
                writer.writerows(rows)


def _rows_to_arrow(rows: list[dict[str, Any]], cols: list[str]) -> pa.Table:
    """Convert a list of row dicts into a PyArrow Table with stable column ordering."""
    data = {col: [r.get(col) for r in rows] for col in cols}
    return pa.table(data)
