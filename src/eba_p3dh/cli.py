"""Command-line interface for the EBA Pillar 3 Data Hub (EDAP) extractor."""

from __future__ import annotations

import csv
import json
import sys
from collections import Counter
from enum import Enum
from pathlib import Path
from typing import Annotated, Literal


class ExportFormat(str, Enum):
    parquet = "parquet"
    csv = "csv"

import typer
from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

from eba_p3dh.client import EDAPClient
from eba_p3dh.config import ExtractorConfig
from eba_p3dh.pipeline import Pillar3Pipeline, filter_submissions
from eba_p3dh.storage import Pillar3Store

app = typer.Typer(
    name="eba-p3dh",
    help="Extract normalized quantitative disclosures from the EBA Pillar 3 Data Hub (EDAP).",
    no_args_is_help=True,
)
console = Console()


def _load_config(
    config_path: Path | None,
    *,
    backend: Literal["duckdb", "sqlite"] | None = None,
    db_path: Path | None = None,
    export_dir: Path | None = None,
) -> ExtractorConfig:
    cfg = ExtractorConfig.from_toml(config_path) if config_path else ExtractorConfig()
    if backend is not None:
        cfg.storage.backend = backend
        if db_path is None and config_path is None:
            ext = "duckdb" if backend == "duckdb" else "sqlite"
            cfg.storage.db_path = Path(f"data/eba_pillar3.{ext}")
    if db_path is not None:
        cfg.storage.db_path = db_path
    if export_dir is not None:
        cfg.storage.export_dir = export_dir
    return cfg


@app.command()
def catalog(
    config: Annotated[Path | None, typer.Option("--config", "-c", help="Path to TOML config file.")] = None,
    country: Annotated[list[str] | None, typer.Option("--country", help="Filter by country (repeatable).")] = None,
    lei: Annotated[list[str] | None, typer.Option("--lei", help="Filter by LEI code (repeatable).")] = None,
    entity: Annotated[str | None, typer.Option("--entity", help="Substring match on entity name.")] = None,
    module: Annotated[list[str] | None, typer.Option("--module", help="Filter by module code, e.g. CODIS, ESGDIS.")] = None,
    ref_date: Annotated[list[str] | None, typer.Option("--ref-date", help="Filter by reference date (YYYY-MM-DD).")] = None,
    all_submissions: Annotated[bool, typer.Option("--all-submissions", help="Include non-latest submissions.")] = False,
    show_sample: Annotated[int, typer.Option("--sample", help="Number of sample submissions to print.")] = 10,
) -> None:
    """Inspect live EBA Pillar 3 Data Hub catalog counts, modules, countries, and reference dates."""
    cfg = _load_config(config)
    if country:
        cfg.filters.countries = country
    if lei:
        cfg.filters.leis = lei
    if entity:
        cfg.filters.entity_name_contains = entity
    if module:
        cfg.filters.modules = module
    if ref_date:
        cfg.filters.reference_dates = ref_date
    if all_submissions:
        cfg.filters.latest_only = False

    with console.status("[bold cyan]Fetching live Pillar 3 catalog from EDAP..."):
        with EDAPClient(cfg.client) as client:
            last_refresh = client.fetch_last_refresh_date()
            modules = client.fetch_modules()
            entities = client.fetch_entities()
            submissions = client.fetch_submissions(
                accepted_only=cfg.filters.accepted_only,
                latest_only=cfg.filters.latest_only,
            )

    entities_by_key = {e["entity_key"]: e for e in entities}
    modules_by_vid = {int(m["module_vid"]): m for m in modules}
    matched = filter_submissions(
        submissions,
        entities_by_key=entities_by_key,
        modules_by_vid=modules_by_vid,
        filters=cfg.filters,
    )

    console.print(f"[bold]EDAP Last Refreshed:[/bold] {last_refresh.isoformat() if last_refresh else 'unknown'}")
    console.print(
        f"[bold]Catalog Submissions:[/bold] {len(submissions):,}  |  "
        f"[bold]Matched Filters:[/bold] [green]{len(matched):,}[/green]"
    )

    mod_counts = Counter(s["_module_code"] for s in matched)
    date_counts = Counter(s["reference_date"] for s in matched)
    country_counts = Counter(s["_country"] or "Unknown" for s in matched)

    mod_tbl = Table(title="Matched Submissions by Module")
    mod_tbl.add_column("Module", style="cyan")
    mod_tbl.add_column("Submissions", justify="right", style="green")
    for mod_code, cnt in mod_counts.most_common():
        mod_tbl.add_row(mod_code, f"{cnt:,}")
    console.print(mod_tbl)

    date_tbl = Table(title="Matched Submissions by Reference Date")
    date_tbl.add_column("Reference Date", style="cyan")
    date_tbl.add_column("Submissions", justify="right", style="green")
    for rd, cnt in sorted(date_counts.items(), reverse=True):
        date_tbl.add_row(str(rd), f"{cnt:,}")
    console.print(date_tbl)

    ctry_tbl = Table(title="Top Countries (Matched Submissions)")
    ctry_tbl.add_column("Country", style="cyan")
    ctry_tbl.add_column("Submissions", justify="right", style="green")
    for ctry, cnt in country_counts.most_common(15):
        ctry_tbl.add_row(ctry, f"{cnt:,}")
    console.print(ctry_tbl)

    if show_sample > 0 and matched:
        sample_tbl = Table(title=f"Sample Matched Submissions (first {min(show_sample, len(matched))})")
        sample_tbl.add_column("InstanceID", style="dim")
        sample_tbl.add_column("Ref Date", style="cyan")
        sample_tbl.add_column("Module", style="magenta")
        sample_tbl.add_column("Country")
        sample_tbl.add_column("LEI", style="dim")
        sample_tbl.add_column("Entity Name")
        for sub in matched[:show_sample]:
            sample_tbl.add_row(
                str(sub["instance_id"]),
                str(sub["reference_date"] or ""),
                str(sub["_module_code"] or ""),
                str(sub["_country"] or ""),
                str(sub["_lei"] or ""),
                str(sub["_entity_name"] or "")[:55],
            )
        console.print(sample_tbl)


@app.command()
def sync(
    config: Annotated[Path | None, typer.Option("--config", "-c", help="Path to TOML config file.")] = None,
    backend: Annotated[Literal["duckdb", "sqlite"] | None, typer.Option("--backend", help="Database backend.")] = None,
    db: Annotated[Path | None, typer.Option("--db", help="Path to local DuckDB or SQLite database.")] = None,
    country: Annotated[list[str] | None, typer.Option("--country", help="Filter by country (repeatable).")] = None,
    lei: Annotated[list[str] | None, typer.Option("--lei", help="Filter by LEI code (repeatable).")] = None,
    entity: Annotated[str | None, typer.Option("--entity", help="Substring match on entity name.")] = None,
    institution_type: Annotated[list[str] | None, typer.Option("--institution-type", help="Filter by institution type.")] = None,
    module: Annotated[list[str] | None, typer.Option("--module", help="Filter by module code, e.g. CODIS, ESGDIS.")] = None,
    template: Annotated[list[str] | None, typer.Option("--template", help="Filter by template code, e.g. K_01.00.")] = None,
    ref_date: Annotated[list[str] | None, typer.Option("--ref-date", help="Filter by reference date (YYYY-MM-DD).")] = None,
    from_date: Annotated[str | None, typer.Option("--from-date", help="Inclusive lower bound on reference date.")] = None,
    to_date: Annotated[str | None, typer.Option("--to-date", help="Inclusive upper bound on reference date.")] = None,
    all_submissions: Annotated[bool, typer.Option("--all-submissions", help="Include superseded/non-latest submissions.")] = False,
    limit: Annotated[int | None, typer.Option("--limit", help="Max submissions to extract in this run.")] = None,
    force: Annotated[bool, typer.Option("--force", help="Re-extract submissions even if already synced.")] = False,
    export: Annotated[bool, typer.Option("--export", help="Export Parquet/CSV files after sync.")] = False,
    export_format: Annotated[list[ExportFormat] | None, typer.Option("--format", help="Export format(s).")] = None,
    export_dir: Annotated[Path | None, typer.Option("--export-dir", help="Export output directory.")] = None,
    layout: Annotated[Literal["flat", "star"], typer.Option("--layout", help="Export layout: flat or star.")] = "flat",
) -> None:
    """Incrementally extract normalized quantitative Pillar 3 data points into local storage."""
    cfg = _load_config(config, backend=backend, db_path=db, export_dir=export_dir)
    if country:
        cfg.filters.countries = country
    if lei:
        cfg.filters.leis = lei
    if entity:
        cfg.filters.entity_name_contains = entity
    if institution_type:
        cfg.filters.institution_types = institution_type
    if module:
        cfg.filters.modules = module
    if template:
        cfg.filters.templates = template
    if ref_date:
        cfg.filters.reference_dates = ref_date
    if from_date:
        cfg.filters.reference_date_from = from_date
    if to_date:
        cfg.filters.reference_date_to = to_date
    if all_submissions:
        cfg.filters.latest_only = False
    if limit is not None:
        cfg.filters.limit_submissions = limit
    if export_format:
        cfg.storage.export_formats = [f.value for f in export_format]  # type: ignore[misc]
        export = True

    with EDAPClient(cfg.client) as client, Pillar3Store(cfg.storage.db_path, cfg.storage.backend) as store:
        pipeline = Pillar3Pipeline(cfg, client, store)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task_id = progress.add_task("[cyan]Refreshing EDAP catalog...", total=None)

            def on_progress(
                idx: int,
                total: int,
                sub: dict,
                written: int | None,
                err: str | None,
            ) -> None:
                if progress.tasks[0].total != total:
                    progress.update(task_id, total=total)
                label = f"{sub.get('_module_code')} | {str(sub.get('_entity_name'))[:32]} ({sub.get('reference_date')})"
                if err:
                    progress.console.print(f"[red]Failed InstanceID={sub['instance_id']}:[/red] {err}")
                progress.update(task_id, completed=idx, description=f"[green]{label}[/green]")

            summary = pipeline.run_sync(
                force=force,
                export=export,
                export_layout=layout,
                progress_cb=on_progress,
            )

    tbl = Table(title="Sync Summary")
    tbl.add_column("Metric", style="cyan")
    tbl.add_column("Value", justify="right", style="bold")
    tbl.add_row("Database", f"{cfg.storage.backend}://{cfg.storage.db_path}")
    tbl.add_row("Catalog Submissions", f"{summary.catalog_submissions:,}")
    tbl.add_row("Matched Filters", f"{summary.matched_submissions:,}")
    tbl.add_row("Skipped (Already Synced)", f"{summary.skipped_already_synced:,}")
    tbl.add_row("Extracted Submissions", f"[green]{summary.extracted_submissions:,}[/green]")
    tbl.add_row("Failed Submissions", f"[red]{summary.failed_submissions:,}[/red]" if summary.failed_submissions else "0")
    tbl.add_row("Datapoints Written", f"[green]{summary.total_datapoints_written:,}[/green]")
    console.print(tbl)

    if summary.exported_files:
        console.print("[bold]Exported files:[/bold]")
        for p in summary.exported_files:
            console.print(f"  - {p}")


@app.command()
def status(
    config: Annotated[Path | None, typer.Option("--config", "-c", help="Path to TOML config file.")] = None,
    backend: Annotated[Literal["duckdb", "sqlite"] | None, typer.Option("--backend", help="Database backend.")] = None,
    db: Annotated[Path | None, typer.Option("--db", help="Path to local DuckDB or SQLite database.")] = None,
) -> None:
    """Show local database coverage, sync counts, and module breakdown."""
    cfg = _load_config(config, backend=backend, db_path=db)
    if not cfg.storage.db_path.exists():
        console.print(f"[yellow]Database not found at {cfg.storage.db_path}. Run `eba-p3dh sync` first.[/yellow]")
        raise typer.Exit(code=1)

    with Pillar3Store(cfg.storage.db_path, cfg.storage.backend) as store:
        info = store.get_status_summary()

    tbl = Table(title="Local EBA Pillar 3 Database Status")
    tbl.add_column("Metric", style="cyan")
    tbl.add_column("Value", justify="right", style="bold")
    tbl.add_row("Backend", info["backend"])
    tbl.add_row("Database Path", info["db_path"])
    tbl.add_row("Database Size", f"{info['db_size_bytes'] / (1024 * 1024):.2f} MB")
    tbl.add_row("Catalog Submissions", f"{info['catalog_submissions']:,}")
    tbl.add_row("Catalog Institutions (LEIs)", f"{info['catalog_entities']:,}")
    tbl.add_row("Synced Submissions", f"[green]{info['synced_submissions']:,}[/green]")
    tbl.add_row("Failed Submissions", f"[red]{info['failed_submissions']:,}[/red]" if info["failed_submissions"] else "0")
    tbl.add_row("Synced Institutions (LEIs)", f"{info['synced_entities']:,}")
    tbl.add_row("Total Quantitative Datapoints", f"[green]{info['total_datapoints']:,}[/green]")
    tbl.add_row("Reference Dates", ", ".join(info["reference_dates"]) or "-")
    console.print(tbl)

    if info["module_breakdown"]:
        mtbl = Table(title="Synced Datapoints by Module")
        mtbl.add_column("Module", style="cyan")
        mtbl.add_column("Submissions", justify="right")
        mtbl.add_column("Datapoints", justify="right", style="green")
        for row in info["module_breakdown"]:
            mtbl.add_row(str(row["module_code"]), f"{row['submissions']:,}", f"{row['datapoints']:,}")
        console.print(mtbl)


@app.command()
def export(
    config: Annotated[Path | None, typer.Option("--config", "-c", help="Path to TOML config file.")] = None,
    backend: Annotated[Literal["duckdb", "sqlite"] | None, typer.Option("--backend", help="Database backend.")] = None,
    db: Annotated[Path | None, typer.Option("--db", help="Path to local DuckDB or SQLite database.")] = None,
    export_dir: Annotated[Path | None, typer.Option("--export-dir", "-o", help="Export output directory.")] = None,
    fmt: Annotated[list[ExportFormat] | None, typer.Option("--format", "-f", help="Export format(s).")] = None,
    layout: Annotated[Literal["flat", "star"], typer.Option("--layout", help="Export flat joined view or normalized star schema tables.")] = "flat",
) -> None:
    """Export extracted Pillar 3 data from local storage to Parquet and/or CSV."""
    cfg = _load_config(config, backend=backend, db_path=db, export_dir=export_dir)
    if fmt:
        cfg.storage.export_formats = [f.value for f in fmt]  # type: ignore[misc]
    if not cfg.storage.db_path.exists():
        console.print(f"[yellow]Database not found at {cfg.storage.db_path}. Run `eba-p3dh sync` first.[/yellow]")
        raise typer.Exit(code=1)

    with Pillar3Store(cfg.storage.db_path, cfg.storage.backend) as store:
        files = store.export_datasets(cfg.storage.export_dir, cfg.storage.export_formats, layout=layout)

    console.print(f"[green]Exported {len(files)} file(s) ({layout} layout):[/green]")
    for f in files:
        size_kb = f.stat().st_size / 1024
        console.print(f"  - {f} ({size_kb:.1f} KB)")


@app.command()
def query(
    sql: Annotated[str, typer.Argument(help="SQL query to run against the local database (e.g. SELECT * FROM v_pillar3_datapoints LIMIT 10).")],
    config: Annotated[Path | None, typer.Option("--config", "-c", help="Path to TOML config file.")] = None,
    backend: Annotated[Literal["duckdb", "sqlite"] | None, typer.Option("--backend", help="Database backend.")] = None,
    db: Annotated[Path | None, typer.Option("--db", help="Path to local DuckDB or SQLite database.")] = None,
    output: Annotated[Literal["table", "csv", "json"], typer.Option("--output", "-o", help="Output format.")] = "table",
) -> None:
    """Run an ad-hoc SQL query against the local Pillar 3 database."""
    cfg = _load_config(config, backend=backend, db_path=db)
    if not cfg.storage.db_path.exists():
        console.print(f"[yellow]Database not found at {cfg.storage.db_path}. Run `eba-p3dh sync` first.[/yellow]")
        raise typer.Exit(code=1)

    with Pillar3Store(cfg.storage.db_path, cfg.storage.backend) as store:
        cols, rows = store.query_sql(sql)

    if output == "json":
        payload = [dict(zip(cols, row, strict=False)) for row in rows]
        typer.echo(json.dumps(payload, indent=2, default=str))
    elif output == "csv":
        writer = csv.writer(sys.stdout)
        writer.writerow(cols)
        writer.writerows(rows)
    else:
        tbl = Table(show_lines=False)
        for col in cols:
            tbl.add_column(col, style="cyan")
        for row in rows:
            tbl.add_row(*["" if v is None else str(v) for v in row])
        console.print(tbl)
        console.print(f"[dim]{len(rows):,} row(s)[/dim]")


if __name__ == "__main__":
    app()
