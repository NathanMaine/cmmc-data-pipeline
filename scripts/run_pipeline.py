"""CLI: Run the full pipeline end-to-end."""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import click
from rich.console import Console
from rich.logging import RichHandler

from pipeline.runner import run_pipeline, load_config

console = Console()


@click.command()
@click.option("--since", help="Incremental since date (YYYY-MM-DD)")
@click.option("--source", "-s", multiple=True, help="Sources to scrape")
@click.option("--skip-validation", is_flag=True, help="Skip validation checks")
@click.option("--auto-merge", is_flag=True, help="Automatically merge into training data")
@click.option("--dry-run", is_flag=True, help="Run without creating snapshots")
@click.option("--skip-scrape", is_flag=True, help="Use existing raw data from disk instead of re-scraping")
@click.option("--config", "config_path", help="Path to config.yaml")
@click.option("--verbose", "-v", is_flag=True, help="Verbose logging")
def main(since, source, skip_validation, auto_merge, dry_run, skip_scrape, config_path, verbose):
    """Run the full CMMC data update pipeline."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(message)s",
        handlers=[RichHandler(rich_tracebacks=True)],
    )

    config = load_config(config_path)
    sources = list(source) if source else None

    result = run_pipeline(
        config=config,
        incremental_since=since,
        sources=sources,
        skip_validation=skip_validation,
        auto_merge=auto_merge,
        dry_run=dry_run,
        skip_scrape=skip_scrape,
    )

    if result and not result.passed:
        console.print("\n[red]Pipeline completed with validation failures[/red]")
        sys.exit(1)
    else:
        console.print("\n[green]Pipeline completed successfully[/green]")


if __name__ == "__main__":
    main()
