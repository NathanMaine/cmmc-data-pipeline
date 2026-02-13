"""CLI: Run scrapers only."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import click
from rich.console import Console

from pipeline.runner import load_config, PROJECT_ROOT
from scrapers.nist_csrc import NISTCSRCScraper
from scrapers.federal_register import FederalRegisterScraper
from scrapers.ecfr import ECFRScraper

console = Console()


@click.command()
@click.option("--source", "-s", multiple=True, help="Source to scrape (nist_csrc, federal_register, ecfr)")
@click.option("--since", help="Incremental since date (YYYY-MM-DD)")
@click.option("--config", "config_path", help="Path to config.yaml")
def main(source, since, config_path):
    """Run scrapers and save raw data."""
    config = load_config(config_path)

    sources = list(source) if source else ["nist_csrc", "federal_register", "ecfr"]

    scraper_map = {
        "nist_csrc": (NISTCSRCScraper, config.get("scrapers", {}).get("nist_csrc", {})),
        "federal_register": (FederalRegisterScraper, config.get("scrapers", {}).get("federal_register", {})),
        "ecfr": (ECFRScraper, config.get("scrapers", {}).get("ecfr", {})),
    }

    for name in sources:
        if name not in scraper_map:
            console.print(f"[red]Unknown source: {name}[/red]")
            continue

        cls, cfg = scraper_map[name]
        scraper = cls(cfg, data_dir=PROJECT_ROOT / "data" / "raw")
        scraper.run(incremental_since=since)


if __name__ == "__main__":
    main()
