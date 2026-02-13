"""Base scraper with rate limiting, retry, and raw data storage."""

import json
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path

import requests
from rich.console import Console

PROJECT_ROOT = Path(__file__).resolve().parent.parent
console = Console()
logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """Base class for all data scrapers."""

    def __init__(self, config: dict, data_dir: Path = None):
        self.config = config
        self.data_dir = data_dir or PROJECT_ROOT / "data" / "raw"
        self.rate_limit = config.get("rate_limit_seconds", 1)
        self.max_retries = 3
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "CMMC-Data-Pipeline/1.0 (compliance research)"
        })
        self._last_request_time = 0
        self.scrape_metadata = {
            "source": self.get_source_name(),
            "started_at": None,
            "completed_at": None,
            "records_fetched": 0,
            "errors": [],
            "urls_accessed": [],
        }

    @abstractmethod
    def get_source_name(self) -> str:
        """Return identifier for this source (e.g., 'nist_csrc')."""

    @abstractmethod
    def scrape(self) -> list[dict]:
        """Scrape all available data. Returns list of raw records."""

    @abstractmethod
    def scrape_incremental(self, since_date: str) -> list[dict]:
        """Scrape only data updated since the given date (YYYY-MM-DD)."""

    def _rate_limit_wait(self):
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self._last_request_time = time.time()

    def _request(self, url: str, params: dict = None) -> requests.Response:
        """Make an HTTP request with retry and rate limiting."""
        self._rate_limit_wait()
        self.scrape_metadata["urls_accessed"].append(url)

        for attempt in range(self.max_retries):
            try:
                resp = self.session.get(url, params=params, timeout=30)
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                wait = 2 ** (attempt + 1)
                logger.warning(
                    "Request failed (attempt %d/%d): %s — retrying in %ds",
                    attempt + 1, self.max_retries, e, wait
                )
                if attempt < self.max_retries - 1:
                    time.sleep(wait)
                else:
                    self.scrape_metadata["errors"].append(str(e))
                    raise

    def save_raw(self, records: list[dict]) -> Path:
        """Save raw records to data/raw/{source}/{date}/records.json."""
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        out_dir = self.data_dir / self.get_source_name() / date_str
        out_dir.mkdir(parents=True, exist_ok=True)

        out_file = out_dir / "records.json"
        with open(out_file, "w") as f:
            json.dump(records, f, indent=2, default=str)

        # Save metadata
        self.scrape_metadata["records_fetched"] = len(records)
        self.scrape_metadata["completed_at"] = datetime.now(timezone.utc).isoformat()
        meta_file = out_dir / "metadata.json"
        with open(meta_file, "w") as f:
            json.dump(self.scrape_metadata, f, indent=2, default=str)

        console.print(
            f"[green]Saved {len(records)} records to {out_file}[/green]"
        )
        return out_file

    def run(self, incremental_since: str = None) -> list[dict]:
        """Run the scraper (full or incremental) and save results."""
        self.scrape_metadata["started_at"] = datetime.now(timezone.utc).isoformat()
        console.print(f"[bold]Scraping {self.get_source_name()}...[/bold]")

        if incremental_since:
            console.print(f"  Incremental since: {incremental_since}")
            records = self.scrape_incremental(incremental_since)
        else:
            console.print("  Full scrape")
            records = self.scrape()

        if records:
            self.save_raw(records)
        else:
            console.print("[yellow]No new records found.[/yellow]")

        return records
