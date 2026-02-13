"""Main pipeline runner that orchestrates scrape → process → validate → version → merge."""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml
from rich.console import Console

from scrapers.nist_csrc import NISTCSRCScraper
from scrapers.federal_register import FederalRegisterScraper
from scrapers.ecfr import ECFRScraper
from scrapers.nist_sp800_171 import NISTSP800171Scraper
from scrapers.nist_csf import NISTCSFScraper
from scrapers.dod_documents import DoDDocumentScraper
from processors.converter import convert_batch
from processors.quality_filter import FilterConfig, filter_batch
from processors.relevance_filter import filter_relevance
from processors.dedup import DedupIndex
from pipeline.versioning import VersionManager
from pipeline.validator import DataValidator

console = Console()
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def load_config(config_path: str = None) -> dict:
    path = Path(config_path) if config_path else PROJECT_ROOT / "config.yaml"
    with open(path) as f:
        return yaml.safe_load(f)


def _load_raw_from_disk(source_name: str, data_dir: Path) -> list[dict]:
    """Load the most recent raw records from disk for a source."""
    source_dir = data_dir / source_name
    if not source_dir.exists():
        return []
    # Find most recent date directory
    date_dirs = sorted(source_dir.iterdir(), reverse=True)
    for d in date_dirs:
        records_file = d / "records.json"
        if records_file.exists():
            with open(records_file) as f:
                records = json.load(f)
            console.print(f"  {source_name}: loaded {len(records)} records from {d.name}")
            return records
    return []


def run_pipeline(
    config: dict = None,
    incremental_since: str = None,
    sources: list[str] = None,
    skip_validation: bool = False,
    auto_merge: bool = False,
    dry_run: bool = False,
    skip_scrape: bool = False,
):
    """Run the full data update pipeline.

    Steps:
    1. Scrape configured sources
    2. Convert to chat training format
    3. Quality filter
    4. Deduplicate against existing training data
    5. Validate
    6. Create versioned snapshot
    7. Optionally merge into training data
    """
    if config is None:
        config = load_config()

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    console.print(f"[bold]Pipeline run {run_id}[/bold]")

    # Determine which sources to scrape
    all_source_names = [
        "nist_csrc", "federal_register", "ecfr",
        "nist_sp800_171", "nist_csf", "dod_documents",
    ]
    enabled_sources = sources or [
        s for s in all_source_names
        if config.get("scrapers", {}).get(s, {}).get("enabled", True)
    ]

    # Step 1: Scrape (or load from disk)
    all_raw = {}
    raw_dir = PROJECT_ROOT / "data" / "raw"

    if skip_scrape:
        console.print("\n[bold cyan]Step 1: Loading raw data from disk (skip-scrape)...[/bold cyan]")
        for source_name in enabled_sources:
            raw = _load_raw_from_disk(source_name, raw_dir)
            if raw:
                all_raw[source_name] = raw
            else:
                console.print(f"  [yellow]{source_name}: no raw data found on disk[/yellow]")
    else:
        console.print("\n[bold cyan]Step 1: Scraping sources...[/bold cyan]")
        scraper_map = {
            "nist_csrc": (NISTCSRCScraper, config.get("scrapers", {}).get("nist_csrc", {})),
            "federal_register": (FederalRegisterScraper, config.get("scrapers", {}).get("federal_register", {})),
            "ecfr": (ECFRScraper, config.get("scrapers", {}).get("ecfr", {})),
            "nist_sp800_171": (NISTSP800171Scraper, config.get("scrapers", {}).get("nist_sp800_171", {})),
            "nist_csf": (NISTCSFScraper, config.get("scrapers", {}).get("nist_csf", {})),
            "dod_documents": (DoDDocumentScraper, config.get("scrapers", {}).get("dod_documents", {})),
        }

        for source_name in enabled_sources:
            if source_name not in scraper_map:
                console.print(f"[yellow]Unknown source: {source_name}[/yellow]")
                continue

            scraper_cls, scraper_config = scraper_map[source_name]
            scraper = scraper_cls(scraper_config, data_dir=raw_dir)

            if incremental_since:
                raw = scraper.run(incremental_since=incremental_since)
            else:
                raw = scraper.run()

            all_raw[source_name] = raw
            console.print(f"  {source_name}: {len(raw)} raw records")

    # Step 1b: Relevance filter (pre-conversion, on raw records)
    console.print("\n[bold cyan]Step 1b: Relevance filtering raw records...[/bold cyan]")
    for source_name in list(all_raw.keys()):
        raw_records = all_raw[source_name]
        filtered_raw, rel_stats = filter_relevance(raw_records, source_name)
        if rel_stats.removed_irrelevant > 0:
            console.print(
                f"  {source_name}: kept {rel_stats.kept}/{rel_stats.total} "
                f"(removed {rel_stats.removed_irrelevant} irrelevant)"
            )
        all_raw[source_name] = filtered_raw

    # Step 2: Convert
    console.print("\n[bold cyan]Step 2: Converting to chat format...[/bold cyan]")
    all_converted = []
    for source_name, raw_records in all_raw.items():
        converted = convert_batch(raw_records, source_name)
        all_converted.extend(converted)
        console.print(f"  {source_name}: {len(converted)} converted records")

    if not all_converted:
        console.print("[yellow]No records after conversion. Pipeline complete.[/yellow]")
        return None

    # Step 3: Quality filter
    console.print("\n[bold cyan]Step 3: Quality filtering...[/bold cyan]")
    filter_config = FilterConfig(
        min_content_length=config.get("quality", {}).get("min_content_length", 100),
        min_answer_length=config.get("quality", {}).get("min_answer_length", 200),
        max_answer_length=config.get("quality", {}).get("max_answer_length", 8000),
        max_table_ratio=config.get("quality", {}).get("max_table_ratio", 0.3),
        min_alpha_ratio=config.get("quality", {}).get("min_alpha_ratio", 0.3),
    )
    # Filter on assistant content (message index 2)
    filtered = []
    filter_rejected = 0
    for record in all_converted:
        messages = record.get("messages", [])
        if len(messages) >= 3:
            content = messages[2].get("content", "")
            from processors.quality_filter import filter_record
            passed, reason = filter_record(content, filter_config, min_length=filter_config.min_answer_length)
            if passed:
                filtered.append(record)
            else:
                filter_rejected += 1

    console.print(f"  Passed: {len(filtered)}, Rejected: {filter_rejected}")

    if not filtered:
        console.print("[yellow]No records passed quality filter. Pipeline complete.[/yellow]")
        return None

    # Step 4: Deduplicate
    console.print("\n[bold cyan]Step 4: Deduplicating...[/bold cyan]")
    dedup = DedupIndex(
        num_perm=config.get("dedup", {}).get("minhash_num_perm", 128),
        lsh_threshold=config.get("dedup", {}).get("lsh_threshold", 0.8),
        shingle_size=config.get("dedup", {}).get("shingle_size", 5),
    )

    training_dir = config.get("training_data_path", "")
    if training_dir and Path(training_dir).exists():
        dedup.load_existing(training_dir)

    # Extract assistant content for dedup
    unique = []
    dedup_stats = {"exact": 0, "near": 0, "unique": 0}
    for record in filtered:
        messages = record.get("messages", [])
        if len(messages) >= 3:
            content = messages[2].get("content", "")
            is_dup, reason = dedup.is_duplicate(content)
            if is_dup:
                dedup_stats[reason] = dedup_stats.get(reason, 0) + 1
            else:
                dedup._add_to_index(content)
                unique.append(record)
                dedup_stats["unique"] += 1

    console.print(f"  Unique: {dedup_stats['unique']}, Exact dupes: {dedup_stats.get('exact', 0)}, Near dupes: {dedup_stats.get('near', 0)}")

    if not unique:
        console.print("[yellow]All records were duplicates. Pipeline complete.[/yellow]")
        return None

    # Step 5: Validate
    console.print("\n[bold cyan]Step 5: Validating...[/bold cyan]")
    validator = DataValidator(config.get("validation", {}))
    validation_result = validator.validate_all(unique, existing_path=training_dir)
    validator.print_report(validation_result)

    if not validation_result.passed and not skip_validation:
        console.print("[red]Validation failed. Use --skip-validation to override.[/red]")
        return validation_result

    # Step 6: Create snapshot
    console.print("\n[bold cyan]Step 6: Creating versioned snapshot...[/bold cyan]")
    if dry_run:
        console.print("[yellow]Dry run — skipping snapshot creation[/yellow]")
        return validation_result

    vm = VersionManager(
        base_dir=str(PROJECT_ROOT / "data" / "pipeline"),
        training_data_dir=training_dir,
    )

    description = f"Pipeline run {run_id}: {len(unique)} records from {', '.join(enabled_sources)}"
    version = vm.create_snapshot(unique, description=description, sources=enabled_sources)

    # Step 7: Merge (optional)
    if auto_merge:
        console.print("\n[bold cyan]Step 7: Merging into training data...[/bold cyan]")
        vm.merge_to_training(version)
    else:
        console.print(
            f"\n[bold]Snapshot {version} created. "
            f"Run 'python -m scripts.merge {version}' to merge into training data.[/bold]"
        )

    return validation_result
