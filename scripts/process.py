"""CLI: Process raw scraped data into training format."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import click
from rich.console import Console

from pipeline.runner import load_config, PROJECT_ROOT
from processors.converter import convert_batch
from processors.quality_filter import FilterConfig, filter_batch, filter_record

console = Console()


@click.command()
@click.argument("raw_dir", type=click.Path(exists=True))
@click.option("--source", "-s", required=True, help="Source type (nist_csrc, federal_register, ecfr)")
@click.option("--output", "-o", help="Output JSONL file path")
@click.option("--config", "config_path", help="Path to config.yaml")
def main(raw_dir, source, output, config_path):
    """Process raw scraped data into training format."""
    config = load_config(config_path)

    # Load raw records
    raw_path = Path(raw_dir) / "records.json"
    if not raw_path.exists():
        console.print(f"[red]No records.json found in {raw_dir}[/red]")
        return

    with open(raw_path) as f:
        raw_records = json.load(f)

    console.print(f"Loaded {len(raw_records)} raw records from {source}")

    # Convert
    converted = convert_batch(raw_records, source)
    console.print(f"Converted: {len(converted)} records")

    # Quality filter
    filter_config = FilterConfig(
        min_content_length=config.get("quality", {}).get("min_content_length", 100),
        min_answer_length=config.get("quality", {}).get("min_answer_length", 200),
    )

    filtered = []
    for record in converted:
        messages = record.get("messages", [])
        if len(messages) >= 3:
            content = messages[2].get("content", "")
            passed, _ = filter_record(content, filter_config, min_length=filter_config.min_answer_length)
            if passed:
                filtered.append(record)

    console.print(f"After quality filter: {len(filtered)} records")

    # Save
    if output:
        out_path = Path(output)
    else:
        out_path = PROJECT_ROOT / "data" / "processed" / f"{source}_processed.jsonl"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        for record in filtered:
            f.write(json.dumps(record, default=str) + "\n")

    console.print(f"[green]Saved {len(filtered)} records to {out_path}[/green]")


if __name__ == "__main__":
    main()
