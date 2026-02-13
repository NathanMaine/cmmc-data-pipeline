"""CLI: Validate processed data."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import click
from rich.console import Console

from pipeline.runner import load_config
from pipeline.validator import DataValidator

console = Console()


@click.command()
@click.argument("data_file", type=click.Path(exists=True))
@click.option("--existing", help="Path to existing training data directory")
@click.option("--spot-check", "spot_check_n", type=int, default=0, help="Show N random samples")
@click.option("--config", "config_path", help="Path to config.yaml")
def main(data_file, existing, spot_check_n, config_path):
    """Validate a JSONL data file."""
    config = load_config(config_path)

    # Load records
    records = []
    with open(data_file) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    console.print(f"Loaded {len(records)} records from {data_file}")

    validator = DataValidator(config.get("validation", {}))
    result = validator.validate_all(records, existing_path=existing)
    validator.print_report(result)

    if spot_check_n > 0:
        console.print(f"\n[bold]Spot Check ({spot_check_n} samples):[/bold]")
        samples = validator.spot_check(records, n=spot_check_n)
        for i, sample in enumerate(samples):
            messages = sample.get("messages", [])
            console.print(f"\n--- Sample {i+1} ---")
            if len(messages) >= 2:
                console.print(f"[bold]Q:[/bold] {messages[1].get('content', '')[:200]}")
            if len(messages) >= 3:
                console.print(f"[bold]A:[/bold] {messages[2].get('content', '')[:300]}...")
            console.print(f"[dim]Source: {sample.get('source', 'unknown')}[/dim]")

    sys.exit(0 if result.passed else 1)


if __name__ == "__main__":
    main()
