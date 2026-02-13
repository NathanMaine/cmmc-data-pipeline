"""CLI: Rollback to a previous version."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import click
from rich.console import Console

from pipeline.runner import load_config, PROJECT_ROOT
from pipeline.versioning import VersionManager

console = Console()


@click.command()
@click.argument("version")
@click.option("--config", "config_path", help="Path to config.yaml")
def main(version, config_path):
    """Rollback to a specific version."""
    config = load_config(config_path)
    training_dir = config.get("training_data_dir", "")

    vm = VersionManager(
        base_dir=str(PROJECT_ROOT / "data" / "pipeline"),
        training_data_dir=training_dir,
    )

    records = vm.rollback(version)
    console.print(f"[green]Rolled back to {version} ({len(records)} records)[/green]")


if __name__ == "__main__":
    main()
