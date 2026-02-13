"""CLI: Merge a versioned snapshot into training data."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import click
from rich.console import Console

from pipeline.runner import load_config, PROJECT_ROOT
from pipeline.versioning import VersionManager

console = Console()


@click.command()
@click.argument("version", required=False)
@click.option("--config", "config_path", help="Path to config.yaml")
def main(version, config_path):
    """Merge a version snapshot into training data. Uses current version if none specified."""
    config = load_config(config_path)
    training_dir = config.get("training_data_path", "")

    vm = VersionManager(
        base_dir=str(PROJECT_ROOT / "data" / "pipeline"),
        training_data_dir=training_dir,
    )

    if not version:
        version = vm.manifest.get("current")
        if not version:
            console.print("[red]No version specified and no current version[/red]")
            return

    console.print(f"Merging version {version} into training data at {training_dir}")
    vm.merge_to_training(version)


if __name__ == "__main__":
    main()
