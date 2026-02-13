"""CLI: Show pipeline status and version history."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import click
from rich.console import Console
from rich.table import Table

from pipeline.runner import load_config, PROJECT_ROOT
from pipeline.versioning import VersionManager

console = Console()


@click.command()
@click.option("--diff", "diff_versions", nargs=2, help="Compare two versions")
@click.option("--config", "config_path", help="Path to config.yaml")
def main(diff_versions, config_path):
    """Show pipeline status and version history."""
    config = load_config(config_path)
    training_dir = config.get("training_data_dir", "")

    vm = VersionManager(
        base_dir=str(PROJECT_ROOT / "data" / "pipeline"),
        training_data_dir=training_dir,
    )

    if diff_versions:
        diff = vm.diff_versions(diff_versions[0], diff_versions[1])
        console.print(f"\n[bold]Diff: {diff['version_a']} → {diff['version_b']}[/bold]")
        console.print(f"  Records: {diff['records_a']} → {diff['records_b']} (delta: {diff['delta']:+d})")
        if diff["new_sources"]:
            console.print(f"  New sources: {', '.join(diff['new_sources'][:10])}")
        if diff["removed_sources"]:
            console.print(f"  Removed sources: {', '.join(diff['removed_sources'][:10])}")
        return

    versions = vm.list_versions()
    current = vm.manifest.get("current", "")

    if not versions:
        console.print("[yellow]No versions found.[/yellow]")
        return

    table = Table(title="Dataset Versions")
    table.add_column("Version", style="bold")
    table.add_column("Created")
    table.add_column("Records")
    table.add_column("Sources")
    table.add_column("Description")
    table.add_column("Current")

    for v in versions:
        is_current = "→" if v["version"] == current else ""
        table.add_row(
            v["version"],
            v.get("created_at", "")[:19],
            str(v.get("record_count", 0)),
            ", ".join(v.get("sources", [])),
            v.get("description", "")[:60],
            is_current,
        )

    console.print(table)


if __name__ == "__main__":
    main()
