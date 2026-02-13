"""Dataset versioning with snapshots and rollback."""

import json
import logging
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from rich.console import Console

console = Console()
logger = logging.getLogger(__name__)


@dataclass
class VersionInfo:
    version: str
    created_at: str
    description: str
    record_count: int
    sources: list[str] = field(default_factory=list)
    parent_version: str = ""


class VersionManager:
    """Manages versioned dataset snapshots."""

    def __init__(self, base_dir: str, training_data_dir: str = None):
        self.base_dir = Path(base_dir)
        self.versions_dir = self.base_dir / "versions"
        self.versions_dir.mkdir(parents=True, exist_ok=True)
        self.manifest_path = self.base_dir / "manifest.json"
        self.training_data_dir = Path(training_data_dir) if training_data_dir else None
        self._load_manifest()

    def _load_manifest(self):
        if self.manifest_path.exists():
            with open(self.manifest_path) as f:
                self.manifest = json.load(f)
        else:
            self.manifest = {"versions": [], "current": None}

    def _save_manifest(self):
        with open(self.manifest_path, "w") as f:
            json.dump(self.manifest, f, indent=2)

    def _next_version(self) -> str:
        existing = [v["version"] for v in self.manifest["versions"]]
        if not existing:
            return "v001"
        last_num = max(int(v.lstrip("v")) for v in existing)
        return f"v{last_num + 1:03d}"

    def create_snapshot(self, records: list[dict], description: str = "", sources: list[str] = None) -> str:
        """Create a new versioned snapshot of processed records."""
        version = self._next_version()
        version_dir = self.versions_dir / version
        version_dir.mkdir(parents=True, exist_ok=True)

        # Save records
        records_file = version_dir / "records.jsonl"
        with open(records_file, "w") as f:
            for record in records:
                f.write(json.dumps(record, default=str) + "\n")

        # Create version info
        info = VersionInfo(
            version=version,
            created_at=datetime.now(timezone.utc).isoformat(),
            description=description,
            record_count=len(records),
            sources=sources or [],
            parent_version=self.manifest.get("current", ""),
        )

        # Save version metadata
        info_file = version_dir / "version_info.json"
        with open(info_file, "w") as f:
            json.dump(info.__dict__, f, indent=2)

        # Update manifest
        self.manifest["versions"].append(info.__dict__)
        self.manifest["current"] = version
        self._save_manifest()

        console.print(f"[green]Created snapshot {version}: {len(records)} records[/green]")
        return version

    def rollback(self, target_version: str) -> list[dict]:
        """Rollback to a specific version. Returns the records from that version."""
        version_dir = self.versions_dir / target_version
        if not version_dir.exists():
            raise ValueError(f"Version {target_version} not found")

        records = self._load_version_records(target_version)

        self.manifest["current"] = target_version
        self._save_manifest()

        console.print(f"[yellow]Rolled back to {target_version}: {len(records)} records[/yellow]")
        return records

    def get_current_records(self) -> list[dict]:
        """Get records from the current version."""
        current = self.manifest.get("current")
        if not current:
            return []
        return self._load_version_records(current)

    def _load_version_records(self, version: str) -> list[dict]:
        records_file = self.versions_dir / version / "records.jsonl"
        if not records_file.exists():
            return []
        records = []
        with open(records_file) as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    def list_versions(self) -> list[dict]:
        """List all available versions with metadata."""
        return self.manifest.get("versions", [])

    def diff_versions(self, version_a: str, version_b: str) -> dict:
        """Compare two versions and show differences."""
        records_a = self._load_version_records(version_a)
        records_b = self._load_version_records(version_b)

        sources_a = set()
        sources_b = set()
        for r in records_a:
            msgs = r.get("messages", [])
            if len(msgs) >= 3:
                sources_a.add(r.get("source", ""))
        for r in records_b:
            msgs = r.get("messages", [])
            if len(msgs) >= 3:
                sources_b.add(r.get("source", ""))

        return {
            "version_a": version_a,
            "version_b": version_b,
            "records_a": len(records_a),
            "records_b": len(records_b),
            "delta": len(records_b) - len(records_a),
            "new_sources": list(sources_b - sources_a),
            "removed_sources": list(sources_a - sources_b),
        }

    def delete_version(self, version: str):
        """Delete a version (cannot delete current)."""
        if version == self.manifest.get("current"):
            raise ValueError("Cannot delete the current version. Rollback first.")
        version_dir = self.versions_dir / version
        if version_dir.exists():
            shutil.rmtree(version_dir)
        self.manifest["versions"] = [
            v for v in self.manifest["versions"] if v["version"] != version
        ]
        self._save_manifest()
        console.print(f"[red]Deleted version {version}[/red]")

    def merge_to_training(self, version: str = None) -> Path:
        """Merge a version's records into the training data directory."""
        if not self.training_data_dir:
            raise ValueError("No training_data_dir configured")

        version = version or self.manifest.get("current")
        if not version:
            raise ValueError("No version specified and no current version")

        records = self._load_version_records(version)
        if not records:
            raise ValueError(f"No records in version {version}")

        # Load existing training data
        train_path = self.training_data_dir / "train.jsonl"
        existing = []
        if train_path.exists():
            with open(train_path) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        existing.append(json.loads(line))

        # Backup existing
        if train_path.exists():
            backup = self.training_data_dir / f"train.jsonl.bak.{version}"
            shutil.copy2(train_path, backup)

        # Merge
        merged = existing + records
        with open(train_path, "w") as f:
            for record in merged:
                f.write(json.dumps(record, default=str) + "\n")

        console.print(
            f"[green]Merged {len(records)} new records into training data "
            f"({len(existing)} existing + {len(records)} new = {len(merged)} total)[/green]"
        )
        return train_path
