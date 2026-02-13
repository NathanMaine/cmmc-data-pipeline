"""Tests for dataset versioning."""

import json
import tempfile
from pathlib import Path

import pytest
from pipeline.versioning import VersionManager


def make_sample_records(n=5):
    return [
        {
            "messages": [
                {"role": "system", "content": "You are a CMMC expert."},
                {"role": "user", "content": f"Question {i}?"},
                {"role": "assistant", "content": f"Answer {i} about CMMC compliance."},
            ],
            "source": f"test_{i}",
        }
        for i in range(n)
    ]


class TestVersionManager:
    def test_create_snapshot(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vm = VersionManager(base_dir=tmpdir)
            records = make_sample_records(3)
            version = vm.create_snapshot(records, description="Test snapshot")

            assert version == "v001"
            assert vm.manifest["current"] == "v001"
            assert len(vm.manifest["versions"]) == 1

    def test_sequential_versions(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vm = VersionManager(base_dir=tmpdir)
            v1 = vm.create_snapshot(make_sample_records(3))
            v2 = vm.create_snapshot(make_sample_records(5))

            assert v1 == "v001"
            assert v2 == "v002"
            assert vm.manifest["current"] == "v002"

    def test_get_current_records(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vm = VersionManager(base_dir=tmpdir)
            records = make_sample_records(3)
            vm.create_snapshot(records)

            loaded = vm.get_current_records()
            assert len(loaded) == 3

    def test_rollback(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vm = VersionManager(base_dir=tmpdir)
            vm.create_snapshot(make_sample_records(3), description="v1")
            vm.create_snapshot(make_sample_records(5), description="v2")

            assert vm.manifest["current"] == "v002"
            records = vm.rollback("v001")
            assert vm.manifest["current"] == "v001"
            assert len(records) == 3

    def test_rollback_nonexistent_version(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vm = VersionManager(base_dir=tmpdir)
            with pytest.raises(ValueError, match="not found"):
                vm.rollback("v999")

    def test_list_versions(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vm = VersionManager(base_dir=tmpdir)
            vm.create_snapshot(make_sample_records(3), sources=["nist_csrc"])
            vm.create_snapshot(make_sample_records(5), sources=["ecfr"])

            versions = vm.list_versions()
            assert len(versions) == 2
            assert versions[0]["sources"] == ["nist_csrc"]
            assert versions[1]["sources"] == ["ecfr"]

    def test_diff_versions(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vm = VersionManager(base_dir=tmpdir)
            vm.create_snapshot(make_sample_records(3))
            vm.create_snapshot(make_sample_records(7))

            diff = vm.diff_versions("v001", "v002")
            assert diff["records_a"] == 3
            assert diff["records_b"] == 7
            assert diff["delta"] == 4

    def test_delete_version(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vm = VersionManager(base_dir=tmpdir)
            vm.create_snapshot(make_sample_records(3))
            vm.create_snapshot(make_sample_records(5))

            vm.delete_version("v001")
            assert len(vm.manifest["versions"]) == 1

    def test_cannot_delete_current(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vm = VersionManager(base_dir=tmpdir)
            vm.create_snapshot(make_sample_records(3))

            with pytest.raises(ValueError, match="Cannot delete the current"):
                vm.delete_version("v001")

    def test_merge_to_training(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            training_dir = Path(tmpdir) / "training"
            training_dir.mkdir()

            # Create existing training data
            train_file = training_dir / "train.jsonl"
            existing = make_sample_records(2)
            with open(train_file, "w") as f:
                for r in existing:
                    f.write(json.dumps(r) + "\n")

            vm = VersionManager(
                base_dir=str(Path(tmpdir) / "versions"),
                training_data_dir=str(training_dir),
            )
            new_records = make_sample_records(3)
            vm.create_snapshot(new_records)
            vm.merge_to_training()

            # Check merged file
            merged = []
            with open(train_file) as f:
                for line in f:
                    merged.append(json.loads(line))
            assert len(merged) == 5  # 2 existing + 3 new

    def test_persistence(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            vm1 = VersionManager(base_dir=tmpdir)
            vm1.create_snapshot(make_sample_records(3))

            # Create new instance (simulates restart)
            vm2 = VersionManager(base_dir=tmpdir)
            assert vm2.manifest["current"] == "v001"
            assert len(vm2.list_versions()) == 1
