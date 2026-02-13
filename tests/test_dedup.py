"""Tests for deduplication."""

import json
import tempfile
from pathlib import Path

import pytest
from processors.dedup import DedupIndex, DedupStats


class TestDedupIndex:
    def test_exact_duplicate_detection(self):
        idx = DedupIndex()
        content = "This is a test content about CMMC compliance."

        # First addition
        is_dup, reason = idx.is_duplicate(content)
        assert is_dup is False
        idx._add_to_index(content)

        # Same content again
        is_dup, reason = idx.is_duplicate(content)
        assert is_dup is True
        assert reason == "exact"

    def test_near_duplicate_detection(self):
        idx = DedupIndex(lsh_threshold=0.8)
        content1 = "CMMC Level 2 requires organizations to implement 110 security controls based on NIST SP 800-171. These controls cover access control, awareness training, audit accountability, and more."
        content2 = "CMMC Level 2 requires organizations to implement 110 security controls based on NIST SP 800-171. These controls cover access control, awareness training, audit and accountability, and more areas."

        idx._add_to_index(content1)
        is_dup, reason = idx.is_duplicate(content2)
        # Near duplicate should be detected
        assert is_dup is True
        assert reason == "near"

    def test_different_content_not_flagged(self):
        idx = DedupIndex()
        content1 = "CMMC Level 2 requires organizations to implement 110 security controls."
        content2 = "HIPAA Security Rule establishes national standards for protecting electronic health information."

        idx._add_to_index(content1)
        is_dup, reason = idx.is_duplicate(content2)
        assert is_dup is False

    def test_batch_dedup(self):
        idx = DedupIndex()
        records = [
            {"text": "Unique content about CMMC compliance requirements and assessment procedures."},
            {"text": "Unique content about CMMC compliance requirements and assessment procedures."},  # exact dup
            {"text": "Different content about NIST SP 800-171 security controls and implementation guidance."},
        ]
        unique, stats = idx.deduplicate_batch(records)
        assert stats.unique == 2
        assert stats.exact_dupes == 1
        assert len(unique) == 2

    def test_load_existing_training_data(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create mock training data
            train_file = Path(tmpdir) / "train.jsonl"
            records = [
                {
                    "messages": [
                        {"role": "system", "content": "system prompt"},
                        {"role": "user", "content": "question"},
                        {"role": "assistant", "content": "This is existing training content about CMMC."},
                    ],
                    "source": "test",
                }
            ]
            with open(train_file, "w") as f:
                for r in records:
                    f.write(json.dumps(r) + "\n")

            idx = DedupIndex()
            idx.load_existing(tmpdir)

            # Check that existing content is detected
            is_dup, _ = idx.is_duplicate("This is existing training content about CMMC.")
            assert is_dup is True

    def test_save_and_load_index(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            idx = DedupIndex()
            idx._add_to_index("test content")
            idx._add_to_index("other content")

            index_path = str(Path(tmpdir) / "hashes.json")
            idx.save_index(index_path)

            idx2 = DedupIndex()
            idx2.load_index(index_path)
            assert len(idx2.exact_hashes) == 2
