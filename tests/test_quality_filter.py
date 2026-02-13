"""Tests for quality filtering."""

import pytest
from processors.quality_filter import FilterConfig, FilterStats, filter_record, filter_batch


class TestFilterRecord:
    def test_passes_good_content(self):
        text = "This is a comprehensive explanation of CMMC Level 2 requirements " * 5
        passed, reason = filter_record(text)
        assert passed is True
        assert reason == ""

    def test_rejects_short_content(self):
        passed, reason = filter_record("Too short")
        assert passed is False
        assert reason == "too_short"

    def test_rejects_section_numbers_only(self):
        passed, reason = filter_record("3.2.1")
        assert passed is False
        # Either too_short or section_numbers_only
        assert reason in ("too_short", "section_numbers_only")

    def test_rejects_table_borders(self):
        text = "|---|---|---|\n" * 20
        passed, reason = filter_record(text)
        assert passed is False

    def test_rejects_table_heavy(self):
        text = "| col1 | col2 | col3 |\n|---|---|---|\n" * 20
        passed, reason = filter_record(text)
        assert passed is False

    def test_rejects_low_alpha(self):
        text = "12345 67890 " * 50
        passed, reason = filter_record(text)
        assert passed is False
        assert reason == "low_alpha"

    def test_custom_min_length(self):
        text = "a" * 150
        passed, _ = filter_record(text, min_length=200)
        assert passed is False

        passed, _ = filter_record(text, min_length=100)
        assert passed is True

    def test_custom_config(self):
        config = FilterConfig(min_content_length=50, min_alpha_ratio=0.1)
        text = "abc 123 " * 10
        passed, _ = filter_record(text, config)
        assert passed is True


class TestFilterBatch:
    def test_filters_batch(self):
        records = [
            {"text": "This is good content about CMMC compliance requirements and controls " * 3},
            {"text": "short"},
            {"text": "Another good record about cybersecurity frameworks and NIST standards " * 3},
        ]
        passed, stats = filter_batch(records)
        assert stats.total == 3
        assert stats.passed == 2
        assert stats.rejected_too_short == 1
        assert len(passed) == 2

    def test_custom_content_key(self):
        records = [
            {"body": "This is content stored in a different key name for testing " * 3},
            {"body": "x"},
        ]
        passed, stats = filter_batch(records, content_key="body")
        assert stats.passed == 1
