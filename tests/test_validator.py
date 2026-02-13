"""Tests for data validation."""

import pytest
from pipeline.validator import DataValidator, ValidationResult


def make_valid_record(answer="This is a detailed answer about CMMC compliance requirements and security controls."):
    return {
        "messages": [
            {"role": "system", "content": "You are a CMMC and cybersecurity compliance expert."},
            {"role": "user", "content": "What is CMMC?"},
            {"role": "assistant", "content": answer},
        ],
        "source": "test_source",
    }


class TestValidateFormat:
    def test_valid_record_passes(self):
        validator = DataValidator()
        records = [make_valid_record() for _ in range(10)]
        result = validator.validate_all(records)
        assert result.passed is True
        assert len(result.format_errors) == 0

    def test_missing_messages(self):
        validator = DataValidator()
        records = [{"text": "no messages"}] * 10
        result = validator.validate_all(records)
        assert result.passed is False

    def test_wrong_role_order(self):
        validator = DataValidator()
        records = [{
            "messages": [
                {"role": "user", "content": "wrong order"},
                {"role": "system", "content": "should be first"},
                {"role": "assistant", "content": "answer"},
            ],
            "source": "test",
        }] * 10
        result = validator.validate_all(records)
        assert len(result.format_errors) > 0

    def test_empty_content_flagged(self):
        validator = DataValidator()
        records = [{
            "messages": [
                {"role": "system", "content": "system"},
                {"role": "user", "content": ""},
                {"role": "assistant", "content": "answer"},
            ],
            "source": "test",
        }] * 10
        result = validator.validate_all(records)
        assert len(result.format_errors) > 0

    def test_too_few_records(self):
        validator = DataValidator({"min_records": 10})
        records = [make_valid_record()]
        result = validator.validate_all(records)
        assert result.passed is False


class TestValidateQuality:
    def test_answer_length_stats(self):
        validator = DataValidator()
        records = [make_valid_record("x" * 500) for _ in range(10)]
        result = validator.validate_all(records)
        assert "avg_answer_length" in result.stats
        assert result.stats["avg_answer_length"] == 500

    def test_source_diversity_tracked(self):
        validator = DataValidator()
        records = [make_valid_record() for _ in range(10)]
        result = validator.validate_all(records)
        assert "unique_sources" in result.stats


class TestSpotCheck:
    def test_returns_samples(self):
        validator = DataValidator()
        records = [make_valid_record() for _ in range(20)]
        samples = validator.spot_check(records, n=5)
        assert len(samples) == 5

    def test_handles_small_dataset(self):
        validator = DataValidator()
        records = [make_valid_record() for _ in range(2)]
        samples = validator.spot_check(records, n=5)
        assert len(samples) == 2
