"""Tests for data conversion to chat format."""

import pytest
from processors.converter import (
    convert_nist_record,
    convert_federal_register_record,
    convert_ecfr_record,
    convert_batch,
    extract_topic,
)


class TestExtractTopic:
    def test_numbered_heading(self):
        text = "3.2.1 Service Discovery Mechanism Threats\nSome body text."
        topic = extract_topic(text)
        assert "Service Discovery" in topic

    def test_plain_heading(self):
        text = "Access Control Requirements\nDetails follow."
        topic = extract_topic(text)
        assert "Access Control" in topic

    def test_short_text_returns_empty(self):
        assert extract_topic("Hi") == ""


class TestConvertNIST:
    def test_basic_conversion(self):
        raw = {
            "text": "AC-1 — Access Control Policy and Procedures\n\nThe organization develops and maintains access control policies.",
            "source": "NIST SP 800-53 Rev. 5 — AC-1",
            "title": "AC-1 Access Control Policy",
            "control_id": "AC-1",
        }
        result = convert_nist_record(raw)
        assert "messages" in result
        assert len(result["messages"]) == 3
        assert result["messages"][0]["role"] == "system"
        assert result["messages"][1]["role"] == "user"
        assert result["messages"][2]["role"] == "assistant"
        assert "source" in result

    def test_assistant_contains_text(self):
        raw = {
            "text": "This is the actual NIST publication content about security controls.",
            "source": "NIST SP 800-171",
            "title": "Security Controls",
            "control_id": "SC-1",
        }
        result = convert_nist_record(raw)
        assert raw["text"] in result["messages"][2]["content"]


class TestConvertFederalRegister:
    def test_single_record(self):
        raw = {
            "text": "The Department of Defense published a final rule regarding CMMC implementation.",
            "title": "CMMC Final Rule",
            "doc_type": "Rule",
            "document_number": "2024-12345",
        }
        result = convert_federal_register_record(raw)
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["messages"][2]["role"] == "assistant"

    def test_chunked_records(self):
        raw_list = [
            {"text": "Chunk 1 content", "title": "CMMC Rule", "doc_type": "Rule", "document_number": "123", "chunk_index": 0},
            {"text": "Chunk 2 content", "title": "CMMC Rule", "doc_type": "Rule", "document_number": "123", "chunk_index": 1},
        ]
        result = convert_federal_register_record(raw_list)
        assert len(result) == 2


class TestConvertECFR:
    def test_basic_conversion(self):
        raw = {
            "text": "Each contractor must implement the security requirements specified in NIST SP 800-171.",
            "cfr_ref": "32 CFR § 170.14",
            "title": "Security Requirements",
            "section_number": "170.14",
            "cfr_title": 32,
            "cfr_part": 170,
        }
        result = convert_ecfr_record(raw)
        assert "messages" in result
        assert len(result["messages"]) == 3
        assert "source" in result


class TestConvertBatch:
    def test_nist_batch(self):
        records = [
            {"text": "Control content 1", "source": "NIST", "title": "Control 1", "control_id": "AC-1"},
            {"text": "Control content 2", "source": "NIST", "title": "Control 2", "control_id": "AC-2"},
        ]
        results = convert_batch(records, "nist_csrc")
        assert len(results) == 2
        assert all("messages" in r for r in results)

    def test_unknown_source_raises(self):
        with pytest.raises(ValueError, match="Unknown source type"):
            convert_batch([], "invalid_source")
