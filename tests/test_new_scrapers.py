"""Tests for the new scrapers: SP 800-171, CSF 2.0, DoD documents."""

import json
import pytest
from unittest.mock import MagicMock, patch

from processors.converter import (
    convert_sp800_171_record,
    convert_csf_record,
    convert_dod_document_record,
    convert_batch,
)
from processors.templates import select_template


# ── SP 800-171 converter tests ────────────────────────────────────

class TestConvertSP800171:
    def test_basic_conversion(self):
        raw = {
            "text": "03.01.01 — Account Management\n\nControl Statement:\na. Define the types of accounts.",
            "source": "NIST SP 800-171 Rev. 3 — 03.01.01",
            "title": "03.01.01 Account Management",
            "control_id": "03.01.01",
            "family": "Access Control",
        }
        result = convert_sp800_171_record(raw)
        assert result["messages"][0]["role"] == "system"
        assert result["messages"][1]["role"] == "user"
        assert result["messages"][2]["role"] == "assistant"
        assert "Account Management" in result["messages"][2]["content"]
        assert result["source"] == "nist_sp800_171_03.01.01"

    def test_question_uses_sp800_171_template(self):
        raw = {
            "text": "03.05.01 — Identification\n\nControl Statement:\nIdentify system users.",
            "title": "03.05.01 Identification",
            "control_id": "03.05.01",
        }
        result = convert_sp800_171_record(raw)
        question = result["messages"][1]["content"]
        # Should reference SP 800-171 or CUI or the topic
        assert any(kw in question for kw in ["800-171", "CUI", "Identification"])


# ── CSF 2.0 converter tests ──────────────────────────────────────

class TestConvertCSF:
    def test_category_conversion(self):
        raw = {
            "text": "GV.OC — Organizational Context\n\nThe circumstances surrounding...",
            "source": "NIST CSF 2.0 — GV.OC Organizational Context",
            "title": "GV.OC Organizational Context",
            "category_id": "GV.OC",
            "function": "GOVERN",
            "function_id": "GV",
        }
        result = convert_csf_record(raw)
        assert result["source"] == "nist_csf_GV.OC"
        assert result["messages"][2]["content"].startswith("GV.OC")

    def test_subcategory_conversion(self):
        raw = {
            "text": "GV.OC-01\n\nThe organizational mission is understood",
            "source": "NIST CSF 2.0 — GV.OC-01",
            "title": "GV.OC-01",
            "subcategory_id": "GV.OC-01",
            "category_id": "GV.OC",
            "function": "GOVERN",
        }
        result = convert_csf_record(raw)
        assert result["source"] == "nist_csf_GV.OC-01"

    def test_question_uses_csf_template(self):
        raw = {
            "text": "PR.AC — Access Control\n\nAccess to assets is managed.",
            "title": "PR.AC Access Control",
            "category_id": "PR.AC",
        }
        result = convert_csf_record(raw)
        question = result["messages"][1]["content"]
        assert any(kw in question for kw in ["CSF", "Cybersecurity Framework", "Access Control"])


# ── DoD document converter tests ──────────────────────────────────

class TestConvertDoDDocument:
    def test_basic_conversion(self):
        raw = {
            "text": "The CMMC Assessment Guide provides procedures for assessing CUI protection.",
            "source": "CMMC Assessment Guide Level 2",
            "doc_name": "CMMC Assessment Guide Level 2",
            "title": "Assessment Procedures Overview",
            "chunk_index": 0,
            "total_chunks": 10,
        }
        result = convert_dod_document_record(raw)
        assert "assistant" in result["messages"][2]["role"]
        assert "Assessment" in result["messages"][2]["content"]
        assert "dod_cmmc_assessment_guide_level_2_chunk0" == result["source"]

    def test_chunk_source_id(self):
        raw = {
            "text": "Scoping guidance for CMMC Level 2 assessments.",
            "doc_name": "CMMC Scoping Guide Level 2",
            "title": "Scoping Overview",
            "chunk_index": 3,
        }
        result = convert_dod_document_record(raw)
        assert "chunk3" in result["source"]


# ── Batch converter with new sources ──────────────────────────────

class TestConvertBatchNewSources:
    def test_sp800_171_batch(self):
        records = [
            {
                "text": "03.01.01 — Account Management\n\nDefine the types of accounts.",
                "title": "03.01.01 Account Management",
                "control_id": "03.01.01",
            },
            {
                "text": "03.01.02 — Access Enforcement\n\nEnforce approved authorizations.",
                "title": "03.01.02 Access Enforcement",
                "control_id": "03.01.02",
            },
        ]
        results = convert_batch(records, "nist_sp800_171")
        assert len(results) == 2
        assert all(r["source"].startswith("nist_sp800_171_") for r in results)

    def test_csf_batch(self):
        records = [
            {"text": "GV.OC — Organizational Context\n\nContext.", "title": "GV.OC Organizational Context", "category_id": "GV.OC"},
        ]
        results = convert_batch(records, "nist_csf")
        assert len(results) == 1

    def test_dod_batch(self):
        records = [
            {"text": "Assessment guide content here.", "doc_name": "Guide", "title": "Intro", "chunk_index": 0},
        ]
        results = convert_batch(records, "dod_documents")
        assert len(results) == 1

    def test_unknown_source_still_raises(self):
        with pytest.raises(ValueError, match="Unknown source type"):
            convert_batch([], "nonexistent_source")


# ── Template selection tests for new frameworks ───────────────────

class TestNewTemplates:
    def test_sp800_171_framework_template(self):
        q = select_template(source="SP 800-171", topic="Account Management", framework="sp800_171")
        assert "Account Management" in q
        assert any(kw in q for kw in ["800-171", "CUI"])

    def test_csf_framework_template(self):
        q = select_template(source="CSF 2.0", topic="Organizational Context", framework="csf")
        assert "Organizational Context" in q
        assert any(kw in q for kw in ["CSF", "Cybersecurity Framework"])

    def test_dod_document_template(self):
        q = select_template(source="CMMC Assessment Guide", topic="Access Control", framework="dod_document")
        assert "Access Control" in q

    def test_framework_none_falls_through(self):
        # When framework is None, should use generic templates
        q = select_template(source="Some Source", topic="Some Topic")
        assert "Some" in q


# ── SP 800-171 scraper unit tests (mock HTTP) ─────────────────────

class TestSP800171Scraper:
    def test_parse_control_with_all_parts(self):
        from scrapers.nist_sp800_171 import NISTSP800171Scraper

        scraper = NISTSP800171Scraper({})

        ctrl = {
            "id": "SP_800_171_03.01.01",
            "title": "Account Management",
            "params": [
                {
                    "id": "A.03.01.01.ODP.01",
                    "label": "time period",
                    "usage": "organization-defined time period",
                    "guidelines": [{"prose": "the time period is defined."}],
                }
            ],
            "props": [{"name": "label", "value": "Account Management (03.01.01)"}],
            "parts": [
                {
                    "name": "statement",
                    "prose": "",
                    "parts": [
                        {
                            "name": "item",
                            "prose": "Define the types of accounts.",
                            "props": [{"name": "label", "value": "a."}],
                        }
                    ],
                },
                {
                    "name": "guidance",
                    "prose": "This requirement focuses on account management.",
                },
                {
                    "name": "assessment-objective",
                    "prose": "system accounts are created properly.",
                },
                {
                    "name": "assessment-method",
                    "prose": "",
                    "props": [{"name": "label", "value": "Examine"}],
                    "parts": [
                        {"name": "assessment-objects", "prose": "access control policy"}
                    ],
                },
            ],
        }

        record = scraper._parse_control(ctrl, "Access Control", "03.01")
        assert record is not None
        assert record["control_id"] == "03.01.01"
        assert record["family"] == "Access Control"
        assert "Account Management" in record["text"]
        assert "Define the types of accounts" in record["text"]
        assert "account management" in record["text"]
        assert "Assessment Objectives" in record["text"]
        assert "Organization-Defined Parameters" in record["text"]
        assert "Examine" in record["text"]

    def test_skip_control_without_parts(self):
        from scrapers.nist_sp800_171 import NISTSP800171Scraper

        scraper = NISTSP800171Scraper({})
        ctrl = {
            "id": "SP_800_171_03.07",
            "title": "Maintenance",
            "props": [{"name": "sort-id", "value": "03.07"}],
        }
        assert scraper._parse_control(ctrl, "Maintenance", "03.07") is None

    def test_oscal_id_conversion(self):
        from scrapers.nist_sp800_171 import NISTSP800171Scraper

        assert NISTSP800171Scraper._oscal_id_to_control_id("SP_800_171_03.01.01") == "03.01.01"
        assert NISTSP800171Scraper._oscal_id_to_control_id("SP_800_171_03.17") == "03.17"
        assert NISTSP800171Scraper._oscal_id_to_control_id("other_id") == "other_id"

    def test_param_cleanup(self):
        from scrapers.nist_sp800_171 import NISTSP800171Scraper

        scraper = NISTSP800171Scraper({})
        cleaned = scraper._clean_prose(
            "after {{ insert: param, A.03.01.01.ODP.01 }} of inactivity"
        )
        assert "{{ insert" not in cleaned
        assert "[organization-defined parameter]" in cleaned


# ── CSF 2.0 scraper unit tests ────────────────────────────────────

class TestCSFScraper:
    def test_parse_subcategory(self):
        from scrapers.nist_csf import NISTCSFScraper

        scraper = NISTCSFScraper({})
        subcat = {
            "id": "GV.OC-01",
            "class": "subcategory",
            "title": "GV.OC-01",
            "parts": [
                {
                    "id": "GV.OC-01_statement",
                    "name": "statement",
                    "prose": "The organizational mission is understood",
                },
                {
                    "id": "GV.OC-01.001",
                    "name": "example",
                    "prose": "Share the organization's mission to provide a basis",
                },
            ],
        }
        record = scraper._parse_subcategory(subcat, "GV.OC", "GV", "GOVERN")
        assert record is not None
        assert record["subcategory_id"] == "GV.OC-01"
        assert "organizational mission" in record["text"]
        assert "Implementation Example" in record["text"]

    def test_parse_category_with_subcategories(self):
        from scrapers.nist_csf import NISTCSFScraper

        scraper = NISTCSFScraper({})
        category = {
            "id": "GV.OC",
            "class": "category",
            "title": "Organizational Context",
            "parts": [
                {
                    "id": "GV.OC_statement",
                    "name": "statement",
                    "prose": "The circumstances surrounding the organization",
                }
            ],
            "controls": [
                {
                    "id": "GV.OC-01",
                    "class": "subcategory",
                    "title": "GV.OC-01",
                    "parts": [
                        {"name": "statement", "prose": "The organizational mission is understood and informs cybersecurity risk management decisions across the organization"},
                    ],
                },
                {
                    "id": "GV.OC-02",
                    "class": "subcategory",
                    "title": "GV.OC-02",
                    "parts": [
                        {"name": "statement", "prose": "Internal and external stakeholders are understood and their needs and expectations regarding cybersecurity risk management are identified"},
                    ],
                },
            ],
        }
        cat_record, sub_records = scraper._parse_category(category, "GV", "GOVERN")
        assert cat_record is not None
        assert "Organizational Context" in cat_record["text"]
        assert "Subcategories:" in cat_record["text"]
        assert len(sub_records) == 2


# ── DoD document scraper unit tests ───────────────────────────────

class TestDoDDocumentScraper:
    def test_chunk_text_short(self):
        from scrapers.dod_documents import DoDDocumentScraper

        scraper = DoDDocumentScraper({})
        # Short text below CHUNK_MAX should return as-is
        text = "This is a meaningful paragraph about CMMC assessment procedures. " * 5
        chunks = scraper._chunk_text(text)
        assert len(chunks) == 1
        assert "CMMC assessment" in chunks[0]

    def test_chunk_text_long(self):
        from scrapers.dod_documents import DoDDocumentScraper

        scraper = DoDDocumentScraper({})
        # Build text that exceeds CHUNK_TARGET
        paragraphs = [f"Paragraph {i}: " + "compliance content " * 40 for i in range(10)]
        text = "\n\n".join(paragraphs)
        chunks = scraper._chunk_text(text)
        assert len(chunks) > 1
        # Each chunk should be non-empty
        assert all(len(c) > 0 for c in chunks)

    def test_junk_detection(self):
        from scrapers.dod_documents import DoDDocumentScraper

        scraper = DoDDocumentScraper({})
        assert scraper._is_junk("short") is True
        assert scraper._is_junk("12345 67890 " * 20) is True  # low alpha
        assert scraper._is_junk("Real content about CMMC " * 10) is False

    def test_detect_heading(self):
        from scrapers.dod_documents import DoDDocumentScraper

        assert DoDDocumentScraper._detect_heading("3.1 Access Control\nContent here") == "3.1 Access Control"
        assert DoDDocumentScraper._detect_heading("Some regular text without heading") is None

    def test_last_sentence(self):
        from scrapers.dod_documents import DoDDocumentScraper

        result = DoDDocumentScraper._last_sentence(
            "First sentence here. Second sentence here. Third sentence about compliance."
        )
        assert result == "Third sentence about compliance."

    def test_incremental_filters_by_date(self):
        from scrapers.dod_documents import DoDDocumentScraper

        scraper = DoDDocumentScraper({})
        # Mock _process_documents to check what gets passed
        with patch.object(scraper, '_process_documents', return_value=[]) as mock:
            scraper.scrape_incremental("2025-06-01")
            # Should filter to only docs with date >= 2025-06-01
            called_docs = mock.call_args[0][0]
            assert all(d["date"] >= "2025-06-01" for d in called_docs)
            # Should include Aug 2025 docs but not Dec 2024
            dates = [d["date"] for d in called_docs]
            assert "2024-12-16" not in dates  # Scoping Guide L3 should be excluded
