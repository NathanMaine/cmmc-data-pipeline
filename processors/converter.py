"""Convert raw scraped data into chat training format."""

import re
import logging
from pathlib import Path

from .templates import SYSTEM_PROMPT, select_template, make_chat_record

logger = logging.getLogger(__name__)


def extract_topic(text: str) -> str:
    """Extract a topic from text using heading patterns."""
    # Pattern: "3.2.1 Service Discovery Mechanism Threats"
    match = re.match(r"[\d\.]+\s+(.+?)(?:\n|$)", text)
    if match:
        topic = match.group(1).strip()
        if 10 < len(topic) < 100:
            return topic

    # Fallback: first line if reasonable length
    first_line = text.split("\n")[0].strip()
    if 10 < len(first_line) < 100 and first_line[0].isalpha():
        return first_line

    return ""


def convert_nist_record(raw: dict) -> dict:
    """Convert a NIST CSRC record to chat format."""
    text = raw.get("text", "")
    source = raw.get("source", "NIST Publication")
    topic = extract_topic(text) or raw.get("title", "")

    question = select_template(source=source, topic=topic if topic else None)
    return make_chat_record(question, text, f"nist_csrc_{raw.get('control_id', '')}")


def convert_federal_register_record(raw: dict) -> list[dict]:
    """Convert a Federal Register document to chat format.

    May return multiple records if document was chunked.
    """
    if isinstance(raw, list):
        # Already chunked by scraper
        return [_convert_single_fr(r) for r in raw]
    return [_convert_single_fr(raw)]


def _convert_single_fr(raw: dict) -> dict:
    """Convert a single FR record."""
    text = raw.get("text", "")
    title = raw.get("title", "")
    doc_type = raw.get("doc_type", "Document")

    topic = title[:100] if title else extract_topic(text)
    question = select_template(topic=topic, doc_type=doc_type)

    source_id = f"federal_register_{raw.get('document_number', '')}"
    if raw.get("chunk_index", 0) > 0:
        source_id += f"_chunk{raw['chunk_index']}"

    return make_chat_record(question, text, source_id)


def convert_ecfr_record(raw: dict) -> dict:
    """Convert an eCFR regulatory section to chat format."""
    text = raw.get("text", "")
    cfr_ref = raw.get("cfr_ref", "")
    topic = raw.get("title", "") or extract_topic(text)

    question = select_template(cfr_ref=cfr_ref, topic=topic if topic else None)

    section = raw.get("section_number", "").replace(".", "_")
    source_id = f"ecfr_{raw.get('cfr_title', '')}_{raw.get('cfr_part', '')}_{section}"

    return make_chat_record(question, text, source_id)


def convert_sp800_171_record(raw: dict) -> dict:
    """Convert a NIST SP 800-171 Rev. 3 control to chat format."""
    text = raw.get("text", "")
    title = raw.get("title", "")
    control_id = raw.get("control_id", "")
    topic = title or extract_topic(text)

    question = select_template(
        source="NIST SP 800-171 Rev. 3",
        topic=topic if topic else None,
        framework="sp800_171",
    )
    return make_chat_record(question, text, f"nist_sp800_171_{control_id}")


def convert_csf_record(raw: dict) -> dict:
    """Convert a NIST CSF 2.0 category or subcategory to chat format."""
    text = raw.get("text", "")
    title = raw.get("title", "")
    cat_id = raw.get("subcategory_id", "") or raw.get("category_id", "")
    topic = title or extract_topic(text)

    question = select_template(
        source="NIST CSF 2.0",
        topic=topic if topic else None,
        framework="csf",
    )
    return make_chat_record(question, text, f"nist_csf_{cat_id}")


def convert_dod_document_record(raw: dict) -> dict:
    """Convert a DoD PDF document chunk to chat format."""
    text = raw.get("text", "")
    doc_name = raw.get("doc_name", raw.get("source", ""))
    title = raw.get("title", "")
    chunk_idx = raw.get("chunk_index", 0)
    topic = title if title and not title.startswith(doc_name) else extract_topic(text)

    question = select_template(
        source=doc_name,
        topic=topic if topic else None,
        framework="dod_document",
    )
    source_id = f"dod_{raw.get('doc_name', '').replace(' ', '_').lower()}_chunk{chunk_idx}"
    return make_chat_record(question, text, source_id)


def convert_batch(records: list[dict], source_type: str) -> list[dict]:
    """Batch convert records based on source type.

    source_type: 'nist_csrc', 'federal_register', 'ecfr',
                 'nist_sp800_171', 'nist_csf', or 'dod_documents'
    """
    converters = {
        "nist_csrc": convert_nist_record,
        "federal_register": convert_federal_register_record,
        "ecfr": convert_ecfr_record,
        "nist_sp800_171": convert_sp800_171_record,
        "nist_csf": convert_csf_record,
        "dod_documents": convert_dod_document_record,
    }

    converter = converters.get(source_type)
    if not converter:
        raise ValueError(f"Unknown source type: {source_type}")

    results = []
    for record in records:
        try:
            result = converter(record)
            if isinstance(result, list):
                results.extend(result)
            else:
                results.append(result)
        except Exception as e:
            logger.warning("Failed to convert record: %s", e)

    return results
