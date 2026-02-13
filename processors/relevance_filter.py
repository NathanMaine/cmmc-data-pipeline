"""Relevance filtering for source-specific content.

Removes records that are technically within a scraped CFR title/part
but aren't actually relevant to CMMC/cybersecurity compliance.
"""

import logging
import re
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# DFARS clauses relevant to CMMC / cybersecurity / CUI
RELEVANT_DFARS_PREFIXES = [
    "252.204-7008",  # Compliance with safeguarding CDI controls
    "252.204-7009",  # Limitations on use/disclosure of third-party cyber incident info
    "252.204-7012",  # Safeguarding CDI and Cyber Incident Reporting
    "252.204-7019",  # Notice of NIST SP 800-171 DoD Assessment Requirements
    "252.204-7020",  # NIST SP 800-171 DoD Assessment Requirements
    "252.204-7021",  # Contractor Compliance with CMMC Level Requirements
    "252.204-7024",  # Notice of CMMC Assessment and Scoping Requirements
    "252.204-7025",  # Notice of CMMC Level Requirements
    "252.239-7009",  # Representation of use of cloud computing
    "252.239-7010",  # Cloud Computing Services
]


@dataclass
class RelevanceStats:
    total: int = 0
    kept: int = 0
    removed_irrelevant: int = 0


def _extract_dfars_clause(title: str) -> str:
    """Extract the DFARS clause number from a record title like '252.204-7012 Safeguarding...'"""
    match = re.match(r"(252\.\d+-\d+)", title)
    return match.group(1) if match else ""


def is_relevant_ecfr(raw_record: dict) -> bool:
    """Check if an eCFR record is relevant to CMMC/cybersecurity.

    - 32 CFR 170 (CMMC rule): always relevant
    - 45 CFR 164 (HIPAA Security Rule): always relevant
    - 48 CFR 252 (DFARS): only specific cyber/CMMC clauses are relevant
    """
    cfr_title = raw_record.get("cfr_title")
    cfr_part = raw_record.get("cfr_part")

    # 32 CFR 170 — CMMC program rule
    if cfr_title == 32 and cfr_part == 170:
        return True

    # 45 CFR 164 — HIPAA Security Rule
    if cfr_title == 45 and cfr_part == 164:
        return True

    # 48 CFR 252 — DFARS clauses (only cyber-relevant ones)
    if cfr_title == 48 and cfr_part == 252:
        title = raw_record.get("title", "")
        clause = _extract_dfars_clause(title)
        return any(clause.startswith(prefix) for prefix in RELEVANT_DFARS_PREFIXES)

    # Unknown CFR reference — keep by default
    return True


def filter_relevance(raw_records: list[dict], source_name: str) -> tuple[list[dict], RelevanceStats]:
    """Apply relevance filtering to raw records based on source.

    Currently only filters eCFR records. Other sources pass through unchanged.
    """
    stats = RelevanceStats(total=len(raw_records))

    if source_name != "ecfr":
        stats.kept = len(raw_records)
        return raw_records, stats

    kept = []
    for record in raw_records:
        if is_relevant_ecfr(record):
            kept.append(record)
            stats.kept += 1
        else:
            stats.removed_irrelevant += 1

    return kept, stats
