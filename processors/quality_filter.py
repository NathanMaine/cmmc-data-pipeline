"""Quality filtering for training data records."""

import re
from dataclasses import dataclass, field


@dataclass
class FilterConfig:
    min_content_length: int = 100
    min_answer_length: int = 200
    max_answer_length: int = 8000
    max_table_ratio: float = 0.3
    min_alpha_ratio: float = 0.3
    max_image_artifacts: int = 2


@dataclass
class FilterStats:
    total: int = 0
    passed: int = 0
    rejected_too_short: int = 0
    rejected_too_long: int = 0
    rejected_table_heavy: int = 0
    rejected_low_alpha: int = 0
    rejected_section_numbers_only: int = 0
    rejected_table_borders_only: int = 0
    rejected_image_artifacts: int = 0


def filter_record(text: str, config: FilterConfig = None, min_length: int = None) -> tuple[bool, str]:
    """Check if a text record passes quality filters.

    Returns (passed, rejection_reason).
    """
    if config is None:
        config = FilterConfig()

    length_threshold = min_length or config.min_content_length

    if len(text) < length_threshold:
        return False, "too_short"

    if config.max_answer_length and len(text) > config.max_answer_length:
        return False, "too_long"

    # Section numbers only
    if re.match(r"^\s*[\d\.]+\s*$", text.strip()):
        return False, "section_numbers_only"

    # Table borders only
    if re.match(r"^[\s\|_\-=]+$", text.strip()):
        return False, "table_borders_only"

    # Table character ratio
    table_chars = text.count("|") + text.count("---") * 3 + text.count("===") * 3
    if len(text) > 0 and table_chars / len(text) > config.max_table_ratio:
        return False, "table_heavy"

    # Alpha character ratio
    alpha_count = sum(1 for c in text if c.isalpha())
    if len(text) > 0 and alpha_count / len(text) < config.min_alpha_ratio:
        return False, "low_alpha"

    # Image artifacts
    if text.count("<!-- image -->") > config.max_image_artifacts:
        return False, "image_artifacts"

    return True, ""


def filter_batch(records: list[dict], config: FilterConfig = None, content_key: str = "text") -> tuple[list[dict], FilterStats]:
    """Filter a batch of records, returning (passed_records, stats)."""
    if config is None:
        config = FilterConfig()

    stats = FilterStats()
    passed = []

    for record in records:
        stats.total += 1
        text = record.get(content_key, "")
        ok, reason = filter_record(text, config)

        if ok:
            stats.passed += 1
            passed.append(record)
        else:
            attr = f"rejected_{reason}"
            if hasattr(stats, attr):
                setattr(stats, attr, getattr(stats, attr) + 1)

    return passed, stats
