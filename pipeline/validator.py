"""Data validation pipeline for quality assurance before merge."""

import json
import logging
import random
from dataclasses import dataclass, field
from pathlib import Path

from rich.console import Console
from rich.table import Table

console = Console()
logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    passed: bool = True
    total_records: int = 0
    format_errors: list[str] = field(default_factory=list)
    quality_warnings: list[str] = field(default_factory=list)
    comparison_notes: list[str] = field(default_factory=list)
    stats: dict = field(default_factory=dict)

    def summary(self) -> str:
        status = "PASSED" if self.passed else "FAILED"
        parts = [f"Validation {status}: {self.total_records} records"]
        if self.format_errors:
            parts.append(f"  Format errors: {len(self.format_errors)}")
        if self.quality_warnings:
            parts.append(f"  Quality warnings: {len(self.quality_warnings)}")
        return "\n".join(parts)


class DataValidator:
    """Validates processed data before merge into training set."""

    def __init__(self, config: dict = None):
        self.config = config or {}
        self.min_records = self.config.get("min_records", 10)
        self.max_quality_drop = self.config.get("max_quality_drop_pct", 5)
        self.min_avg_length = self.config.get("min_avg_answer_length", 200)
        self.max_avg_length = self.config.get("max_avg_answer_length", 5000)
        self.required_system_prompt_substring = self.config.get(
            "required_system_prompt", "CMMC"
        )

    def validate_all(self, records: list[dict], existing_path: str = None) -> ValidationResult:
        """Run all validation checks."""
        result = ValidationResult(total_records=len(records))

        # Format validation
        self._validate_format(records, result)

        # Quality validation
        self._validate_quality(records, result)

        # Comparison against existing data
        if existing_path:
            self._validate_against_existing(records, existing_path, result)

        # Determine pass/fail
        result.passed = len(result.format_errors) == 0
        return result

    def _validate_format(self, records: list[dict], result: ValidationResult):
        """Validate chat message format."""
        for i, record in enumerate(records):
            messages = record.get("messages")
            if not isinstance(messages, list):
                result.format_errors.append(f"Record {i}: 'messages' is not a list")
                continue

            if len(messages) < 3:
                result.format_errors.append(f"Record {i}: needs >= 3 messages, got {len(messages)}")
                continue

            # Check roles
            expected_roles = ["system", "user", "assistant"]
            for j, expected_role in enumerate(expected_roles):
                if j >= len(messages):
                    break
                actual_role = messages[j].get("role", "")
                if actual_role != expected_role:
                    result.format_errors.append(
                        f"Record {i}: message {j} role is '{actual_role}', expected '{expected_role}'"
                    )

            # Check content not empty
            for j, msg in enumerate(messages):
                content = msg.get("content", "")
                if not content or not content.strip():
                    result.format_errors.append(f"Record {i}: message {j} has empty content")

            # Check system prompt contains expected keyword
            system_content = messages[0].get("content", "")
            if self.required_system_prompt_substring not in system_content:
                result.quality_warnings.append(
                    f"Record {i}: system prompt missing '{self.required_system_prompt_substring}'"
                )

            # Check source field
            if not record.get("source"):
                result.quality_warnings.append(f"Record {i}: missing 'source' field")

    def _validate_quality(self, records: list[dict], result: ValidationResult):
        """Validate content quality metrics."""
        if len(records) < self.min_records:
            result.format_errors.append(
                f"Too few records: {len(records)} < minimum {self.min_records}"
            )
            return

        # Answer length stats
        answer_lengths = []
        for record in records:
            messages = record.get("messages", [])
            if len(messages) >= 3:
                answer_lengths.append(len(messages[2].get("content", "")))

        if answer_lengths:
            avg_len = sum(answer_lengths) / len(answer_lengths)
            min_len = min(answer_lengths)
            max_len = max(answer_lengths)

            result.stats["avg_answer_length"] = round(avg_len)
            result.stats["min_answer_length"] = min_len
            result.stats["max_answer_length"] = max_len

            if avg_len < self.min_avg_length:
                result.quality_warnings.append(
                    f"Average answer length ({avg_len:.0f}) below threshold ({self.min_avg_length})"
                )
            if avg_len > self.max_avg_length:
                result.quality_warnings.append(
                    f"Average answer length ({avg_len:.0f}) above threshold ({self.max_avg_length})"
                )

        # Source diversity
        sources = set()
        for record in records:
            sources.add(record.get("source", "unknown"))
        result.stats["unique_sources"] = len(sources)
        result.stats["source_list"] = sorted(sources)[:20]

    def _validate_against_existing(self, records: list[dict], existing_path: str, result: ValidationResult):
        """Compare new data against existing training data."""
        path = Path(existing_path)
        existing_records = []

        for jsonl_file in ["train.jsonl", "validation.jsonl"]:
            filepath = path / jsonl_file
            if not filepath.exists():
                continue
            with open(filepath) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        existing_records.append(json.loads(line))

        if not existing_records:
            result.comparison_notes.append("No existing training data found for comparison")
            return

        # Compare answer length distributions
        existing_lengths = []
        for record in existing_records:
            messages = record.get("messages", [])
            if len(messages) >= 3:
                existing_lengths.append(len(messages[2].get("content", "")))

        new_lengths = []
        for record in records:
            messages = record.get("messages", [])
            if len(messages) >= 3:
                new_lengths.append(len(messages[2].get("content", "")))

        if existing_lengths and new_lengths:
            existing_avg = sum(existing_lengths) / len(existing_lengths)
            new_avg = sum(new_lengths) / len(new_lengths)

            result.stats["existing_avg_length"] = round(existing_avg)
            result.stats["new_avg_length"] = round(new_avg)

            pct_diff = abs(new_avg - existing_avg) / existing_avg * 100
            if pct_diff > self.max_quality_drop:
                result.quality_warnings.append(
                    f"Average answer length differs by {pct_diff:.1f}% from existing data "
                    f"(existing: {existing_avg:.0f}, new: {new_avg:.0f})"
                )

        result.comparison_notes.append(
            f"Compared against {len(existing_records)} existing records"
        )
        result.stats["existing_record_count"] = len(existing_records)
        result.stats["addition_pct"] = round(len(records) / len(existing_records) * 100, 1)

    def spot_check(self, records: list[dict], n: int = 5) -> list[dict]:
        """Return n random records for manual review."""
        sample_size = min(n, len(records))
        samples = random.sample(records, sample_size)
        return samples

    def print_report(self, result: ValidationResult):
        """Print a formatted validation report."""
        table = Table(title="Validation Report")
        table.add_column("Metric", style="bold")
        table.add_column("Value")

        table.add_row("Status", "[green]PASSED[/green]" if result.passed else "[red]FAILED[/red]")
        table.add_row("Total Records", str(result.total_records))
        table.add_row("Format Errors", str(len(result.format_errors)))
        table.add_row("Quality Warnings", str(len(result.quality_warnings)))

        for key, value in result.stats.items():
            if key != "source_list":
                table.add_row(key, str(value))

        console.print(table)

        if result.format_errors:
            console.print("\n[red]Format Errors:[/red]")
            for err in result.format_errors[:10]:
                console.print(f"  - {err}")
            if len(result.format_errors) > 10:
                console.print(f"  ... and {len(result.format_errors) - 10} more")

        if result.quality_warnings:
            console.print("\n[yellow]Quality Warnings:[/yellow]")
            for warn in result.quality_warnings[:10]:
                console.print(f"  - {warn}")

        if result.comparison_notes:
            console.print("\n[blue]Comparison Notes:[/blue]")
            for note in result.comparison_notes:
                console.print(f"  - {note}")
