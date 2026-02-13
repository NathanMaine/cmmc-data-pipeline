"""Scraper for NIST Cybersecurity Framework (CSF) v2.0.

Uses the OSCAL catalog JSON from the NIST GitHub repository.
Produces two record types:
  - Category-level records (34) — function category with all subcategory summaries
  - Subcategory-level records (185) — individual requirements with implementation examples
"""

import logging
import re

from rich.console import Console

from .base import BaseScraper

console = Console()
logger = logging.getLogger(__name__)

# OSCAL catalog JSON on GitHub — authoritative machine-readable source
OSCAL_CATALOG_URL = (
    "https://raw.githubusercontent.com/usnistgov/oscal-content"
    "/main/nist.gov/CSF/v2.0/json/NIST_CSF_v2.0_catalog.json"
)

# Publication date for CSF 2.0 final release
CSF_DATE = "2024-02-26"

# Landing page for the framework
CSF_URL = "https://csrc.nist.gov/pubs/cswp/29/the-nist-cybersecurity-framework-csf-20/final"


class NISTCSFScraper(BaseScraper):
    """Scrapes NIST CSF 2.0 functions, categories, and subcategories."""

    def get_source_name(self) -> str:
        return "nist_csf"

    def scrape(self) -> list[dict]:
        """Scrape all CSF 2.0 categories and subcategories."""
        url = self.config.get("oscal_catalog_url", OSCAL_CATALOG_URL)

        console.print("  Fetching OSCAL CSF 2.0 catalog from GitHub...")
        try:
            resp = self._request(url)
            data = resp.json()
        except Exception as e:
            logger.error("Failed to fetch OSCAL CSF catalog: %s", e)
            return []

        catalog = data.get("catalog", data)
        groups = catalog.get("groups", [])
        console.print(f"  Found {len(groups)} CSF functions")

        records = []
        category_count = 0
        subcategory_count = 0

        for group in groups:
            function_id = group.get("id", "").upper()
            function_title = group.get("title", "")
            categories = group.get("controls", [])

            for category in categories:
                cat_records, sub_records = self._parse_category(
                    category, function_id, function_title
                )
                if cat_records:
                    records.append(cat_records)
                    category_count += 1
                records.extend(sub_records)
                subcategory_count += len(sub_records)

        console.print(
            f"  Extracted {category_count} categories + "
            f"{subcategory_count} subcategories = {len(records)} total records"
        )
        return records

    def scrape_incremental(self, since_date: str) -> list[dict]:
        """CSF 2.0 is a static publication.

        Re-scrape the full catalog and let the dedup stage handle it.
        """
        return self.scrape()

    # ── Parsing helpers ──────────────────────────────────────────────

    def _parse_category(
        self, category: dict, function_id: str, function_title: str
    ) -> tuple[dict | None, list[dict]]:
        """Parse a category control and its subcategory children.

        Returns a (category_record, [subcategory_records]) tuple.
        """
        cat_id = category.get("id", "").upper()
        cat_title = category.get("title", "")

        # Extract category-level statement prose
        cat_statement = self._extract_prose(category.get("parts", []), "statement")

        # Parse all subcategories
        subcategories = category.get("controls", [])
        sub_records = []
        sub_summaries = []

        for subcat in subcategories:
            sub_record = self._parse_subcategory(
                subcat, cat_id, function_id, function_title
            )
            if sub_record:
                sub_records.append(sub_record)

            # Build a one-line summary for the category record regardless
            sub_id = subcat.get("id", "").upper()
            sub_statement = self._extract_prose(subcat.get("parts", []), "statement")
            if sub_statement:
                sub_summaries.append(f"{sub_id}: {sub_statement}")

        # Build category-level record text
        text_parts = [f"{cat_id} — {cat_title}"]
        if cat_statement:
            text_parts.append(f"\n{cat_statement}")
        if sub_summaries:
            text_parts.append("\nSubcategories:")
            text_parts.extend(sub_summaries)

        text = "\n".join(text_parts).strip()

        cat_record = None
        if len(text) >= 50:
            cat_record = {
                "text": text,
                "source": f"NIST CSF 2.0 — {cat_id} {cat_title}",
                "title": f"{cat_id} {cat_title}",
                "category_id": cat_id,
                "function": function_title,
                "function_id": function_id,
                "date": CSF_DATE,
                "url": CSF_URL,
                "scraper": "nist_csf",
            }

        return cat_record, sub_records

    def _parse_subcategory(
        self,
        subcat: dict,
        category_id: str,
        function_id: str,
        function_title: str,
    ) -> dict | None:
        """Parse a single subcategory control into a training record."""
        sub_id = subcat.get("id", "").upper()

        parts = subcat.get("parts", [])
        statement = self._extract_prose(parts, "statement")
        example = self._extract_prose(parts, "example")

        if not statement:
            return None

        # Build text content
        text_parts = [sub_id, f"\n{statement}"]
        if example:
            text_parts.append(f"\nImplementation Example:\n{example}")

        text = "\n".join(text_parts).strip()

        if len(text) < 50:
            return None

        return {
            "text": text,
            "source": f"NIST CSF 2.0 — {sub_id}",
            "title": sub_id,
            "subcategory_id": sub_id,
            "category_id": category_id,
            "function": function_title,
            "function_id": function_id,
            "date": CSF_DATE,
            "url": CSF_URL,
            "scraper": "nist_csf",
        }

    def _extract_prose(self, parts: list, part_name: str) -> str:
        """Extract prose text from OSCAL parts matching the given name.

        Matches on either the ``name`` field equaling *part_name* or the
        ``id`` field ending with ``_{part_name}`` (OSCAL uses both
        conventions depending on the catalog).
        """
        texts = []
        for part in parts:
            name_match = part.get("name") == part_name
            id_match = part.get("id", "").endswith(f"_{part_name}")

            if name_match or id_match:
                prose = part.get("prose", "")
                if prose:
                    # Clean OSCAL parameter insertions
                    prose = re.sub(
                        r"\{\{\s*insert:\s*param,\s*[\w.\-]+\s*\}\}",
                        "[organization-defined parameter]",
                        prose,
                    )
                    texts.append(prose.strip())

                # Recurse into nested sub-parts
                for sub in part.get("parts", []):
                    sub_prose = sub.get("prose", "")
                    if sub_prose:
                        sub_prose = re.sub(
                            r"\{\{\s*insert:\s*param,\s*[\w.\-]+\s*\}\}",
                            "[organization-defined parameter]",
                            sub_prose,
                        )
                        # Use label if present (e.g. "a.", "1.")
                        label = self._get_label(sub)
                        prefix = f"{label} " if label else ""
                        texts.append(f"{prefix}{sub_prose.strip()}")

        return "\n".join(texts)

    @staticmethod
    def _get_label(part: dict) -> str:
        """Extract the label property from an OSCAL part, if present."""
        for prop in part.get("props", []):
            if prop.get("name") == "label":
                return prop.get("value", "")
        return ""
