"""Scraper for NIST SP 800-171 Revision 3 CUI security controls.

Uses the OSCAL catalog JSON from the NIST GitHub repository.  SP 800-171
defines the 110 security requirements that protect Controlled Unclassified
Information (CUI) in non-federal systems — the technical backbone of
CMMC Level 2.
"""

import logging
import re
from pathlib import Path

from rich.console import Console

from .base import BaseScraper

console = Console()
logger = logging.getLogger(__name__)

# OSCAL catalog JSON on GitHub — authoritative machine-readable source
OSCAL_CATALOG_URL = (
    "https://raw.githubusercontent.com/usnistgov/oscal-content"
    "/main/nist.gov/SP800-171/rev3/json/NIST_SP800-171_rev3_catalog.json"
)

# Regex to replace OSCAL parameter insertion markup with a readable placeholder
_PARAM_RE = re.compile(r"\{\{\s*insert:\s*param,\s*[\w.\-]+\s*\}\}")

# SP 800-171 Rev. 3 final publication date
_PUB_DATE = "2024-05-14"

# Canonical landing page
_PUB_URL = "https://csrc.nist.gov/pubs/sp/800/171/r3/final"


class NISTSP800171Scraper(BaseScraper):
    """Scrapes NIST SP 800-171 Rev. 3 controls from the OSCAL catalog."""

    def get_source_name(self) -> str:
        return "nist_sp800_171"

    def scrape(self) -> list[dict]:
        """Scrape all SP 800-171 Rev. 3 controls."""
        return self._scrape_oscal_catalog()

    def scrape_incremental(self, since_date: str) -> list[dict]:
        """SP 800-171 controls are static between revisions.

        Re-scrape the full catalog and let the dedup stage handle it.
        """
        return self.scrape()

    # ── OSCAL JSON parsing ───────────────────────────────────────────

    def _scrape_oscal_catalog(self) -> list[dict]:
        """Fetch and parse the OSCAL SP 800-171 Rev. 3 catalog JSON."""
        url = self.config.get("oscal_catalog_url", OSCAL_CATALOG_URL)
        records: list[dict] = []

        console.print("  Fetching OSCAL SP 800-171 Rev. 3 catalog from GitHub...")
        try:
            resp = self._request(url)
            data = resp.json()
        except Exception as e:
            logger.error("Failed to fetch OSCAL catalog: %s", e)
            return records

        catalog = data.get("catalog", data)
        groups = catalog.get("groups", [])
        console.print(f"  Found {len(groups)} control families")

        for group in groups:
            family_title = group.get("title", "")
            family_oscal_id = group.get("id", "")  # e.g. "SP_800_171_03.01"
            family_id = self._oscal_id_to_control_id(family_oscal_id)

            for ctrl in group.get("controls", []):
                record = self._parse_control(ctrl, family_title, family_id)
                if record:
                    records.append(record)

        console.print(f"  Extracted {len(records)} controls")
        return records

    def _parse_control(
        self, ctrl: dict, family_title: str, family_id: str
    ) -> dict | None:
        """Parse a single OSCAL control into a training record."""
        oscal_id = ctrl.get("id", "")
        control_id = self._oscal_id_to_control_id(oscal_id)
        title = ctrl.get("title", "")

        # Get the human-readable label from props (e.g. "Account Management (03.01.01)")
        label = self._get_prop(ctrl, "label") or title

        parts = ctrl.get("parts", [])
        params = ctrl.get("params", [])

        # Skip controls with no substantive parts (family-level placeholders)
        if not parts:
            return None

        # ── Build the rich text content ──────────────────────────────

        sections: list[str] = []

        # Header
        sections.append(f"{control_id} — {title}")

        # Control statement
        statement = self._extract_statement(parts)
        if statement:
            sections.append(f"Control Statement:\n{statement}")

        # Supplemental guidance
        guidance = self._extract_prose_by_name(parts, "guidance")
        if guidance:
            sections.append(f"Supplemental Guidance:\n{guidance}")

        # Assessment objectives
        objectives = self._extract_assessment_objectives(parts)
        if objectives:
            sections.append(f"Assessment Objectives:\n{objectives}")

        # Organization-Defined Parameters
        odp_text = self._extract_odps(params)
        if odp_text:
            sections.append(f"Organization-Defined Parameters:\n{odp_text}")

        # Assessment methods
        methods = self._extract_assessment_methods(parts)
        if methods:
            sections.append(f"Assessment Methods:\n{methods}")

        content = "\n\n".join(sections).strip()

        if len(content) < 50:
            return None

        return {
            "text": content,
            "source": f"NIST SP 800-171 Rev. 3 — {control_id}",
            "title": f"{control_id} {title}",
            "control_id": control_id,
            "family": family_title,
            "family_id": family_id,
            "date": _PUB_DATE,
            "url": _PUB_URL,
            "scraper": "nist_sp800_171",
        }

    # ── Part extraction helpers ──────────────────────────────────────

    def _extract_statement(self, parts: list) -> str:
        """Extract the control statement, recursing into sub-items."""
        for part in parts:
            if part.get("name") == "statement":
                return self._render_part_tree(part)
        return ""

    def _render_part_tree(self, part: dict, depth: int = 0) -> str:
        """Recursively render a part and its children with labels."""
        lines: list[str] = []
        prose = self._clean_prose(part.get("prose", ""))
        label = self._get_prop(part, "label")

        if prose:
            indent = "  " * depth
            prefix = f"{label} " if label else ""
            lines.append(f"{indent}{prefix}{prose}")

        for child in part.get("parts", []):
            if child.get("name") == "item":
                lines.append(self._render_part_tree(child, depth + 1))

        return "\n".join(lines)

    def _extract_prose_by_name(self, parts: list, name: str) -> str:
        """Extract and clean the prose from all top-level parts matching name."""
        texts: list[str] = []
        for part in parts:
            if part.get("name") == name:
                prose = self._clean_prose(part.get("prose", ""))
                if prose:
                    texts.append(prose)
        return "\n".join(texts)

    def _extract_assessment_objectives(self, parts: list) -> str:
        """Extract assessment objectives (800-171A) from the catalog."""
        lines: list[str] = []
        for part in parts:
            if part.get("name") == "assessment-objective":
                # Top-level objective may have prose
                prose = self._clean_prose(part.get("prose", ""))
                if prose:
                    lines.append(f"- {prose}")
                # Sub-objectives
                for child in part.get("parts", []):
                    child_prose = self._clean_prose(child.get("prose", ""))
                    if child_prose:
                        lines.append(f"- {child_prose}")
                    # Third-level objectives
                    for grandchild in child.get("parts", []):
                        gc_prose = self._clean_prose(grandchild.get("prose", ""))
                        if gc_prose:
                            lines.append(f"  - {gc_prose}")
        return "\n".join(lines)

    def _extract_assessment_methods(self, parts: list) -> str:
        """Extract assessment methods (examine, interview, test)."""
        lines: list[str] = []
        for part in parts:
            if part.get("name") == "assessment-method":
                method_label = self._get_prop(part, "label") or ""
                prose = self._clean_prose(part.get("prose", ""))
                # Collect objects listed in sub-parts
                objects: list[str] = []
                for child in part.get("parts", []):
                    child_prose = self._clean_prose(child.get("prose", ""))
                    if child_prose:
                        objects.append(child_prose)
                obj_str = "; ".join(objects) if objects else ""
                # Build a single line per method, e.g. "Examine: policy docs; procedures..."
                if method_label and obj_str:
                    lines.append(f"{method_label}: {obj_str}")
                elif method_label and prose:
                    lines.append(f"{method_label}: {prose}")
                elif prose:
                    lines.append(prose)
        return "\n".join(lines)

    def _extract_odps(self, params: list) -> str:
        """Extract Organization-Defined Parameters from the control."""
        lines: list[str] = []
        for param in params:
            param_id = param.get("id", "")
            label = param.get("label", "")
            usage = param.get("usage", "")
            # Get guideline prose if available
            guidelines = param.get("guidelines", [])
            guideline_prose = ""
            for g in guidelines:
                p = g.get("prose", "")
                if p:
                    guideline_prose = p
                    break

            # Build a human-readable description
            desc = label or guideline_prose or usage or param_id
            desc = self._clean_prose(desc)
            if desc:
                lines.append(f"- {param_id}: {desc}")
        return "\n".join(lines)

    # ── Utility methods ──────────────────────────────────────────────

    @staticmethod
    def _oscal_id_to_control_id(oscal_id: str) -> str:
        """Convert OSCAL id to human-readable control id.

        Example: "SP_800_171_03.01.01" → "03.01.01"
        """
        prefix = "SP_800_171_"
        if oscal_id.startswith(prefix):
            return oscal_id[len(prefix):]
        return oscal_id

    @staticmethod
    def _get_prop(obj: dict, name: str) -> str:
        """Get the value of a named property from an OSCAL object's props."""
        for prop in obj.get("props", []):
            if prop.get("name") == name:
                return prop.get("value", "")
        return ""

    @staticmethod
    def _clean_prose(text: str) -> str:
        """Clean OSCAL parameter markup from prose text."""
        if not text:
            return ""
        text = _PARAM_RE.sub("[organization-defined parameter]", text)
        return text.strip()
