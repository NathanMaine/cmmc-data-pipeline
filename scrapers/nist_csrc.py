"""Scraper for NIST SP 800-53 controls and related publications.

Uses the OSCAL catalog JSON from the NIST GitHub repository (the old
CSRC JSON API was retired). Also fetches the CSV control catalog from
the NIST CSRC downloads page as a fallback.
"""

import csv
import io
import logging
import re
from pathlib import Path

from rich.console import Console

from .base import BaseScraper

console = Console()
logger = logging.getLogger(__name__)

# OSCAL catalog JSON on GitHub — the authoritative machine-readable source
OSCAL_CATALOG_URL = (
    "https://raw.githubusercontent.com/usnistgov/oscal-content"
    "/main/nist.gov/SP800-53/rev5/json/NIST_SP-800-53_rev5_catalog.json"
)

# CSV fallback from NIST downloads page
CSV_CATALOG_URL = (
    "https://csrc.nist.gov/CSRC/media/Projects/risk-management"
    "/800-53%20Downloads/800-53r5/NIST_SP-800-53_rev5_catalog_load.csv"
)


class NISTCSRCScraper(BaseScraper):
    """Scrapes NIST SP 800-53 controls from the OSCAL catalog."""

    def get_source_name(self) -> str:
        return "nist_csrc"

    def scrape(self) -> list[dict]:
        """Scrape all SP 800-53 controls."""
        records = self._scrape_oscal_catalog()
        if not records:
            console.print("  [yellow]OSCAL catalog failed, falling back to CSV...[/yellow]")
            records = self._scrape_csv_catalog()
        return records

    def scrape_incremental(self, since_date: str) -> list[dict]:
        """SP 800-53 controls don't change frequently.

        Re-scrape the full catalog and let the dedup stage handle it.
        New revisions (e.g. 5.1 → 5.2) will naturally produce new records.
        """
        return self.scrape()

    # ── Primary: OSCAL JSON from GitHub ─────────────────────────────

    def _scrape_oscal_catalog(self) -> list[dict]:
        """Fetch and parse the OSCAL SP 800-53 Rev 5 catalog JSON."""
        url = self.config.get("oscal_catalog_url", OSCAL_CATALOG_URL)
        records = []

        console.print("  Fetching OSCAL SP 800-53 catalog from GitHub...")
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
            family_id = group.get("id", "").upper()
            family_title = group.get("title", "")
            controls = group.get("controls", [])

            for ctrl in controls:
                record = self._parse_oscal_control(ctrl, family_id)
                if record:
                    records.append(record)

                # Also get control enhancements (sub-controls)
                for enhancement in ctrl.get("controls", []):
                    record = self._parse_oscal_control(enhancement, family_id)
                    if record:
                        records.append(record)

        console.print(f"  Extracted {len(records)} controls + enhancements")
        return records

    def _parse_oscal_control(self, ctrl: dict, family_id: str) -> dict | None:
        """Parse a single OSCAL control into a training record."""
        ctrl_id = ctrl.get("id", "").upper().replace("-", "-")
        title = ctrl.get("title", "")

        # Extract statement text from parts
        statement = self._extract_parts_text(ctrl.get("parts", []), "statement")
        guidance = self._extract_parts_text(ctrl.get("parts", []), "guidance")

        if not statement and not guidance:
            return None

        content = f"{ctrl_id} — {title}\n\n"
        if statement:
            content += f"Control Statement:\n{statement}\n\n"
        if guidance:
            content += f"Supplemental Guidance:\n{guidance}"

        content = content.strip()
        if len(content) < 50:
            return None

        return {
            "text": content,
            "source": f"NIST SP 800-53 Rev. 5 — {ctrl_id}",
            "title": f"{ctrl_id} {title}",
            "control_id": ctrl_id,
            "family": family_id,
            "date": "2024-11-07",  # Rev 5.1.1 date
            "url": f"https://csrc.nist.gov/projects/cprt/catalog#/cprt/framework/version/SP_800_53_5_1_1/home?element={ctrl_id}",
            "scraper": "nist_csrc",
        }

    def _extract_parts_text(self, parts: list, part_name: str) -> str:
        """Recursively extract prose text from OSCAL parts."""
        texts = []
        for part in parts:
            if part.get("name") == part_name or part.get("id", "").endswith(f"_smt"):
                prose = part.get("prose", "")
                if prose:
                    # Clean OSCAL markup: {{ insert: param, ac-1_prm_1 }} → [parameter]
                    prose = re.sub(
                        r"\{\{\s*insert:\s*param,\s*[\w.\-]+\s*\}\}",
                        "[organization-defined parameter]",
                        prose,
                    )
                    texts.append(prose)

                # Recurse into sub-parts (lettered items a, b, c, ...)
                for sub in part.get("parts", []):
                    sub_prose = sub.get("prose", "")
                    if sub_prose:
                        label = sub.get("props", [{}])
                        label_val = ""
                        for p in sub.get("props", []):
                            if p.get("name") == "label":
                                label_val = p.get("value", "")
                        prefix = f"{label_val} " if label_val else ""
                        sub_prose = re.sub(
                            r"\{\{\s*insert:\s*param,\s*[\w.\-]+\s*\}\}",
                            "[organization-defined parameter]",
                            sub_prose,
                        )
                        texts.append(f"{prefix}{sub_prose}")

                        # Third level (e.g. a.1, a.2)
                        for subsub in sub.get("parts", []):
                            ss_prose = subsub.get("prose", "")
                            if ss_prose:
                                ss_label = ""
                                for p in subsub.get("props", []):
                                    if p.get("name") == "label":
                                        ss_label = p.get("value", "")
                                ss_prefix = f"  {ss_label} " if ss_label else "  "
                                ss_prose = re.sub(
                                    r"\{\{\s*insert:\s*param,\s*[\w.\-]+\s*\}\}",
                                    "[organization-defined parameter]",
                                    ss_prose,
                                )
                                texts.append(f"{ss_prefix}{ss_prose}")

        return "\n".join(texts)

    # ── Fallback: CSV from NIST downloads ───────────────────────────

    def _scrape_csv_catalog(self) -> list[dict]:
        """Fallback: parse the CSV control catalog from NIST downloads."""
        url = self.config.get("csv_catalog_url", CSV_CATALOG_URL)
        records = []

        console.print("  Fetching SP 800-53 CSV catalog...")
        try:
            resp = self._request(url)
            text = resp.text
        except Exception as e:
            logger.error("Failed to fetch CSV catalog: %s", e)
            return records

        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            identifier = row.get("identifier", "").strip()
            name = row.get("name", "").strip()
            control_text = row.get("control_text", "").strip()
            discussion = row.get("discussion", "").strip()

            if not identifier or not control_text:
                continue

            content = f"{identifier} — {name}\n\n{control_text}"
            if discussion:
                content += f"\n\nDiscussion:\n{discussion}"

            if len(content) < 50:
                continue

            family = identifier.split("-")[0] if "-" in identifier else ""

            records.append({
                "text": content,
                "source": f"NIST SP 800-53 Rev. 5 — {identifier}",
                "title": f"{identifier} {name}",
                "control_id": identifier,
                "family": family,
                "date": "2024-11-07",
                "url": f"https://csrc.nist.gov/projects/cprt/catalog#/cprt/framework/version/SP_800_53_5_1_1/home?element={identifier}",
                "scraper": "nist_csrc",
            })

        console.print(f"  Extracted {len(records)} controls from CSV")
        return records
