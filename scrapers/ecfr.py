"""Scraper for the Electronic Code of Federal Regulations (eCFR) API."""

import logging
import re
from pathlib import Path

from bs4 import BeautifulSoup
from rich.console import Console

from .base import BaseScraper

console = Console()
logger = logging.getLogger(__name__)


class ECFRScraper(BaseScraper):
    """Scrapes eCFR for CMMC, DFARS, and HIPAA regulatory text."""

    def get_source_name(self) -> str:
        return "ecfr"

    def scrape(self) -> list[dict]:
        """Full scrape of all configured CFR titles/parts."""
        records = []
        for title_cfg in self.config.get("titles", []):
            title_num = title_cfg["title"]
            for part_num in title_cfg.get("parts", []):
                part_records = self._scrape_part(title_num, part_num)
                records.extend(part_records)
        return records

    def scrape_incremental(self, since_date: str) -> list[dict]:
        """Check for amendments since the given date and re-scrape changed parts."""
        records = []
        for title_cfg in self.config.get("titles", []):
            title_num = title_cfg["title"]
            for part_num in title_cfg.get("parts", []):
                if self._has_amendments(title_num, part_num, since_date):
                    console.print(f"  [yellow]Amendment found: Title {title_num} Part {part_num}[/yellow]")
                    part_records = self._scrape_part(title_num, part_num)
                    records.extend(part_records)
                else:
                    console.print(f"  Title {title_num} Part {part_num}: no changes since {since_date}")
        return records

    def _has_amendments(self, title: int, part: int, since_date: str) -> bool:
        """Check if a part has been amended since the given date."""
        base_url = self.config.get("base_url", "https://www.ecfr.gov/api/versioner/v1")
        try:
            resp = self._request(f"{base_url}/versions/title-{title}", params={"part": part})
            versions = resp.json()
            version_list = versions.get("content_versions", versions.get("versions", []))
            for v in version_list:
                v_date = v.get("date", v.get("amendment_date", ""))
                if v_date >= since_date:
                    return True
        except Exception as e:
            logger.warning("Failed to check versions for Title %d Part %d: %s", title, part, e)
            # If we can't check, assume there are changes (safer)
            return True
        return False

    def _scrape_part(self, title: int, part: int) -> list[dict]:
        """Scrape all sections from a specific CFR title/part."""
        base_url = self.config.get("base_url", "https://www.ecfr.gov/api/versioner/v1")
        records = []
        cfr_label = self._cfr_label(title, part)
        console.print(f"  Scraping {cfr_label}...")

        # Get the structure first
        try:
            resp = self._request(
                f"{base_url}/structure/current/title-{title}.json",
                params={"part": part}
            )
            structure = resp.json()
        except Exception as e:
            logger.error("Failed to get structure for %s: %s", cfr_label, e)
            return records

        # Get the full rendered text
        try:
            resp = self._request(
                f"https://www.ecfr.gov/api/renderer/v1/content/enhanced/current/title-{title}",
                params={"part": part}
            )
            html = resp.text
        except Exception as e:
            logger.error("Failed to get content for %s: %s", cfr_label, e)
            return records

        # Parse HTML into sections
        sections = self._parse_sections(html, title, part)
        records.extend(sections)

        console.print(f"    Extracted {len(records)} sections from {cfr_label}")
        return records

    def _parse_sections(self, html: str, title: int, part: int) -> list[dict]:
        """Parse eCFR HTML into individual section records."""
        soup = BeautifulSoup(html, "lxml")
        records = []

        # Find section elements
        section_divs = soup.find_all("div", class_=re.compile(r"section"))
        if not section_divs:
            # Fallback: split by heading tags
            section_divs = soup.find_all(["h2", "h3", "h4"])

        for div in section_divs:
            # Extract section number and title
            heading = div.find(re.compile(r"h[1-6]")) if div.name == "div" else div
            if not heading:
                continue

            heading_text = heading.get_text(strip=True)
            section_match = re.match(r"§\s*([\d\.]+)\s*(.*)", heading_text)

            if section_match:
                section_num = section_match.group(1)
                section_title = section_match.group(2).strip(" .-—")
                cfr_ref = f"{title} CFR § {section_num}"
            else:
                section_num = ""
                section_title = heading_text
                cfr_ref = f"{title} CFR Part {part}"

            # Extract section body text
            if div.name == "div":
                body = div.get_text(separator="\n", strip=True)
            else:
                # Get text until next heading
                parts = []
                for sibling in heading.find_next_siblings():
                    if sibling.name and re.match(r"h[1-6]", sibling.name):
                        break
                    parts.append(sibling.get_text(separator="\n", strip=True))
                body = "\n".join(parts)

            # Clean up
            body = re.sub(r"\n{3,}", "\n\n", body)
            body = body.strip()

            if len(body) < 50:
                continue

            records.append({
                "text": body,
                "source": f"{cfr_ref} — {section_title}" if section_title else cfr_ref,
                "title": section_title,
                "cfr_ref": cfr_ref,
                "section_number": section_num,
                "cfr_title": title,
                "cfr_part": part,
                "date": "",  # Filled from version data if available
                "url": f"https://www.ecfr.gov/current/title-{title}/part-{part}",
                "scraper": "ecfr",
            })

        return records

    def _cfr_label(self, title: int, part: int) -> str:
        """Generate a human-readable CFR label."""
        labels = {
            (32, 170): "32 CFR Part 170 (CMMC)",
            (48, 252): "48 CFR Part 252 (DFARS)",
            (45, 164): "45 CFR Part 164 (HIPAA Security Rule)",
        }
        return labels.get((title, part), f"{title} CFR Part {part}")
