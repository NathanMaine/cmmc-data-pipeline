"""Scraper for the Federal Register API."""

import logging
import re
from pathlib import Path

from bs4 import BeautifulSoup
from rich.console import Console

from .base import BaseScraper

console = Console()
logger = logging.getLogger(__name__)


class FederalRegisterScraper(BaseScraper):
    """Scrapes the Federal Register for CMMC/DFARS/NIST documents."""

    def get_source_name(self) -> str:
        return "federal_register"

    def scrape(self) -> list[dict]:
        """Full scrape of all relevant Federal Register documents."""
        return self._search_documents(since_date=None)

    def scrape_incremental(self, since_date: str) -> list[dict]:
        """Scrape documents published after the given date."""
        return self._search_documents(since_date=since_date)

    def _search_documents(self, since_date: str = None) -> list[dict]:
        """Search Federal Register for relevant documents."""
        base_url = self.config.get("base_url", "https://www.federalregister.gov/api/v1")
        search_terms = self.config.get("search_terms", ["CMMC"])
        agencies = self.config.get("agencies", [])

        all_records = []
        seen_numbers = set()

        for term in search_terms:
            console.print(f"  Searching: '{term}'")
            params = {
                "conditions[term]": term,
                "per_page": 100,
                "order": "newest",
                "fields[]": [
                    "title", "abstract", "document_number", "publication_date",
                    "type", "agencies", "body_html_url", "html_url",
                ],
            }
            for agency in agencies:
                params.setdefault("conditions[agencies][]", []).append(agency)

            if since_date:
                params["conditions[publication_date][gte]"] = since_date

            page_url = f"{base_url}/documents.json"

            while page_url:
                try:
                    resp = self._request(page_url, params=params)
                    data = resp.json()
                except Exception as e:
                    logger.error("Failed to search '%s': %s", term, e)
                    break

                results = data.get("results", [])
                for doc in results:
                    doc_num = doc.get("document_number", "")
                    if doc_num in seen_numbers:
                        continue
                    seen_numbers.add(doc_num)

                    record = self._process_document(doc)
                    if record:
                        all_records.append(record)

                # Pagination
                page_url = data.get("next_page_url")
                params = None  # URL already contains params

                if not results:
                    break

            console.print(f"    Found {len(seen_numbers)} documents so far")

        console.print(f"  Total: {len(all_records)} documents")
        return all_records

    def _process_document(self, doc: dict) -> dict:
        """Process a single Federal Register document."""
        title = doc.get("title", "")
        abstract = doc.get("abstract", "")
        doc_type = doc.get("type", "Document")
        pub_date = doc.get("publication_date", "")

        # Fetch full body text
        body_url = doc.get("body_html_url")
        full_text = ""
        if body_url:
            try:
                resp = self._request(body_url)
                full_text = self._html_to_text(resp.text)
            except Exception as e:
                logger.warning("Failed to fetch body for %s: %s", title[:50], e)

        # Use abstract if no body
        content = full_text or abstract or ""
        if not content or len(content) < 100:
            return None

        # Split long documents into chunks
        chunks = self._chunk_text(content, max_chars=3000)

        agencies = [a.get("name", "") for a in doc.get("agencies", []) if isinstance(a, dict)]

        records = []
        for i, chunk in enumerate(chunks):
            record = {
                "text": chunk,
                "source": f"Federal Register — {title[:100]}",
                "title": title,
                "doc_type": doc_type,
                "date": pub_date,
                "document_number": doc.get("document_number", ""),
                "url": doc.get("html_url", ""),
                "agencies": agencies,
                "chunk_index": i,
                "total_chunks": len(chunks),
                "scraper": "federal_register",
            }
            records.append(record)

        return records[0] if len(records) == 1 else records

    def _html_to_text(self, html: str) -> str:
        """Convert HTML to clean text."""
        soup = BeautifulSoup(html, "lxml")

        # Remove script and style elements
        for tag in soup(["script", "style", "nav", "header", "footer"]):
            tag.decompose()

        # Get text with paragraph breaks
        text = soup.get_text(separator="\n\n")

        # Clean up whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]+", " ", text)
        return text.strip()

    def _chunk_text(self, text: str, max_chars: int = 3000) -> list[str]:
        """Split text into chunks at paragraph boundaries."""
        if len(text) <= max_chars:
            return [text]

        paragraphs = text.split("\n\n")
        chunks = []
        current = []
        current_len = 0

        for para in paragraphs:
            para = para.strip()
            if not para:
                continue
            if current_len + len(para) > max_chars and current:
                chunks.append("\n\n".join(current))
                current = [para]
                current_len = len(para)
            else:
                current.append(para)
                current_len += len(para)

        if current:
            chunks.append("\n\n".join(current))

        return chunks
