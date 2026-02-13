"""Scraper for DoD CMMC documents available only as PDFs.

Downloads PDFs from dodcio.defense.gov and NIST, extracts text with
pypdf, and chunks them into training records at paragraph boundaries.
"""

import io
import logging
import re
import tempfile
from pathlib import Path

from rich.console import Console

from .base import BaseScraper

try:
    from pypdf import PdfReader
except ImportError:
    raise ImportError(
        "pypdf is required for PDF scraping. Install it with: pip install pypdf"
    )

console = Console()
logger = logging.getLogger(__name__)

# Target chunk size in characters (~512 tokens at ~3-4 chars/token)
CHUNK_TARGET = 1500
CHUNK_MAX = 2000
CHUNK_MIN = 100

# Overlap: carry the last sentence of the previous chunk into the next
OVERLAP_SENTENCES = 1

DOD_DOCUMENTS = [
    {
        "name": "CMMC Assessment Guide Level 2",
        "url": "https://dodcio.defense.gov/Portals/0/Documents/CMMC/AssessmentGuideL2v2.pdf",
        "source_id": "cmmc_assessment_guide_l2",
        "date": "2025-08-01",
    },
    {
        "name": "CMMC Scoping Guide Level 2",
        "url": "https://dodcio.defense.gov/Portals/0/Documents/CMMC/ScopingGuideL2v2.pdf",
        "source_id": "cmmc_scoping_guide_l2",
        "date": "2025-08-01",
    },
    {
        "name": "CMMC Scoping Guide Level 3",
        "url": "https://dodcio.defense.gov/Portals/0/Documents/CMMC/ScopingGuideL3.pdf",
        "source_id": "cmmc_scoping_guide_l3",
        "date": "2024-12-16",
    },
    {
        "name": "DoD SP 800-171 Rev 3 Organization-Defined Parameters",
        "url": "https://dodcio.defense.gov/Portals/0/Documents/CMMC/OrgDefinedParmsNISTSP800-171.pdf",
        "source_id": "dod_odp_values",
        "date": "2025-04-15",
    },
    {
        "name": "NIST SP 800-172 Rev 3 Final Public Draft",
        "url": "https://nvlpubs.nist.gov/nistpubs/SpecialPublications/NIST.SP.800-172r3.fpd.pdf",
        "source_id": "nist_sp800_172_r3",
        "date": "2025-09-29",
    },
]

# Pattern for section headings: numbered sections, ALL CAPS, or Title Case lines
_HEADING_RE = re.compile(
    r"^("
    r"(?:Section|Chapter|CHAPTER|SECTION|Appendix|APPENDIX)\s+[\w.]+.*"
    r"|(?:\d+(?:\.\d+)*)\s+[A-Z].*"           # "3.1 Access Control"
    r"|[A-Z][A-Z\s\-:]{4,80}$"                 # ALL CAPS line (5-80 chars)
    r"|Table\s+\d+[-.]?\s+.*"                   # "Table 1. ..."
    r"|Figure\s+\d+[-.]?\s+.*"                  # "Figure 1. ..."
    r")$",
    re.MULTILINE,
)

# Lines that look like page numbers or running headers/footers
_PAGE_NUM_RE = re.compile(
    r"^\s*(?:Page\s+)?\d{1,4}\s*$"
    r"|^\s*\d{1,4}\s+of\s+\d{1,4}\s*$",
    re.MULTILINE | re.IGNORECASE,
)

# Repeated header/footer threshold — if a line appears on this many pages,
# treat it as a running header/footer
_HEADER_FOOTER_THRESHOLD = 3


class DoDDocumentScraper(BaseScraper):
    """Downloads DoD/NIST CMMC PDFs, extracts text, and chunks for training."""

    def get_source_name(self) -> str:
        return "dod_documents"

    def scrape(self) -> list[dict]:
        """Scrape all DoD CMMC PDF documents."""
        return self._process_documents(DOD_DOCUMENTS)

    def scrape_incremental(self, since_date: str) -> list[dict]:
        """Only process documents with date >= since_date."""
        filtered = [d for d in DOD_DOCUMENTS if d["date"] >= since_date]
        if not filtered:
            console.print(f"  No documents newer than {since_date}")
            return []
        console.print(
            f"  {len(filtered)}/{len(DOD_DOCUMENTS)} documents "
            f"match since_date={since_date}"
        )
        return self._process_documents(filtered)

    # ── Core pipeline ────────────────────────────────────────────────

    def _process_documents(self, documents: list[dict]) -> list[dict]:
        """Download, extract, chunk, and return records for each document."""
        all_records = []

        for doc in documents:
            name = doc["name"]
            url = doc["url"]
            console.print(f"  [bold]{name}[/bold]")
            console.print(f"    Downloading {url}")

            try:
                raw_text = self._download_and_extract(url)
            except Exception as e:
                logger.warning("Failed to download/extract %s: %s", name, e)
                self.scrape_metadata["errors"].append(
                    f"Failed: {name} — {e}"
                )
                continue

            if not raw_text or len(raw_text.strip()) < CHUNK_MIN:
                logger.warning("No usable text extracted from %s", name)
                continue

            cleaned = self._clean_text(raw_text)
            chunks = self._chunk_text(cleaned)

            console.print(
                f"    Extracted {len(cleaned):,} chars -> {len(chunks)} chunks"
            )

            for i, chunk_text in enumerate(chunks):
                heading = self._detect_heading(chunk_text)
                title = heading if heading else f"{name} chunk {i + 1}"

                all_records.append({
                    "text": chunk_text,
                    "source": name,
                    "title": title,
                    "doc_name": name,
                    "chunk_index": i,
                    "total_chunks": len(chunks),
                    "date": doc["date"],
                    "url": url,
                    "scraper": "dod_documents",
                })

        console.print(f"  [green]Total: {len(all_records)} chunks from "
                       f"{len(documents)} documents[/green]")
        return all_records

    # ── PDF download and extraction ──────────────────────────────────

    def _download_and_extract(self, url: str) -> str:
        """Download a PDF and extract all text using pypdf."""
        resp = self._request(url)

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as tmp:
            tmp.write(resp.content)
            tmp.flush()

            reader = PdfReader(tmp.name)
            pages_text = []

            for page in reader.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(text)

        return "\n\n".join(pages_text)

    # ── Text cleaning ────────────────────────────────────────────────

    def _clean_text(self, text: str) -> str:
        """Remove repeated headers/footers, page numbers, and normalize whitespace."""

        # Split into page-like segments (pypdf usually joins with form-feed or
        # double newlines between pages)
        pages = re.split(r"\f|\n{3,}", text)

        # Detect repeated header/footer lines across pages
        if len(pages) >= _HEADER_FOOTER_THRESHOLD:
            line_counts = {}
            for page in pages:
                lines = page.strip().splitlines()
                # Check first 3 and last 3 lines of each page
                candidates = lines[:3] + lines[-3:]
                for line in candidates:
                    stripped = line.strip()
                    if stripped and len(stripped) < 200:
                        line_counts[stripped] = line_counts.get(stripped, 0) + 1

            repeated = {
                line for line, count in line_counts.items()
                if count >= _HEADER_FOOTER_THRESHOLD
            }

            if repeated:
                logger.debug(
                    "Removing %d repeated header/footer lines", len(repeated)
                )
                cleaned_pages = []
                for page in pages:
                    cleaned_lines = [
                        line for line in page.splitlines()
                        if line.strip() not in repeated
                    ]
                    cleaned_pages.append("\n".join(cleaned_lines))
                pages = cleaned_pages

        text = "\n\n".join(pages)

        # Remove standalone page numbers
        text = _PAGE_NUM_RE.sub("", text)

        # Collapse runs of whitespace
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)

        return text.strip()

    # ── Chunking ─────────────────────────────────────────────────────

    def _chunk_text(self, text: str) -> list[str]:
        """Split text into chunks at paragraph boundaries with overlap.

        Strategy:
        1. Split on double newlines (paragraph boundaries) and section headings
        2. Accumulate paragraphs until CHUNK_TARGET is reached
        3. When a chunk is full, finalize it and carry the last sentence forward
        4. Skip chunks that are too short or mostly whitespace/TOC
        """
        if len(text) <= CHUNK_MAX:
            if self._is_junk(text):
                return []
            return [text]

        paragraphs = re.split(r"\n\n+", text)
        chunks = []
        current_paras = []
        current_len = 0
        overlap_text = ""

        for para in paragraphs:
            para = para.strip()
            if not para:
                continue

            # If adding this paragraph exceeds target and we have content,
            # finalize the current chunk
            if current_len + len(para) > CHUNK_TARGET and current_paras:
                chunk = "\n\n".join(current_paras)
                if not self._is_junk(chunk):
                    chunks.append(chunk)

                # Extract last sentence for overlap
                overlap_text = self._last_sentence(current_paras[-1])

                # Start new chunk with overlap + current paragraph
                if overlap_text:
                    current_paras = [overlap_text, para]
                    current_len = len(overlap_text) + len(para)
                else:
                    current_paras = [para]
                    current_len = len(para)
            else:
                current_paras.append(para)
                current_len += len(para)

        # Final chunk
        if current_paras:
            chunk = "\n\n".join(current_paras)
            if not self._is_junk(chunk):
                chunks.append(chunk)

        return chunks

    def _is_junk(self, text: str) -> bool:
        """Return True if the text is mostly whitespace, page numbers, or TOC."""
        stripped = text.strip()
        if len(stripped) < CHUNK_MIN:
            return True

        # Mostly dots (table of contents leader lines)
        dot_count = stripped.count(".")
        if dot_count > len(stripped) * 0.3 and len(stripped) > 50:
            # Looks like TOC with dot leaders
            lines = stripped.splitlines()
            toc_lines = sum(
                1 for line in lines if re.search(r"\.{4,}", line)
            )
            if toc_lines > len(lines) * 0.5:
                return True

        # Mostly numbers and whitespace
        alpha_chars = sum(1 for c in stripped if c.isalpha())
        if alpha_chars < len(stripped) * 0.3:
            return True

        return False

    @staticmethod
    def _last_sentence(text: str) -> str:
        """Extract the last sentence from a paragraph for overlap context."""
        # Split on sentence-ending punctuation followed by space or end
        sentences = re.split(r"(?<=[.!?])\s+", text.strip())
        if not sentences:
            return ""
        last = sentences[-1].strip()
        # Don't carry over very short or very long fragments
        if len(last) < 20 or len(last) > 300:
            return ""
        return last

    # ── Section heading detection ────────────────────────────────────

    @staticmethod
    def _detect_heading(chunk_text: str) -> str | None:
        """Try to find a section heading at the start of the chunk.

        Looks for lines that are:
        - ALL CAPS or Title Case
        - Short (< 100 chars)
        - Match heading patterns (numbered sections, Chapter/Section)
        """
        lines = chunk_text.strip().splitlines()

        # Check first few lines for a heading
        for line in lines[:5]:
            line = line.strip()
            if not line or len(line) > 100:
                continue

            # Check against heading regex
            if _HEADING_RE.match(line):
                return line

            # Title Case heuristic: most words capitalized, short line
            words = line.split()
            if 2 <= len(words) <= 12:
                cap_words = sum(
                    1 for w in words
                    if w[0].isupper() or w.lower() in {
                        "a", "an", "the", "and", "or", "of", "in",
                        "for", "to", "with", "on", "at", "by",
                    }
                )
                if cap_words == len(words) and not line.endswith((".", ",")):
                    return line

        return None
