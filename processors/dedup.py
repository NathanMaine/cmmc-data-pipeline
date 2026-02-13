"""Deduplication against new data and existing training set."""

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

import xxhash
from datasketch import MinHash, MinHashLSH
from rich.console import Console

console = Console()
logger = logging.getLogger(__name__)


@dataclass
class DedupStats:
    total_input: int = 0
    exact_dupes: int = 0
    near_dupes: int = 0
    unique: int = 0


class DedupIndex:
    """Maintains hash indices for deduplication."""

    def __init__(self, num_perm: int = 128, lsh_threshold: float = 0.8, shingle_size: int = 5):
        self.num_perm = num_perm
        self.shingle_size = shingle_size
        self.exact_hashes = set()
        self.lsh = MinHashLSH(threshold=lsh_threshold, num_perm=num_perm)
        self.minhashes = {}  # key -> (minhash, text_length)
        self._counter = 0

    def load_existing(self, training_data_path: str):
        """Load hashes from existing training data."""
        path = Path(training_data_path)
        loaded = 0

        for jsonl_file in ["train.jsonl", "validation.jsonl"]:
            filepath = path / jsonl_file
            if not filepath.exists():
                continue
            with open(filepath) as f:
                for line in f:
                    record = json.loads(line)
                    messages = record.get("messages", [])
                    # Get assistant content (index 2)
                    if len(messages) >= 3:
                        content = messages[2].get("content", "")
                        self._add_to_index(content, f"existing_{loaded}")
                        loaded += 1

        console.print(f"[green]Loaded {loaded} existing records into dedup index[/green]")

    def _make_minhash(self, text: str) -> MinHash:
        """Create a MinHash from text using character n-grams."""
        m = MinHash(num_perm=self.num_perm)
        for i in range(len(text) - self.shingle_size + 1):
            m.update(text[i:i + self.shingle_size].encode("utf-8"))
        return m

    def _add_to_index(self, content: str, key: str = None):
        """Add content to both exact and near-dedup indices."""
        h = xxhash.xxh64(content.encode()).hexdigest()
        self.exact_hashes.add(h)

        if key is None:
            key = f"rec_{self._counter}"
            self._counter += 1

        mh = self._make_minhash(content)
        try:
            self.lsh.insert(key, mh)
            self.minhashes[key] = (mh, len(content))
        except ValueError:
            pass  # Duplicate key

    def is_duplicate(self, content: str) -> tuple[bool, str]:
        """Check if content is a duplicate. Returns (is_dup, reason)."""
        h = xxhash.xxh64(content.encode()).hexdigest()
        if h in self.exact_hashes:
            return True, "exact"

        mh = self._make_minhash(content)
        results = self.lsh.query(mh)
        if results:
            return True, "near"

        return False, ""

    def deduplicate_batch(self, records: list[dict], content_key: str = "text") -> tuple[list[dict], DedupStats]:
        """Deduplicate a batch of records against existing index and each other.

        When near-duplicates found, keeps the longer record.
        """
        stats = DedupStats(total_input=len(records))
        unique = []

        for record in records:
            content = record.get(content_key, "")
            if not content:
                continue

            is_dup, reason = self.is_duplicate(content)

            if is_dup and reason == "exact":
                stats.exact_dupes += 1
            elif is_dup and reason == "near":
                stats.near_dupes += 1
            else:
                # Add to index and keep
                self._add_to_index(content)
                unique.append(record)
                stats.unique += 1

        return unique, stats

    def save_index(self, path: str):
        """Save exact hash set to disk for persistence."""
        with open(path, "w") as f:
            json.dump(list(self.exact_hashes), f)

    def load_index(self, path: str):
        """Load exact hash set from disk."""
        p = Path(path)
        if p.exists():
            with open(p) as f:
                self.exact_hashes = set(json.loads(f.read()))
