# CMMC Data Pipeline

Automated data pipeline that scrapes authoritative CMMC and cybersecurity compliance sources, converts them into chat training pairs, and produces deduplicated datasets for fine-tuning language models.

Built for the [CMMC Compliance AI Model](https://github.com/NathanMaine/cmmc-compliance-ai-model) project.

## What It Does

1. **Scrapes** 5 government sources via their public APIs
2. **Filters** irrelevant regulatory sections (e.g., non-cyber DFARS clauses)
3. **Converts** raw text into chat format (`system / user / assistant` message triples)
4. **Quality filters** by length, content ratio, and structure
5. **Deduplicates** using xxHash (exact) and MinHash LSH (near-duplicate detection)
6. **Validates** format and quality against configurable thresholds
7. **Versions** each run as an immutable snapshot for reproducibility
8. **Merges** new data with existing training sets

## Sources

| Source | Records | What It Provides |
|--------|---------|------------------|
| NIST SP 800-53 Rev. 5 | 1,016 | Security and privacy controls (OSCAL JSON) |
| DoD Documents | 606 | CMMC assessment guides, scoping guides, policy PDFs |
| NIST CSF 2.0 | 208 | Cybersecurity Framework categories and subcategories |
| NIST SP 800-171 Rev. 3 | 97 | CUI protection controls (OSCAL JSON) |
| eCFR | 413 (75 after filter) | 32 CFR 170 (CMMC), 45 CFR 164 (HIPAA), 48 CFR 252 (DFARS) |

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

### Full pipeline (scrape + process)

```bash
python -m scripts.run_pipeline --verbose
```

### Reprocess existing raw data (no re-scraping)

```bash
python -m scripts.run_pipeline --skip-scrape --verbose
```

### Select specific sources

```bash
python -m scripts.run_pipeline --skip-scrape -s nist_csrc -s ecfr -s dod_documents
```

### Incremental update (only new data since date)

```bash
python -m scripts.run_pipeline --since 2026-03-01
```

### Merge a snapshot into training data

```bash
python -m scripts.merge v004
```

### Check pipeline status

```bash
python -m scripts.status
```

### Other options

```
--dry-run            Run without creating snapshots
--skip-validation    Skip validation checks
--auto-merge         Automatically merge into training data after validation
--config PATH        Custom config.yaml path
```

## Configuration

All settings are in `config.yaml`:

- **Source URLs** and API endpoints for each scraper
- **Quality thresholds**: min/max answer length, table ratio, alpha ratio
- **Dedup parameters**: MinHash permutations, LSH threshold, shingle size
- **Training data path**: where to merge final output
- **Validation rules**: min/max record counts, duplicate ratios

## Pipeline Output

Each run creates a versioned snapshot under `data/pipeline/versions/`:

```
data/pipeline/versions/v004/
  records.jsonl        # Chat-format training records
  version_info.json    # Metadata (record count, sources, timestamp)
```

Records are in OpenAI chat format:

```json
{
  "messages": [
    {"role": "system", "content": "You are a CMMC and cybersecurity compliance expert..."},
    {"role": "user", "content": "What does NIST SP 800-53 require for AC-2?"},
    {"role": "assistant", "content": "AC-2 — Account Management\n\nControl Statement: ..."}
  ],
  "source": "nist_csrc_AC-2"
}
```

## Project Structure

```
scrapers/              Source-specific scrapers (one per data source)
processors/
  converter.py         Raw text -> chat format conversion
  relevance_filter.py  Removes non-CMMC regulatory sections
  quality_filter.py    Length, structure, and content checks
  dedup.py             xxHash exact + MinHash LSH near-dedup
pipeline/
  runner.py            Main orchestrator (7-step pipeline)
  versioning.py        Immutable snapshot management
  validator.py         Format and quality validation
scripts/               CLI entry points
tests/                 Unit tests
config.yaml            All pipeline configuration
data/                  Raw + versioned data (gitignored)
```

## Results

**v004** (current): 1,841 unique records from 5 sources, merged with 16,906 existing v1.0 records for **18,747 total training examples**.

## License

MIT
