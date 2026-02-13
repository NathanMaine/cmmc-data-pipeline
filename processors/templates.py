"""Instruction templates for generating training examples."""

import random

# Unified system prompt — MUST match existing training data exactly
SYSTEM_PROMPT = (
    "You are a CMMC and cybersecurity compliance expert with deep knowledge of "
    "CMMC 2.0, NIST SP 800-171, NIST SP 800-172, NIST CSF, HIPAA Security Rule, "
    "and related frameworks. You provide accurate, practical guidance on compliance "
    "requirements, security controls, implementation procedures, and assessment "
    "preparation. You cite specific standards, controls, and regulatory references."
)

# Templates when source document is known but no specific topic extracted
QUESTION_TEMPLATES = [
    "What does {source} say about this topic?",
    "According to {source}, what are the key requirements?",
    "Summarize the guidance provided in {source}.",
    "What are the compliance requirements described in {source}?",
    "Explain the security controls outlined in {source}.",
    "What does {source} recommend for implementation?",
    "What guidance does {source} provide?",
]

# Templates when both source and topic are known
TOPIC_TEMPLATES = [
    "What does {source} say about {topic}?",
    "What are the requirements for {topic} according to {source}?",
    "Explain {topic} as described in {source}.",
    "How does {source} address {topic}?",
    "What controls does {source} require for {topic}?",
]

# Templates for Federal Register documents
FEDERAL_REGISTER_TEMPLATES = [
    "What changes does this Federal Register notice introduce regarding {topic}?",
    "Summarize the key provisions of this {doc_type} about {topic}.",
    "What are the compliance implications of this {doc_type} for {topic}?",
    "What does the Federal Register say about {topic} in this {doc_type}?",
]

# Templates for regulatory text (eCFR)
REGULATION_TEMPLATES = [
    "What does {cfr_ref} require regarding {topic}?",
    "Explain the requirements in {cfr_ref} for {topic}.",
    "What are the regulatory requirements for {topic} under {cfr_ref}?",
    "Summarize {cfr_ref} section on {topic}.",
]

# Templates for NIST SP 800-171 controls
SP800_171_TEMPLATES = [
    "What does NIST SP 800-171 Rev. 3 require for {topic}?",
    "Explain the {topic} control in SP 800-171 Rev. 3 and how to assess it.",
    "What are the CUI security requirements for {topic} under SP 800-171?",
    "Describe the {topic} requirement in SP 800-171 Rev. 3 including assessment objectives.",
    "How should organizations implement {topic} per NIST SP 800-171 Rev. 3?",
]

# Templates for NIST CSF 2.0
CSF_TEMPLATES = [
    "What does NIST CSF 2.0 say about {topic}?",
    "Explain the {topic} category in the NIST Cybersecurity Framework 2.0.",
    "How does NIST CSF 2.0 address {topic}?",
    "What are the CSF 2.0 recommendations for {topic}?",
]

# Templates for DoD CMMC documents (assessment guides, scoping guides, etc.)
DOD_DOCUMENT_TEMPLATES = [
    "What does the {source} say about {topic}?",
    "According to the {source}, what are the key requirements for {topic}?",
    "Summarize the guidance in the {source} regarding {topic}.",
    "What does DoD guidance recommend for {topic}?",
]

# Fallback template
FALLBACK_TEMPLATE = "What are the key cybersecurity compliance requirements described here?"


def select_template(source=None, topic=None, doc_type=None, cfr_ref=None,
                    framework=None):
    """Select and fill an appropriate question template.

    framework: optional hint for source-specific templates
               ('sp800_171', 'csf', 'dod_document')
    """
    # Deterministic per-record randomization
    seed_str = f"{source}{topic}{doc_type}{cfr_ref}{framework}"
    random.seed(hash(seed_str) % 2**32)

    if cfr_ref and topic:
        template = random.choice(REGULATION_TEMPLATES)
        return template.format(cfr_ref=cfr_ref, topic=topic)
    elif framework == "sp800_171" and topic:
        template = random.choice(SP800_171_TEMPLATES)
        return template.format(topic=topic)
    elif framework == "csf" and topic:
        template = random.choice(CSF_TEMPLATES)
        return template.format(topic=topic)
    elif framework == "dod_document" and topic and source:
        template = random.choice(DOD_DOCUMENT_TEMPLATES)
        return template.format(source=source, topic=topic)
    elif doc_type and topic:
        template = random.choice(FEDERAL_REGISTER_TEMPLATES)
        return template.format(topic=topic, doc_type=doc_type)
    elif source and topic:
        template = random.choice(TOPIC_TEMPLATES)
        return template.format(source=source, topic=topic)
    elif source:
        template = random.choice(QUESTION_TEMPLATES)
        return template.format(source=source)
    else:
        return FALLBACK_TEMPLATE


def make_chat_record(question, answer, source_id):
    """Create a chat-format training record."""
    return {
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": question},
            {"role": "assistant", "content": answer},
        ],
        "source": source_id,
    }
