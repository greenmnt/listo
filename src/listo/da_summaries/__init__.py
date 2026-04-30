"""Ollama-based DA-document summarisation pipeline.

Phases:
- summarise.py — Phase 1: first + last doc per DA → da_doc_summaries
- escalate.py — Phase 2: incomplete DAs → tier-2 priority docs
- aggregate.py — Phase 3: merge per-doc rows + process stats → da_summaries
- businesses.py — Phase 4: Google-search builder/architect names → business_links

All four are independently runnable + idempotent. Each accepts
--computer-index / --computer-count for two-machine modulo partitioning.
"""
