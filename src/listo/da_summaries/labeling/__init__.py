"""Labeling pipeline for the LayoutLMv3 entity extractor.

Pieces:
  - schema.py     : the fixed label set (BIO tags, role mapping)
  - dataset.py    : entity_evidence → HuggingFace token-classification format
  - review.py     : (future) CLI to manually verify/correct labels

Schema-first design: until labels are stable, model training output will
be unstable. Bumping the schema means retraining from scratch — so we
keep it small (≤10 labels) and conservative.
"""
