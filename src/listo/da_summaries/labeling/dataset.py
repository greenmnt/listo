"""Build a LayoutLMv3-style training dataset from `entity_evidence`.

For each document with entity rows, load the source PDF, walk each
page's text spans (treating each pdf-span as a "word" — natural
granularity for LayoutLMv3), and tag spans whose char range overlaps
an entity_evidence span with the appropriate BIO label.

Output is JSON Lines; each line is one **page** of one **document**:

    {
      "doc_id": 109744,
      "page": 0,
      "metadata": {"kind": "ir_council", "type": "...", ...},
      "words":  ["Our", "reference:", "COM/2021/115", ...],
      "bboxes": [[33,109,50,118], ...],     # x0,y0,x1,y1 in PDF points
      "labels": ["O", "O", "O", "B-APPLICANT", "I-APPLICANT", ...],
      "page_w": 595, "page_h": 842,
      "n_entities": 3,                       # how many entity rows we tagged here
    }

The training script later normalises bboxes to [0, 1000] and uses the
metadata fields as prefix tokens (zero bbox).
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

from sqlalchemy import text as sql_text

from listo.db import session_scope
from listo.da_summaries.entity_evidence import DocLayout
from listo.da_summaries.doc_metadata import doc_metadata_for
from listo.da_summaries.labeling.schema import normalise_role, role_to_bio


logger = logging.getLogger(__name__)


@dataclass
class ExportStats:
    docs_seen: int = 0
    docs_skipped_no_pdf: int = 0
    pages_emitted: int = 0
    entity_rows_used: int = 0
    entity_rows_dropped_no_role: int = 0
    entity_rows_dropped_no_overlap: int = 0
    spans_total: int = 0
    spans_tagged: int = 0


def _entity_rows_for_doc(s, doc_id: int) -> list:
    """All entity_evidence rows for a doc, with truth-role-if-set
    falling back to candidate-role. Returns the saved source_text too
    (per-row — plan rows save page text, correspondence rows save
    whole-doc concat) so the exporter can re-anchor offsets against
    the fresh DocLayout."""
    return s.execute(sql_text("""
        SELECT
          id, candidate_role, truth_role, candidate_name,
          span_start, span_end, status, source_text
        FROM entity_evidence
        WHERE source_doc_id = :doc_id
          AND status != 'rejected'
        ORDER BY span_start
    """), {"doc_id": doc_id}).fetchall()


def _resolve_role(row) -> str | None:
    """Use truth_role when verified/corrected; else candidate_role.
    Pass through schema.normalise_role to fold synonyms / drop ambiguous."""
    raw = row.truth_role if row.status in ("verified", "corrected") else row.candidate_role
    return normalise_role(raw)


def _is_bad_name(name: str | None) -> bool:
    """Filter known regex artefacts out of the training set even when
    they're already in entity_evidence (the existing buggy rows are
    ignored at export time rather than retroactively rewritten — see
    discussion of forward-only fixes).

    Bad patterns:
      - 1-3 char fragments (PyMuPDF line-wrap leftovers like 'Dj', 'R')
      - Captured contact-person suffix: '… (Attention: …)' / 'Att: …'
      - Multi-line capture (newline mid-name)
    """
    if not name:
        return True
    n = name.strip()
    if len(n) < 4:
        return True
    if "\n" in n:
        return True
    low = n.lower()
    if "(attention:" in low or "(attn:" in low or " att: " in low or " attn: " in low:
        return True
    return False


def _page_for_offset(doc: DocLayout, char_offset: int) -> int | None:
    """Which page's text contains the given char offset in `concat_text`?"""
    from bisect import bisect_right
    pi = bisect_right(doc.page_offsets, char_offset) - 1
    if pi < 0 or pi >= len(doc.pages):
        return None
    return pi


def export_doc(s, doc_id: int) -> Iterator[dict]:
    """Yield one JSON record per page for the given doc_id."""
    meta = doc_metadata_for(s, doc_id)
    if meta is None:
        return

    file_path_row = s.execute(sql_text(
        "SELECT file_path FROM council_application_documents WHERE id = :i"
    ), {"i": doc_id}).fetchone()
    if not file_path_row or not file_path_row.file_path:
        return

    doc = DocLayout.from_pdf(file_path_row.file_path)
    if doc is None:
        return

    entity_rows = _entity_rows_for_doc(s, doc_id)
    if not entity_rows:
        return

    # Bucket entity rows by which page they fall on.
    # The harvester stores `source_text` differently for plans vs
    # correspondence: plans save one page's text + page-local offsets,
    # correspondence saves the whole-doc concat + concat-relative
    # offsets. To work in either case, locate the saved source_text
    # within the fresh concat_text and translate spans by the delta.
    entities_by_page: dict[int, list] = {}
    for er in entity_rows:
        role = _resolve_role(er)
        if role is None:
            continue
        if not er.source_text:
            continue
        if _is_bad_name(er.candidate_name):
            continue
        anchor = doc.concat_text.find(er.source_text)
        if anchor < 0:
            # Source text doesn't appear in fresh concat (shouldn't
            # happen unless the PDF changed on disk). Skip.
            continue
        concat_span_start = anchor + er.span_start
        concat_span_end = anchor + er.span_end
        pi = _page_for_offset(doc, concat_span_start)
        if pi is None:
            continue
        page_off_start = concat_span_start - doc.page_offsets[pi]
        page_off_end = concat_span_end - doc.page_offsets[pi]
        entities_by_page.setdefault(pi, []).append({
            "role": role,
            "start": page_off_start,
            "end": page_off_end,
            "name": er.candidate_name,
        })

    if not entities_by_page:
        return

    metadata_dict = {
        "kind": meta.kind,
        "type": meta.type,
        "council": meta.council,
        "state": meta.state,
        "vendor": meta.vendor,
    }

    for pi, page_entities in entities_by_page.items():
        page = doc.pages[pi]
        # Each pdf-span is a "word" — natural LayoutLMv3 granularity.
        words: list[str] = []
        bboxes: list[list[float]] = []
        labels: list[str] = []
        for sp in page.spans:
            text = page.text[sp.char_start: sp.char_end]
            text_stripped = text.strip()
            if not text_stripped:
                continue
            words.append(text_stripped)
            bboxes.append([
                round(sp.bbox[0], 2), round(sp.bbox[1], 2),
                round(sp.bbox[2], 2), round(sp.bbox[3], 2),
            ])
            # Default label; overridden below if this span is in an entity.
            labels.append("O")

        # Tag spans that overlap any entity range.
        # First pass: build (span_start_in_page, span_end_in_page, idx_in_words)
        # by re-scanning page.spans (skipping whitespace-only ones).
        word_idx_to_span_range: list[tuple[int, int, int]] = []
        wi = 0
        for sp in page.spans:
            text = page.text[sp.char_start: sp.char_end]
            if not text.strip():
                continue
            word_idx_to_span_range.append((sp.char_start, sp.char_end, wi))
            wi += 1

        for ent in page_entities:
            tagged_any = False
            first = True
            for s_start, s_end, w_idx in word_idx_to_span_range:
                # Overlap test: span ∩ entity ≠ ∅
                if s_end <= ent["start"] or s_start >= ent["end"]:
                    continue
                pos = "B" if first else "I"
                labels[w_idx] = role_to_bio(ent["role"], pos)
                first = False
                tagged_any = True
            ent["_tagged"] = tagged_any

        if not words:
            continue
        n_tagged = sum(1 for l in labels if l != "O")
        n_entities_used = sum(1 for e in page_entities if e.get("_tagged"))

        yield {
            "doc_id": doc_id,
            "page": pi,
            "metadata": metadata_dict,
            "words": words,
            "bboxes": bboxes,
            "labels": labels,
            "page_w": page.page_w,
            "page_h": page.page_h,
            "n_words": len(words),
            "n_words_tagged": n_tagged,
            "n_entities": n_entities_used,
        }


def export_all(out_path: Path) -> ExportStats:
    """Export every doc with entity_evidence rows to a JSONL file."""
    stats = ExportStats()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with session_scope() as s, out_path.open("w") as f:
        doc_ids = [r.source_doc_id for r in s.execute(sql_text("""
            SELECT DISTINCT source_doc_id
            FROM entity_evidence
            WHERE status != 'rejected'
              AND source_doc_id IS NOT NULL
            ORDER BY source_doc_id
        """)).fetchall()]
        stats.docs_seen = len(doc_ids)

        for doc_id in doc_ids:
            for record in export_doc(s, doc_id):
                f.write(json.dumps(record))
                f.write("\n")
                stats.pages_emitted += 1
                stats.spans_total += record["n_words"]
                stats.spans_tagged += record["n_words_tagged"]
                stats.entity_rows_used += record["n_entities"]

    return stats
