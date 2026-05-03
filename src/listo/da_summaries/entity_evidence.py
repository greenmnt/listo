"""Layout-aware evidence collection for entity-extraction training data.

Two halves:

1. **Layout extraction** — wrap pymupdf's `page.get_text("dict")` in a
   data structure that lets the harvester find the bbox + font of any
   character span. The flat text it produces is identical to what
   `get_text("text")` returns (modulo trailing whitespace) so existing
   regex-based parsers keep working unchanged.

2. **Evidence persistence** — `record_evidence()` writes one row per
   regex emission into `entity_evidence`, capturing source_text +
   char-offsets + (optional) layout JSON. Idempotent on
   (extractor, source_doc_id, span_start, span_end).
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pymupdf  # type: ignore[import-untyped]
from sqlalchemy import text as sql_text


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------- layout
@dataclass
class _SpanMeta:
    """Per-pdf-span metadata captured from pymupdf."""
    char_start: int
    char_end: int
    bbox: tuple[float, float, float, float]
    font: str
    size: float
    flags: int


@dataclass
class PageLayout:
    """Flat text of a PDF page plus per-span layout."""
    page_index: int                  # 0-based
    page_w: float
    page_h: float
    text: str                        # flat reconstruction (concat of spans)
    spans: list[_SpanMeta] = field(default_factory=list)
    rotation: int = 0


def page_layout(page: "pymupdf.Page", page_index: int) -> PageLayout:
    """Build a PageLayout from a pymupdf.Page. Concats every text span
    into a flat string, recording each span's char range so we can look
    up bbox/font for any later regex match offset."""
    parts: list[str] = []
    spans: list[_SpanMeta] = []
    cursor = 0
    d = page.get_text("dict")
    for blk in d.get("blocks", []):
        if blk.get("type") != 0:  # 0 = text block; 1 = image
            continue
        for line in blk.get("lines", []):
            for span in line.get("spans", []):
                t = span.get("text", "")
                if not t:
                    continue
                start = cursor
                parts.append(t)
                cursor += len(t)
                spans.append(_SpanMeta(
                    char_start=start,
                    char_end=cursor,
                    bbox=tuple(span["bbox"]),
                    font=span.get("font", ""),
                    size=float(span.get("size", 0.0)),
                    flags=int(span.get("flags", 0)),
                ))
            # Newline between lines so multi-line regexes still work.
            parts.append("\n")
            cursor += 1
        parts.append("\n")
        cursor += 1
    return PageLayout(
        page_index=page_index,
        page_w=float(page.rect.width),
        page_h=float(page.rect.height),
        text="".join(parts),
        spans=spans,
        rotation=int(getattr(page, "rotation", 0) or 0),
    )


def load_pdf_pages(file_path: str, max_pages: int | None = None) -> list[PageLayout]:
    """Open a PDF and return PageLayout for each page (up to max_pages)."""
    p = Path(file_path)
    if not p.exists():
        return []
    out: list[PageLayout] = []
    try:
        with pymupdf.open(p) as pdf:
            n = pdf.page_count if max_pages is None else min(pdf.page_count, max_pages)
            for i in range(n):
                out.append(page_layout(pdf[i], i))
    except Exception as exc:  # noqa: BLE001
        logger.warning("pymupdf failed scanning %s: %s", p, exc)
    return out


def layout_for_span(page: PageLayout, span_start: int, span_end: int) -> dict[str, Any] | None:
    """Compute the union bbox + dominant font for a char-span on a page.
    Returns None if no overlapping pdf-spans were found (e.g., the
    span is purely whitespace/newlines that we synthesised)."""
    bbox: list[float] | None = None
    font = None
    size = None
    flags = None
    for sp in page.spans:
        if sp.char_end <= span_start or sp.char_start >= span_end:
            continue
        if bbox is None:
            bbox = list(sp.bbox)
            font, size, flags = sp.font, sp.size, sp.flags
        else:
            bbox[0] = min(bbox[0], sp.bbox[0])
            bbox[1] = min(bbox[1], sp.bbox[1])
            bbox[2] = max(bbox[2], sp.bbox[2])
            bbox[3] = max(bbox[3], sp.bbox[3])
            # Keep the first span's font as "dominant" — good enough for
            # most title blocks where the whole block uses one font.
    if bbox is None:
        return None
    return {
        "page_index": page.page_index,
        "page_w": page.page_w,
        "page_h": page.page_h,
        "bbox": [round(x, 2) for x in bbox],
        "font": font,
        "size": round(size, 2) if size is not None else None,
        "flags": flags,
        "rotation": page.rotation,
    }


# ---------------------------------------------------------------- offsets in flat text


def find_offset_in_text(text: str, needle: str, near: int = 0) -> tuple[int, int] | None:
    """Find `needle` in `text`, preferring the first occurrence at or
    after `near`. Falls back to first global occurrence. Returns None
    when not found at all."""
    if not needle:
        return None
    idx = text.find(needle, near) if near > 0 else -1
    if idx == -1:
        idx = text.find(needle)
    if idx == -1:
        return None
    return idx, idx + len(needle)


# ---------------------------------------------------------------- doc-level


@dataclass
class DocLayout:
    """A whole-PDF view: concatenated text across pages + per-page
    layout info + the offset where each page starts in the concat.

    Built lazily once per harvested doc — every emission for that doc
    reuses the same DocLayout via `find_layout_for_name`.
    """
    concat_text: str
    pages: list[PageLayout]
    page_offsets: list[int]   # parallel to `pages`; concat_text[page_offsets[i]:] starts page i

    @classmethod
    def from_pdf(cls, file_path: str, max_pages: int | None = None) -> "DocLayout | None":
        pages = load_pdf_pages(file_path, max_pages=max_pages)
        if not pages:
            return None
        parts: list[str] = []
        offsets: list[int] = []
        cursor = 0
        for p in pages:
            offsets.append(cursor)
            parts.append(p.text)
            cursor += len(p.text)
            # Form-feed page separator — keeps the parser able to
            # detect page boundaries if it ever needs to, doesn't
            # interfere with most regex.
            parts.append("\f")
            cursor += 1
        return cls(
            concat_text="".join(parts),
            pages=pages,
            page_offsets=offsets,
        )

    def find_layout_for_name(
        self, needle: str, *, near: int = 0
    ) -> tuple[int, int, dict | None] | None:
        """Locate `needle` in concat_text and return
        `(span_start, span_end, layout_json_or_none)` — the layout dict
        carries page geometry + bbox + font for the span. Returns None
        when the name isn't anywhere in the doc."""
        sp = find_offset_in_text(self.concat_text, needle, near=near)
        if sp is None:
            return None
        # Find which page contains the span_start.
        from bisect import bisect_right
        pi = bisect_right(self.page_offsets, sp[0]) - 1
        if pi < 0 or pi >= len(self.pages):
            return sp[0], sp[1], None
        page = self.pages[pi]
        page_off_start = sp[0] - self.page_offsets[pi]
        page_off_end = min(sp[1] - self.page_offsets[pi], len(page.text))
        layout = layout_for_span(page, page_off_start, page_off_end)
        return sp[0], sp[1], layout


# ---------------------------------------------------------------- DB write


def record_evidence(
    s,
    *,
    application_id: int,
    source_doc_id: int | None,
    extractor: str,
    source_text: str,
    span_start: int,
    span_end: int,
    candidate_name: str,
    candidate_role: str | None,
    confidence: str | None,
    layout: dict | None = None,
) -> None:
    """Insert/refresh one row in `entity_evidence`.

    Idempotent on (extractor, source_doc_id, span_start, span_end) — the
    same extractor version emitting the same span twice updates rather
    than duplicates. Verification fields (status, truth_*, verifier)
    are NEVER touched on update so manual labels survive re-runs.
    """
    layout_json = json.dumps(layout) if layout else None
    s.execute(
        sql_text("""
            INSERT INTO entity_evidence (
                application_id, source_doc_id, extractor,
                source_text, span_start, span_end,
                candidate_name, candidate_role, confidence, layout
            ) VALUES (
                :app_id, :doc_id, :extractor,
                :source_text, :span_start, :span_end,
                :name, :role, :conf, :layout
            )
            ON DUPLICATE KEY UPDATE
                source_text     = VALUES(source_text),
                candidate_name  = VALUES(candidate_name),
                candidate_role  = VALUES(candidate_role),
                confidence      = VALUES(confidence),
                -- Preserve layout when this call didn't supply one
                -- (e.g., a re-run from cached text without re-loading
                -- the PDF). Verification fields are never touched.
                layout          = COALESCE(VALUES(layout), layout),
                application_id  = VALUES(application_id)
        """),
        {
            "app_id": application_id,
            "doc_id": source_doc_id,
            "extractor": extractor,
            "source_text": source_text,
            "span_start": span_start,
            "span_end": span_end,
            "name": candidate_name,
            "role": candidate_role,
            "conf": confidence,
            "layout": layout_json,
        },
    )
