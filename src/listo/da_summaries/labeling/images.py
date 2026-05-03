"""Pre-render the PDF page images that LayoutLMv3 needs.

The trainer's "image" branch wants a 224×224 RGB tile of every page
referenced in `entity_train.jsonl`. Rendering at training time means
the GPU host needs the PDFs available locally — but the canonical PDF
store lives on the server, while we'd like to train on whatever
machine has a GPU.

This module renders every page in the JSONL once, server-side, into
`data/labeling/images/{doc_id}_p{page}.jpg`. Total payload is small
(~30 KB per image × ~2000 pages ≈ 60 MB), so it can be rsync'd to
any GPU host without bringing the underlying PDFs along.

The trainer prefers this cache when it exists and falls back to
rendering from PDF otherwise — so single-host runs still work.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

import pymupdf  # type: ignore[import-untyped]
from PIL import Image
from sqlalchemy import bindparam, text as sql_text

from listo.db import session_scope


logger = logging.getLogger(__name__)

IMAGE_SIZE = 224


@dataclass
class RenderStats:
    pages_seen: int = 0
    pages_rendered: int = 0
    pages_skipped_existing: int = 0
    pages_failed: int = 0


def cache_path_for(images_dir: Path, doc_id: int, page: int) -> Path:
    return images_dir / f"{doc_id}_p{page}.jpg"


def _resolve_pdf_paths(doc_ids: list[int]) -> dict[int, str]:
    out: dict[int, str] = {}
    if not doc_ids:
        return out
    with session_scope() as s:
        rows = s.execute(
            sql_text(
                "SELECT id, file_path FROM council_application_documents "
                "WHERE id IN :ids"
            ).bindparams(bindparam("ids", expanding=True)),
            {"ids": doc_ids},
        ).fetchall()
        for r in rows:
            if r.file_path:
                out[r.id] = r.file_path
    return out


def _render_one(pdf_path: str, page_index: int, out_path: Path) -> bool:
    try:
        with pymupdf.open(pdf_path) as pdf:
            if page_index >= pdf.page_count:
                return False
            page = pdf[page_index]
            pix = page.get_pixmap(dpi=72)
            img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
    except Exception as exc:
        logger.warning("render failed %s p%d: %s", pdf_path, page_index, exc)
        return False
    img = img.resize((IMAGE_SIZE, IMAGE_SIZE), Image.Resampling.LANCZOS)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path, format="JPEG", quality=85, optimize=True)
    return True


def render_all(jsonl_path: Path, images_dir: Path) -> RenderStats:
    """Walk the training JSONL and render every (doc_id, page) into
    the image cache. Idempotent — skips files that already exist."""
    stats = RenderStats()
    images_dir.mkdir(parents=True, exist_ok=True)

    pairs: list[tuple[int, int]] = []
    seen: set[tuple[int, int]] = set()
    with jsonl_path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            key = (int(r["doc_id"]), int(r["page"]))
            if key in seen:
                continue
            seen.add(key)
            pairs.append(key)
    stats.pages_seen = len(pairs)

    pdf_paths = _resolve_pdf_paths(sorted({d for d, _ in pairs}))

    for i, (doc_id, page) in enumerate(pairs, 1):
        if i == 1 or i % 100 == 0 or i == len(pairs):
            logger.info("progress: %d/%d  rendered=%d skipped=%d failed=%d",
                        i, len(pairs), stats.pages_rendered,
                        stats.pages_skipped_existing, stats.pages_failed)
        out_path = cache_path_for(images_dir, doc_id, page)
        if out_path.exists():
            stats.pages_skipped_existing += 1
            continue
        pdf = pdf_paths.get(doc_id)
        if not pdf:
            stats.pages_failed += 1
            continue
        if _render_one(pdf, page, out_path):
            stats.pages_rendered += 1
        else:
            stats.pages_failed += 1

    return stats
