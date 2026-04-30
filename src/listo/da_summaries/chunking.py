"""Page-windowed chunking for the build-features / risk lanes.

Tier-1 / tier-2 entity extraction folds a whole document into one prompt
because the relevant facts (applicant name, dwelling count) live up-front
in DA Form 1. The build-features and risk lanes scan documents that may
be 80+ pages — a GFA table can sit on page 4, materials on page 17,
landscaping on page 53. Feeding all 80 pages at once tanks 7B-class
models. So we split the document into overlapping page windows, run the
extractor on each, and merge results downstream in aggregate.

Page-aligned chunking (rather than char-aligned) keeps natural document
structure — section headers, tables, drawing title blocks all live on
specific pages, so a chunk boundary at a page boundary loses nothing.
"""
from __future__ import annotations

from dataclasses import dataclass


# Below this many usable chars in a chunk, skip — empty/scanned pages.
MIN_CHUNK_CHARS = 200

# Hard cap on chunk text fed to a 7B model. ~5K tokens leaves comfortable
# room for system+user prompts and the JSON response schema.
MAX_CHUNK_CHARS = 18_000


@dataclass
class Chunk:
    chunk_index: int      # 0-based, in walk order
    page_start: int       # 1-indexed, inclusive
    page_end: int         # 1-indexed, inclusive
    text: str             # concatenated page text, trimmed to MAX_CHUNK_CHARS


def chunk_pages(
    pages: list[str],
    *,
    size: int = 5,
    overlap: int = 1,
) -> list[Chunk]:
    """Split a per-page text list into overlapping page windows.

    `pages[i]` is the (possibly empty) text of source page `i+1`. The
    return list is in walk order; each chunk records its inclusive
    1-indexed page span. Chunks whose joined text is below
    `MIN_CHUNK_CHARS` are dropped (so all-image regions don't waste an
    LLM call).
    """
    if size < 1:
        raise ValueError("size must be >= 1")
    if overlap < 0 or overlap >= size:
        raise ValueError("overlap must be in [0, size)")

    n = len(pages)
    if n == 0:
        return []

    step = max(size - overlap, 1)
    out: list[Chunk] = []
    chunk_idx = 0
    start = 0
    while start < n:
        end = min(start + size, n)  # exclusive
        text_parts: list[str] = []
        running = 0
        for i in range(start, end):
            t = pages[i] or ""
            if not t:
                continue
            piece = t if running == 0 else "\n\n" + t
            if running + len(piece) > MAX_CHUNK_CHARS:
                # Truncate this chunk at the cap; the next chunk picks
                # up via overlap so we don't strand content.
                remaining = MAX_CHUNK_CHARS - running
                if remaining > 0:
                    text_parts.append(piece[:remaining])
                running = MAX_CHUNK_CHARS
                break
            text_parts.append(piece)
            running += len(piece)

        text = "".join(text_parts).strip()
        if len(text) >= MIN_CHUNK_CHARS:
            out.append(
                Chunk(
                    chunk_index=chunk_idx,
                    page_start=start + 1,
                    page_end=end,
                    text=text,
                )
            )
            chunk_idx += 1

        if end >= n:
            break
        start += step

    return out
