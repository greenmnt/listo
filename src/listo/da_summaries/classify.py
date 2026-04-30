"""PDF feature extraction + treatment-class routing.

The pipeline used to route LLM calls by the council-side `doc_type` string,
which is unreliable: a "Forms" document might be a fillable AcroForm, and a
"Specialist Report" might be a scanned image with no text. We now classify
each PDF by features observable from the file itself.

The output is a `treatment` class (one of `acroform_filled`,
`narrative_long`, `narrative_short`, `titleblock`, `image_only`,
`unsupported`). Downstream code maps the treatment + the council-side
`doc_type` hint to a prompt `template_key`.

Features are cached in `document_features` so a doc is only inspected
once. Bumping `ANALYZER_VERSION` invalidates the cache (caller decides
whether to recompute).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path

import pymupdf  # type: ignore[import-untyped]
from sqlalchemy import select, text as sql_text
from sqlalchemy.dialects.mysql import insert as mysql_insert

from listo.db import session_scope
from listo.models import DocumentFeatures


logger = logging.getLogger(__name__)


# Bump when classification rules change (forces re-analysis on next call
# with `force=True`; rows with older versions are ignored as cache).
ANALYZER_VERSION = "v1"


# --------- treatments ---------


TREATMENT_ACROFORM_FILLED = "acroform_filled"
TREATMENT_NARRATIVE_LONG = "narrative_long"
TREATMENT_NARRATIVE_SHORT = "narrative_short"
TREATMENT_TITLEBLOCK = "titleblock"
TREATMENT_IMAGE_ONLY = "image_only"
TREATMENT_UNSUPPORTED = "unsupported"

SKIP_TREATMENTS = {TREATMENT_IMAGE_ONLY, TREATMENT_UNSUPPORTED}


# --------- feature dataclass ---------


@dataclass
class PdfFeatures:
    page_count: int
    total_text_chars: int
    mean_chars_per_page: int
    has_acroform: bool
    n_text_widgets: int
    n_text_widgets_filled: int
    n_checkbox_widgets: int
    pdf_producer: str | None
    pdf_creator: str | None
    pdf_format: str | None
    treatment: str
    extraction_notes: str | None = None


# --------- low-level analysis ---------


def _is_useful_text_widget(name: str | None, value: str | None) -> bool:
    """Same heuristic as text_extract — count auto-named / empty widgets out."""
    if not value or not value.strip():
        return False
    if not name:
        return False
    n = name.strip().lower()
    if n.startswith(("undefined", "checkbox", "text_", "other_", "untitled", "field_")):
        return False
    if n.isdigit():
        return False
    return True


def _classify(features: PdfFeatures) -> str:
    """Pick a treatment class given the raw features."""
    # AcroForm with real filled fields → form-treatment regardless of page count
    if features.n_text_widgets_filled >= 3:
        return TREATMENT_ACROFORM_FILLED

    # No text and no widgets → image-only / scanned
    if features.total_text_chars < 200 and features.n_text_widgets_filled == 0:
        return TREATMENT_IMAGE_ONLY

    # 1-page low-density layout → architectural title-block
    if features.page_count == 1 and features.mean_chars_per_page < 1500:
        return TREATMENT_TITLEBLOCK

    # Multi-page narrative
    if features.page_count >= 6 and features.mean_chars_per_page >= 1500:
        return TREATMENT_NARRATIVE_LONG

    # Default: short narrative
    return TREATMENT_NARRATIVE_SHORT


def analyze_pdf(file_path: str | Path) -> PdfFeatures:
    """Open a PDF, extract characteristics + classify treatment.

    Cheap — only opens the file once and reads metadata + (page-by-page)
    text + form widgets. Does NOT cache; caller decides via
    `get_or_compute_features()`.
    """
    p = Path(file_path)
    notes: list[str] = []

    try:
        with pymupdf.open(p) as pdf:
            n = pdf.page_count
            meta = dict(pdf.metadata or {})

            # Aggregate text length per page
            total_chars = 0
            for idx in range(n):
                t = pdf[idx].get_text("text") or ""
                total_chars += len(t)

            # Inspect form widgets across every page
            n_text = 0
            n_text_filled = 0
            n_checkbox = 0
            for page in pdf:
                widgets = page.widgets()
                if not widgets:
                    continue
                for w in widgets:
                    type_str = (w.field_type_string or "").lower()
                    if "checkbox" in type_str:
                        n_checkbox += 1
                    elif "text" in type_str:
                        n_text += 1
                        if _is_useful_text_widget(w.field_name, w.field_value):
                            n_text_filled += 1

            mean = (total_chars // n) if n > 0 else 0

            features = PdfFeatures(
                page_count=n,
                total_text_chars=total_chars,
                mean_chars_per_page=mean,
                has_acroform=bool(pdf.is_form_pdf),
                n_text_widgets=n_text,
                n_text_widgets_filled=n_text_filled,
                n_checkbox_widgets=n_checkbox,
                pdf_producer=(meta.get("producer") or None),
                pdf_creator=(meta.get("creator") or None),
                pdf_format=(meta.get("format") or None),
                treatment=TREATMENT_UNSUPPORTED,  # placeholder; reclassified below
            )
    except Exception as exc:  # noqa: BLE001 — corrupt / encrypted PDFs
        logger.warning("pymupdf failed analyzing %s: %s", p, exc)
        return PdfFeatures(
            page_count=0,
            total_text_chars=0,
            mean_chars_per_page=0,
            has_acroform=False,
            n_text_widgets=0,
            n_text_widgets_filled=0,
            n_checkbox_widgets=0,
            pdf_producer=None,
            pdf_creator=None,
            pdf_format=None,
            treatment=TREATMENT_UNSUPPORTED,
            extraction_notes=f"pymupdf error: {exc}",
        )

    features.treatment = _classify(features)
    if notes:
        features.extraction_notes = "; ".join(notes)
    return features


# --------- public entrypoint with caching ---------


def get_or_compute_features(
    *,
    document_id: int,
    file_path: str | None,
    mime_type: str | None,
    force: bool = False,
) -> PdfFeatures:
    """Return cached features if present + analyzer_version matches, else
    compute + persist."""
    # Cache lookup
    if not force:
        with session_scope() as s:
            cached = s.execute(
                select(DocumentFeatures).where(DocumentFeatures.document_id == document_id)
            ).scalar_one_or_none()
            if cached is not None and cached.analyzer_version == ANALYZER_VERSION:
                return PdfFeatures(
                    page_count=cached.page_count or 0,
                    total_text_chars=cached.total_text_chars,
                    mean_chars_per_page=cached.mean_chars_per_page,
                    has_acroform=bool(cached.has_acroform),
                    n_text_widgets=cached.n_text_widgets,
                    n_text_widgets_filled=cached.n_text_widgets_filled,
                    n_checkbox_widgets=cached.n_checkbox_widgets,
                    pdf_producer=cached.pdf_producer,
                    pdf_creator=cached.pdf_creator,
                    pdf_format=cached.pdf_format,
                    treatment=cached.treatment,
                    extraction_notes=cached.extraction_notes,
                )

    # Compute
    features = _compute(file_path=file_path, mime_type=mime_type)

    # Persist (UPSERT via DELETE + INSERT to avoid the AS-new syntax we can't use)
    with session_scope() as s:
        s.execute(
            sql_text("DELETE FROM document_features WHERE document_id = :id"),
            {"id": document_id},
        )
        s.add(DocumentFeatures(
            document_id=document_id,
            analyzed_at=datetime.utcnow(),
            analyzer_version=ANALYZER_VERSION,
            mime_type=mime_type,
            page_count=features.page_count or None,
            total_text_chars=features.total_text_chars,
            mean_chars_per_page=features.mean_chars_per_page,
            has_acroform=features.has_acroform,
            n_text_widgets=features.n_text_widgets,
            n_text_widgets_filled=features.n_text_widgets_filled,
            n_checkbox_widgets=features.n_checkbox_widgets,
            pdf_producer=features.pdf_producer,
            pdf_creator=features.pdf_creator,
            pdf_format=features.pdf_format,
            treatment=features.treatment,
            extraction_notes=features.extraction_notes,
        ))
    return features


def _compute(*, file_path: str | None, mime_type: str | None) -> PdfFeatures:
    if not file_path:
        return PdfFeatures(0, 0, 0, False, 0, 0, 0, None, None, None,
                           TREATMENT_UNSUPPORTED, "no file_path on record")
    p = Path(file_path)
    if not p.exists():
        return PdfFeatures(0, 0, 0, False, 0, 0, 0, None, None, None,
                           TREATMENT_UNSUPPORTED, f"file not found: {file_path}")

    mt = (mime_type or "").lower()
    if mt.startswith("application/pdf") or p.suffix.lower() == ".pdf":
        return analyze_pdf(p)

    return PdfFeatures(0, 0, 0, False, 0, 0, 0, None, None, None,
                       TREATMENT_UNSUPPORTED, f"unsupported mime: {mime_type or p.suffix}")


# --------- routing: features + doc_type → template_key ---------


def pick_template_key(*, features: PdfFeatures, doc_type: str | None) -> str | None:
    """Return the prompt template_key to use, or None to skip the doc.

    Features are authoritative (the PDF doesn't lie); doc_type is a hint
    used only to pick between narrative templates.
    """
    if features.treatment in SKIP_TREATMENTS:
        return None

    if features.treatment == TREATMENT_ACROFORM_FILLED:
        # AcroForm wins regardless of council label — the form's own fields
        # are the most reliable source.
        return "da_form_1"

    if features.treatment == TREATMENT_TITLEBLOCK:
        return "plans"

    # Narrative — let doc_type pick the focus.
    dt = (doc_type or "").lower()
    if "decision notice" in dt or "delegated report" in dt:
        return "decision_notice"
    if "specialist" in dt:
        return "specialist"
    if "stamped approved plan" in dt or dt == "plans":
        return "plans"
    if "supporting" in dt or "cover letter" in dt:
        return "supporting"

    # Long narrative without a doc_type hint → specialist treatment is the
    # most thorough; short narrative → generic.
    if features.treatment == TREATMENT_NARRATIVE_LONG:
        return "specialist"
    return "generic"
