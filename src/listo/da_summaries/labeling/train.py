"""LayoutLMv3 token-classification trainer for DA entity extraction.

Reads `data/labeling/entity_train.jsonl` (one JSON record per PDF page,
emitted by `listo da export-labels`) and fine-tunes
`microsoft/layoutlmv3-base` to tag tokens with the BIO labels defined in
`labeling/schema.py` (17 labels: 8 roles × B/I + O).

Pipeline per record:
  1. Look up the PDF path via `council_application_documents.file_path`.
  2. Render that page to a PIL image with pymupdf.
  3. Feed words + bboxes + image into `LayoutLMv3Processor`
     (apply_ocr=False — we already have the layout).
  4. Align word-level BIO labels onto subword tokens — label the
     first subword of each word, set the rest to -100.

Splitting is stratified by doc_id (pages from the same PDF share
writing patterns and would leak across splits otherwise).
"""
from __future__ import annotations

import argparse
import json
import logging
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pymupdf  # type: ignore[import-untyped]
from PIL import Image
from sqlalchemy import text as sql_text
from transformers import (
    LayoutLMv3ForTokenClassification,
    LayoutLMv3Processor,
    Trainer,
    TrainingArguments,
)

from listo.db import session_scope
from listo.da_summaries.labeling.images import cache_path_for as _image_cache_path
from listo.da_summaries.labeling.schema import ID2LABEL, LABEL2ID, LABELS


logger = logging.getLogger(__name__)


# LayoutLMv3 spec: bboxes must be ints in [0, 1000]; image side 224.
BBOX_MAX = 1000
IMAGE_SIZE = 224
MODEL_ID = "microsoft/layoutlmv3-base"

# Set at CLI start; default mirrors model max. Lower it for tight-RAM
# CPU smoke tests (`--max-length 256`) since activations are quadratic
# in sequence length and dominate memory once the model + optimiser
# fit.
_MAX_LENGTH = 512

# Prepend metadata as zero-bbox prefix tokens so the model can
# condition on doc kind / council / vendor without us hardcoding it.
METADATA_FIELDS = ("kind", "type", "council", "state", "vendor")


@dataclass
class PageRecord:
    doc_id: int
    page: int
    metadata: dict[str, str | None]
    words: list[str]
    bboxes: list[list[float]]
    labels: list[str]
    page_w: float
    page_h: float


def _load_jsonl(path: Path) -> list[PageRecord]:
    records: list[PageRecord] = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            records.append(PageRecord(
                doc_id=r["doc_id"],
                page=r["page"],
                metadata=r.get("metadata") or {},
                words=r["words"],
                bboxes=r["bboxes"],
                labels=r["labels"],
                page_w=r["page_w"],
                page_h=r["page_h"],
            ))
    return records


def _resolve_pdf_paths(doc_ids: list[int]) -> dict[int, str]:
    """Bulk-fetch file_path for every distinct doc_id once, up-front,
    so we don't reopen the DB for every page."""
    out: dict[int, str] = {}
    if not doc_ids:
        return out
    with session_scope() as s:
        rows = s.execute(sql_text("""
            SELECT id, file_path
              FROM council_application_documents
             WHERE id IN :ids
        """).bindparams(__import__("sqlalchemy").bindparam("ids", expanding=True)),
            {"ids": doc_ids},
        ).fetchall()
        for row in rows:
            if row.file_path:
                out[row.id] = row.file_path
    return out


# Image cache directory — populated by `listo da render-training-images`
# server-side, then rsync'd to the GPU host. Set in main().
_IMAGE_CACHE_DIR: Path | None = None

# In-memory cache of recently-loaded images. Tiny (224×224 JPEG ≈ 30 KB
# decoded; cap at 4000 entries ≈ 600 MB worst case) and it survives across
# epochs so the dataloader doesn't re-decode every record on epoch 2+.
_IMAGE_CACHE: dict[tuple[int, int], Image.Image] = {}
_IMAGE_CACHE_MAX = 4000


def _render_page_image(doc_id: int, file_path: str | None, page_index: int) -> Image.Image | None:
    """Return the page image. Prefers the on-disk cache (populated
    server-side by the render-training-images CLI) and falls back to
    rendering from the PDF if the cache miss happens AND we have a
    valid PDF path."""
    cache_key = (doc_id, page_index)
    if cache_key in _IMAGE_CACHE:
        return _IMAGE_CACHE[cache_key]

    if _IMAGE_CACHE_DIR is not None:
        cache_file = _image_cache_path(_IMAGE_CACHE_DIR, doc_id, page_index)
        if cache_file.exists():
            try:
                img = Image.open(cache_file).convert("RGB")
                if len(_IMAGE_CACHE) < _IMAGE_CACHE_MAX:
                    _IMAGE_CACHE[cache_key] = img
                return img
            except Exception as exc:
                logger.warning("cache image unreadable %s: %s", cache_file, exc)

    if not file_path:
        return None
    try:
        with pymupdf.open(file_path) as pdf:
            if page_index >= pdf.page_count:
                return None
            page = pdf[page_index]
            pix = page.get_pixmap(dpi=72)
            img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
    except Exception as exc:
        logger.warning("render failed for %s p%d: %s", file_path, page_index, exc)
        return None
    img = img.resize((IMAGE_SIZE, IMAGE_SIZE), Image.Resampling.LANCZOS)
    if len(_IMAGE_CACHE) < _IMAGE_CACHE_MAX:
        _IMAGE_CACHE[cache_key] = img
    return img


def _normalise_bbox(b: list[float], page_w: float, page_h: float) -> list[int]:
    """Scale PDF-point bbox to [0, BBOX_MAX] integer range, clamp to
    valid bounds (x1≥x0, y1≥y0)."""
    x0, y0, x1, y1 = b
    nx0 = max(0, min(BBOX_MAX, int(x0 / page_w * BBOX_MAX)))
    ny0 = max(0, min(BBOX_MAX, int(y0 / page_h * BBOX_MAX)))
    nx1 = max(0, min(BBOX_MAX, int(x1 / page_w * BBOX_MAX)))
    ny1 = max(0, min(BBOX_MAX, int(y1 / page_h * BBOX_MAX)))
    if nx1 < nx0:
        nx0, nx1 = nx1, nx0
    if ny1 < ny0:
        ny0, ny1 = ny1, ny0
    return [nx0, ny0, nx1, ny1]


def _stratified_split(records: list[PageRecord], val_frac: float, seed: int):
    """Split by doc_id: every page of a given doc lands wholly in one
    side of the split. Avoids same-document leakage."""
    rng = random.Random(seed)
    doc_ids = sorted({r.doc_id for r in records})
    rng.shuffle(doc_ids)
    n_val = max(1, int(len(doc_ids) * val_frac))
    val_doc_ids = set(doc_ids[:n_val])
    train = [r for r in records if r.doc_id not in val_doc_ids]
    val = [r for r in records if r.doc_id in val_doc_ids]
    return train, val


def _build_inputs(
    record: PageRecord,
    pdf_path: str | None,
    processor,
):
    """Build a single tokenised example or return None to skip."""
    image = _render_page_image(record.doc_id, pdf_path, record.page)
    if image is None:
        return None

    # Prepend metadata as fake "words" with zero bbox. The processor
    # will tokenise + assign them position 0 layout-wise, which is the
    # convention for special-token-style inputs in LayoutLMv3.
    meta_words: list[str] = []
    meta_boxes: list[list[int]] = []
    meta_labels: list[str] = []
    for f in METADATA_FIELDS:
        val = record.metadata.get(f)
        if not val:
            continue
        meta_words.append(f"[{f.upper()}={val}]")
        meta_boxes.append([0, 0, 0, 0])
        meta_labels.append("O")

    body_boxes = [_normalise_bbox(b, record.page_w, record.page_h) for b in record.bboxes]
    words = meta_words + record.words
    boxes = meta_boxes + body_boxes
    word_labels = meta_labels + record.labels

    if not words:
        return None

    enc = processor(
        image,
        words,
        boxes=boxes,
        truncation=True,
        padding="max_length",
        max_length=_MAX_LENGTH,
        return_tensors=None,
    )

    # Strip the implicit batch dim that LayoutLMv3Processor adds for a
    # single image: pixel_values comes out as [1, 3, 224, 224] which
    # the default collator stacks into 5D and conv2d rejects.
    pv = enc.get("pixel_values")
    if pv is not None:
        # could be list[ndarray], ndarray, or torch.Tensor — handle each
        if isinstance(pv, list):
            if len(pv) == 1:
                enc["pixel_values"] = pv[0]
        else:
            arr = np.asarray(pv)
            if arr.ndim == 4 and arr.shape[0] == 1:
                enc["pixel_values"] = arr[0]
            else:
                enc["pixel_values"] = arr

    # Align word-level labels → subword token labels. word_ids() gives
    # the source-word index for every token; first subword of each
    # word inherits the BIO label, rest become -100 (ignored by loss).
    word_ids = enc.word_ids()
    aligned: list[int] = []
    prev_wid: int | None = None
    for wid in word_ids:
        if wid is None:
            aligned.append(-100)
        elif wid != prev_wid:
            tag = word_labels[wid] if wid < len(word_labels) else "O"
            aligned.append(LABEL2ID.get(tag, LABEL2ID["O"]))
        else:
            aligned.append(-100)
        prev_wid = wid
    enc["labels"] = aligned
    return enc


class _TaggedDataset:
    """Minimal torch-style Dataset that lazily builds inputs.
    Skips any record whose image (cache or PDF render) fails."""

    def __init__(
        self,
        records: list[PageRecord],
        pdf_paths: dict[int, str],
        processor,
        images_dir: Path | None,
    ):
        self.records = records
        self.pdf_paths = pdf_paths
        self.processor = processor
        # Records are usable if either a cached image exists OR we
        # have a PDF path to render from. With the cache populated
        # server-side, pdf_paths can be empty on a GPU host that
        # doesn't carry the PDFs — that's fine.
        def _usable(r: PageRecord) -> bool:
            if r.doc_id in pdf_paths:
                return True
            if images_dir is not None:
                return _image_cache_path(images_dir, r.doc_id, r.page).exists()
            return False
        self._usable = [r for r in records if _usable(r)]

    def __len__(self) -> int:
        return len(self._usable)

    def __getitem__(self, idx: int):
        rec = self._usable[idx]
        out = _build_inputs(rec, self.pdf_paths.get(rec.doc_id), self.processor)
        if out is None:
            # Fall back to the next usable record. Trainer with
            # remove_unused_columns=False expects a dict, so we can't
            # return None — wrap-around is the simplest recovery.
            return self.__getitem__((idx + 1) % len(self._usable))
        return out


def _compute_metrics_factory():
    """Closure over seqeval — imported lazily so the file is importable
    without seqeval (e.g. just to introspect the schema)."""
    from seqeval.metrics import classification_report, f1_score, precision_score, recall_score

    def compute(eval_pred):
        logits, labels = eval_pred
        preds = np.argmax(logits, axis=-1)
        true_seqs: list[list[str]] = []
        pred_seqs: list[list[str]] = []
        for p_row, l_row in zip(preds, labels):
            ts: list[str] = []
            ps: list[str] = []
            for p_id, l_id in zip(p_row, l_row):
                if l_id == -100:
                    continue
                ts.append(ID2LABEL[int(l_id)])
                ps.append(ID2LABEL[int(p_id)])
            true_seqs.append(ts)
            pred_seqs.append(ps)
        return {
            "precision": precision_score(true_seqs, pred_seqs),
            "recall": recall_score(true_seqs, pred_seqs),
            "f1": f1_score(true_seqs, pred_seqs),
            "report": classification_report(true_seqs, pred_seqs, digits=3),
        }
    return compute


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--jsonl", default="data/labeling/entity_train.jsonl")
    ap.add_argument("--out",   default="data/labeling/model")
    ap.add_argument("--epochs", type=int, default=4)
    ap.add_argument("--batch-size", type=int, default=4)
    ap.add_argument("--lr", type=float, default=5e-5)
    ap.add_argument("--val-frac", type=float, default=0.2)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--limit", type=int, default=0,
                    help="train on first N records only (smoke test)")
    ap.add_argument("--max-length", type=int, default=512,
                    help="processor sequence length — drop to 256 for tight CPU RAM")
    ap.add_argument("--images-dir", default="data/labeling/images",
                    help="pre-rendered page images (populated by `da render-training-images`); "
                         "skipped silently if the directory is empty + PDFs are accessible")
    args = ap.parse_args()

    global _MAX_LENGTH, _IMAGE_CACHE_DIR
    _MAX_LENGTH = args.max_length
    _IMAGE_CACHE_DIR = Path(args.images_dir) if args.images_dir else None

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    jsonl_path = Path(args.jsonl)
    out_path = Path(args.out)
    out_path.mkdir(parents=True, exist_ok=True)

    logger.info("loading %s", jsonl_path)
    records = _load_jsonl(jsonl_path)
    if args.limit > 0:
        records = records[: args.limit]
    logger.info("%d page records loaded", len(records))

    train_recs, val_recs = _stratified_split(records, args.val_frac, args.seed)
    logger.info("split: train=%d val=%d (by doc_id)", len(train_recs), len(val_recs))

    pdf_paths = _resolve_pdf_paths(sorted({r.doc_id for r in records}))
    logger.info("resolved %d / %d pdf paths",
                len(pdf_paths), len({r.doc_id for r in records}))

    logger.info("loading processor + base model: %s", MODEL_ID)
    # Direct class instead of AutoProcessor — auto-discovery makes
    # extra HTTP calls that trip a transformers 5.x + httpx bug
    # ("Cannot send a request, as the client has been closed").
    processor = LayoutLMv3Processor.from_pretrained(MODEL_ID, apply_ocr=False)
    model = LayoutLMv3ForTokenClassification.from_pretrained(
        MODEL_ID,
        num_labels=len(LABELS),
        id2label=ID2LABEL,
        label2id=LABEL2ID,
    )

    train_ds = _TaggedDataset(train_recs, pdf_paths, processor, _IMAGE_CACHE_DIR)
    val_ds = _TaggedDataset(val_recs, pdf_paths, processor, _IMAGE_CACHE_DIR)
    logger.info("usable: train=%d val=%d", len(train_ds), len(val_ds))
    if _IMAGE_CACHE_DIR is not None and _IMAGE_CACHE_DIR.is_dir():
        n_cached = len(list(_IMAGE_CACHE_DIR.glob("*.jpg")))
        logger.info("image cache: %d files in %s", n_cached, _IMAGE_CACHE_DIR)

    # warmup_steps = 10% of total optimizer steps (≈ old warmup_ratio=0.1)
    steps_per_epoch = max(1, len(train_ds) // max(1, args.batch_size))
    total_steps = steps_per_epoch * args.epochs
    warmup_steps = max(1, total_steps // 10)

    targs = TrainingArguments(
        output_dir=str(out_path),
        num_train_epochs=args.epochs,
        per_device_train_batch_size=args.batch_size,
        per_device_eval_batch_size=args.batch_size,
        learning_rate=args.lr,
        warmup_steps=warmup_steps,
        weight_decay=0.01,
        logging_steps=25,
        eval_strategy="epoch",
        save_strategy="epoch",
        save_total_limit=2,
        load_best_model_at_end=True,
        metric_for_best_model="f1",
        greater_is_better=True,
        seed=args.seed,
        report_to=[],
    )

    trainer = Trainer(
        model=model,
        args=targs,
        train_dataset=train_ds,
        eval_dataset=val_ds,
        processing_class=processor,
        compute_metrics=_compute_metrics_factory(),
    )

    trainer.train()
    metrics = trainer.evaluate()
    logger.info("final eval: %s", {k: v for k, v in metrics.items() if k != "report"})
    if "report" in metrics:
        logger.info("\n%s", metrics["report"])

    trainer.save_model(str(out_path))
    processor.save_pretrained(str(out_path))
    logger.info("saved to %s", out_path)


if __name__ == "__main__":
    main()
