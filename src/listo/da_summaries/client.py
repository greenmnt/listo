"""Ollama client wrapper.

Uses the official `ollama` Python package with `format=<JSON schema>` so
the LLM is constrained to emit JSON parseable by Pydantic. Has one
retry on parse failure with a "your last reply wasn't valid JSON" hint.

Defaults to `qwen2.5:7b-instruct` (4.7 GB Q4_K_M, fits the user's
RTX 4060 Mobile 8 GB). Override per-machine with the
`LISTO_OLLAMA_MODEL` env var so machine-A and machine-B can run
different models.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass

import ollama
from pydantic import ValidationError

from listo.da_summaries.schemas import DocFacts


logger = logging.getLogger(__name__)


DEFAULT_MODEL = os.environ.get("LISTO_OLLAMA_MODEL", "qwen2.5:7b-instruct")
DEFAULT_HOST = os.environ.get("LISTO_OLLAMA_HOST", "http://127.0.0.1:11434")


@dataclass
class ExtractResult:
    facts: DocFacts
    model: str
    raw_response: dict


class OllamaError(RuntimeError):
    """Wraps any Ollama-side failure with enough context to log + skip."""


class OllamaExtractor:
    def __init__(self, *, model: str | None = None, host: str | None = None):
        self.model = model or DEFAULT_MODEL
        self.client = ollama.Client(host=host or DEFAULT_HOST)
        self._schema = DocFacts.model_json_schema()

    def extract(self, *, system: str, user: str) -> ExtractResult:
        """Call the LLM and parse the response into DocFacts.

        Retries once on JSON-parse / Pydantic-validation failure with a
        nudge appended to the user message.
        """
        try:
            return self._call(system=system, user=user)
        except (json.JSONDecodeError, ValidationError) as first_err:
            nudge = (
                user
                + "\n\nYour last reply was not valid JSON matching the schema. "
                + "Reply ONLY with a JSON object — no prose, no code fences."
            )
            try:
                return self._call(system=system, user=nudge)
            except (json.JSONDecodeError, ValidationError) as second_err:
                raise OllamaError(
                    f"two consecutive JSON-parse failures: {first_err!r} → {second_err!r}"
                ) from second_err
        except Exception as exc:  # noqa: BLE001 — surface the raw client error
            raise OllamaError(f"ollama call failed: {exc!r}") from exc

    def _call(self, *, system: str, user: str) -> ExtractResult:
        resp = self.client.chat(
            model=self.model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            format=self._schema,
            options={
                "temperature": 0,
                "num_ctx": 8192,
                # num_predict default is 128 — way too tight for our 11-field
                # JSON. 768 leaves slack for long applicant/builder/agency
                # names + a 200-char project_description.
                "num_predict": 768,
            },
        )
        # ollama 0.6+ returns a ChatResponse object; older returns a dict.
        # Handle both.
        msg = resp["message"] if isinstance(resp, dict) else resp.message
        content = msg["content"] if isinstance(msg, dict) else msg.content
        facts = DocFacts.model_validate_json(content)
        # Build a JSON-safe raw response for the DB column.
        if hasattr(resp, "model_dump"):
            raw = resp.model_dump()
        elif isinstance(resp, dict):
            raw = resp
        else:
            raw = {"content": content}
        return ExtractResult(facts=facts, model=self.model, raw_response=raw)
