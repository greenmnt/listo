from __future__ import annotations

import json
from typing import Any


def extract_argonaut_exchange(script_text: str) -> str | None:
    """Find `window.ArgonautExchange = { ... }` and extract the object literal.

    Brace-count from the first `{` after the assignment to find the matching `}`.
    Direct port of scrape/src/realestate.rs:24-43.
    """
    start = script_text.find("window.ArgonautExchange")
    if start < 0:
        return None
    brace_rel = script_text[start:].find("{")
    if brace_rel < 0:
        return None
    after_eq = start + brace_rel
    depth = 0
    for i, c in enumerate(script_text[after_eq:]):
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return script_text[after_eq : after_eq + i + 1]
    return None


def parse_stringified_json(s: str) -> Any:
    """Parse a string that may already be JSON, or may be a JSON-escaped string.

    Tries `json.loads(s)` first; on failure, wraps in quotes to unescape, then
    parses the unescaped result. Mirrors the Rust trio's defensive logic.
    """
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        unescaped = json.loads(f'"{s}"')
        return json.loads(unescaped)


def recursively_parse_json(value: Any) -> Any:
    """Walk a JSON-decoded structure; for any string that looks like JSON, parse it.

    Idempotent: repeated calls converge. Mirrors recursively_parse_json in the
    Rust source. Returns the (possibly-replaced) value.
    """
    if isinstance(value, str):
        s = value.lstrip()
        if s.startswith("{") or s.startswith("["):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                return value
            return recursively_parse_json(parsed)
        return value
    if isinstance(value, list):
        return [recursively_parse_json(v) for v in value]
    if isinstance(value, dict):
        return {k: recursively_parse_json(v) for k, v in value.items()}
    return value
