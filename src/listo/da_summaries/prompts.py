"""Per-doc-type prompt templates as a versioned registry.

A `template_key` identifies which template applies to a given doc type
('da_form_1', 'decision_notice', 'specialist', 'plans', 'supporting',
'generic'). The active set of templates is keyed by
`(prompt_version, template_key)` and is stored verbatim in the
`prompt_templates` table the first time each pair is used. Once a row
exists in the DB for a (version, key), changing the template in code is
ignored — bump the version instead. This locks the audit trail so we
always know what prompt produced any past summary.

The user template uses Python `str.format()`-style placeholders
`{text}` and `{app_id}`. We escape any literal `{` / `}` in the body
explicitly (none currently, but worth knowing).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import text as sql_text

from listo.db import session_scope


logger = logging.getLogger(__name__)


# ---------- system prompt (shared across template_keys for v1) ----------


SYSTEM_PROMPT_V1 = """You extract structured facts from Queensland \
Development Application (DA) documents.

Output ONLY valid JSON matching the provided schema. Do not include \
prose, code fences, or commentary outside the JSON.

If a field is not stated in the document text, use null. Do NOT invent, \
guess, or infer beyond what the text says.

Names should be written as they appear in the document — preserve \
spelling, capitalisation, and trailing 'Pty Ltd' / 'Pty. Ltd.' / etc.

If the document text contains a 'FORM FIELDS' block, the values there \
are the AUTHORITATIVE answers for those fields — they were typed \
directly into the form by the applicant. Treat them as ground truth and \
prefer them over anything inferred from surrounding narrative.

Conventional form-field labels you'll see and how to map them:
- 'Name' (under Applicant section) → applicant_name
- 'Property address' / 'Site address' → street_address
- 'Lot number' + 'Registered plan number' → lot_on_plan as 'Lot {{number}} {{RP|SP}}{{plan}}'.
  Example: 'Lot number: 376' + 'Registered plan number: RP21903' → lot_on_plan = 'Lot 376 RP21903'
- 'If applicable what is the proposed land use' / 'Proposed development' → project_description
- 'Builder' / 'Building Contractor' → builder_name
- 'Architect' / 'Designer' → architect_name

The proposed-land-use field also DETERMINES dwelling_count + dwelling_kind:
- 'Dual occupancy' or 'Dual Occupancy' → dwelling_kind='duplex', dwelling_count=2
- 'Secondary dwelling' or 'Granny flat' → dwelling_kind='granny', dwelling_count=1
- 'Multi-unit' / 'Multiple dwelling' (4+ units) → dwelling_kind='townhouses' or 'apartments', dwelling_count = the stated unit count
- 'Triplex' or 'Three dwellings' → dwelling_kind='triplex', dwelling_count=3

For dwelling_kind:
- 'house': single dwelling
- 'granny': secondary / granny / auxiliary dwelling on a lot with an existing house
- 'duplex': two attached dwellings on one lot ('dual occupancy')
- 'triplex': three attached dwellings
- 'townhouses': 4+ attached terrace-style dwellings
- 'apartments': multi-storey strata building
- 'mixed': combined residential + non-residential or combined kinds
- 'other': anything not covered (e.g. operational works, signage, vegetation)
- 'unknown': cannot tell from the text

Set confidence='high' when the document or its FORM FIELDS explicitly \
state the field, 'medium' when it's clearly implied (e.g. 'Dwelling A \
and Dwelling B' implies dwelling_count=2), 'low' otherwise."""


# ---------- per-template-key user prompts ----------


_USER_DA_FORM_1_V1 = """Document type: DA Form 1 (Queensland Development Application form).
This is the AUTHORITATIVE source for applicant_name, owner_name, lot_on_plan, \
street_address. builder_name is often blank on Form 1 — use null if so.

Application ID: {app_id}

--- DOCUMENT TEXT ---
{text}
--- END DOCUMENT TEXT ---

Extract every field stated in the document. project_description should \
be one or two sentences from the proposal / development description \
section if present."""


_USER_DECISION_NOTICE_V1 = """Document type: Signed Decision Notice or Delegated Report \
(council's official approval).
This is the AUTHORITATIVE source for approved dwelling_count and \
dwelling_kind. builder_name and architect_name are usually NOT in this \
document — leave them null unless explicitly named.

Application ID: {app_id}

--- DOCUMENT TEXT ---
{text}
--- END DOCUMENT TEXT ---

Pay particular attention to the 'Approved Development' / 'Decision' \
sections for dwelling_count and dwelling_kind. project_description \
should be the council's own one-line summary if present."""


_USER_SPECIALIST_V1 = """Document type: Specialist / Town-planning Report.
These reports often state builder, architect, applicant, and the full \
project description in detail. Use them to fill ANY field you can support \
from the text.

Application ID: {app_id}

--- DOCUMENT TEXT ---
{text}
--- END DOCUMENT TEXT ---"""


_USER_PLANS_V1 = """Document type: Architectural Plans (title-block on page 1).
The title-block typically lists: architect / drafter, project name, \
dwelling count, lot/plan, address. Other fields are usually NOT on the \
plans — leave them null.

Application ID: {app_id}

--- DOCUMENT TEXT ---
{text}
--- END DOCUMENT TEXT ---"""


_USER_SUPPORTING_V1 = """Document type: Supporting / Cover-letter Document.
Could be a planning report, statutory declaration, owner's consent, \
QBCC certificate, etc. Extract whatever facts are stated — null \
everything else.

Application ID: {app_id}

--- DOCUMENT TEXT ---
{text}
--- END DOCUMENT TEXT ---"""


_USER_GENERIC_V1 = """Application ID: {app_id}

--- DOCUMENT TEXT ---
{text}
--- END DOCUMENT TEXT ---

Extract any facts the schema asks for. Use null for anything not \
explicitly stated."""


# ---------- registry ----------


@dataclass(frozen=True)
class Template:
    template_key: str
    system_prompt: str
    user_template: str
    notes: str


# ---------- v2 system prompt: adds AU entity parsing (ACN/ABN/Pty Ltd boundary, c/-) ----------


SYSTEM_PROMPT_V2 = SYSTEM_PROMPT_V1 + """

ENTITY PARSING RULES (Australian conventions — applies to applicant_name, \
applicant_acn, applicant_abn, applicant_entity_type, applicant_agent_name, \
owner_name, owner_acn, owner_abn, owner_entity_type):

A typical applicant string looks like:
  'Great Southern Men Developments Pty Ltd (A.C.N 652 330 928) c/- HPC Planning'

Parse this into separate fields as follows:

1. The PRIMARY ENTITY ends at its company-suffix marker. Common suffixes:
   - 'Pty Ltd' / 'Pty. Ltd.' / 'Pty Limited'  → entity_type = 'company'
   - 'Limited' / 'Ltd'                         → entity_type = 'company'
   - 'Inc' / 'Inc.' / 'Corporation' / 'Corp'   → entity_type = 'company'
   Include the suffix in the *_name field. Example:
     applicant_name = 'Great Southern Men Developments Pty Ltd'

2. ACN (Australian Company Number) is 9 digits, often shown as
   '(A.C.N 652 330 928)' or 'ACN: 652330928' or 'ACN 652-330-928'.
   Extract DIGITS ONLY into applicant_acn / owner_acn:
     applicant_acn = '652330928'

3. ABN (Australian Business Number) is 11 digits, often 'ABN 12 345 678 901'.
   Extract DIGITS ONLY (11 chars):
     applicant_abn = '12345678901'

4. The 'c/-' marker (also written 'C/-' or 'care of') introduces a
   LODGING AGENT — a separate consultancy (often a town planner) that
   submitted the DA on behalf of the applicant. Their name goes in
   applicant_agent_name (NOT in applicant_name):
     applicant_agent_name = 'HPC Planning'

5. entity_type rules:
   - Has 'Pty Ltd' / 'Limited' / 'Inc' / etc.  → 'company'
   - Has 'ATF' (As Trustee For) / 'As Trustee For' / 'Family Trust'  → 'trust'
   - Plain 'Mr/Mrs/Ms/Dr Firstname Lastname'  → 'individual'
   - Plain personal name with no suffix       → 'individual'
   - Cannot tell                              → 'unknown'

If the field doesn't contain that piece (e.g. no ACN stated), use null \
for ACN/ABN and 'unknown' for entity_type."""


# v3 system prompt: same as v2 but the worked-example uses placeholder syntax
# instead of a real applicant name. The v2 example ("Great Southern Men
# Developments Pty Ltd ... c/- HPC Planning") is a real Gold Coast applicant
# from EDA/2021/97, and the 3B model — when fed sparse text (like a Plans PDF
# title-block) — was copying it verbatim into other DAs' applicant_name.
# Switched to abstract <PLACEHOLDER> syntax + an explicit "do not invent
# real-sounding names" instruction.
SYSTEM_PROMPT_V3 = SYSTEM_PROMPT_V1 + """

ENTITY PARSING RULES (Australian conventions — applies to applicant_name, \
applicant_acn, applicant_abn, applicant_entity_type, applicant_agent_name, \
owner_name, owner_acn, owner_abn, owner_entity_type):

A typical applicant string looks like:
  '<ENTITY_NAME> Pty Ltd (A.C.N <NINE DIGITS>) c/- <AGENT_NAME>'

Parse this into separate fields as follows:

1. The PRIMARY ENTITY ends at its company-suffix marker. Common suffixes:
   - 'Pty Ltd' / 'Pty. Ltd.' / 'Pty Limited'  → entity_type = 'company'
   - 'Limited' / 'Ltd'                         → entity_type = 'company'
   - 'Inc' / 'Inc.' / 'Corporation' / 'Corp'   → entity_type = 'company'
   Include the suffix in the *_name field — keep the entity exactly as \
written in the document.

2. ACN (Australian Company Number) is 9 digits, often shown as
   '(A.C.N <9 digits>)' or 'ACN: <9 digits>' or 'ACN <3>-<3>-<3>'.
   Extract DIGITS ONLY (no spaces, no hyphens, no parentheses) into \
applicant_acn / owner_acn.

3. ABN (Australian Business Number) is 11 digits, often 'ABN <2> <3> <3> <3>'.
   Extract DIGITS ONLY into applicant_abn / owner_abn.

4. The 'c/-' marker (also written 'C/-' or 'care of') introduces a
   LODGING AGENT — a separate consultancy (often a town planner) that
   submitted the DA on behalf of the applicant. Their name goes in
   applicant_agent_name (NOT in applicant_name).

5. entity_type rules:
   - Has 'Pty Ltd' / 'Limited' / 'Inc' / etc.  → 'company'
   - Has 'ATF' (As Trustee For) / 'As Trustee For' / 'Family Trust' / \
'The <Name> Trust' → 'trust'
   - Two or more personal names joined by 'and' / '&' → 'individual'
     (preserve them all in the name field, including any joined trust e.g. \
trust-plus-individuals as written in the document)
   - Plain 'Mr/Mrs/Ms/Dr Firstname Lastname'  → 'individual'
   - Cannot tell                              → 'unknown'

CRITICAL: The placeholders above (<ENTITY_NAME>, <AGENT_NAME>, <NINE DIGITS>, \
etc.) are syntax illustrations only. NEVER copy them into your output, and \
NEVER substitute a plausible-sounding real name for a placeholder. If the \
document does not state a field, use null. Do not guess from training data."""


# (prompt_version, template_key) -> Template
TEMPLATES: dict[tuple[str, str], Template] = {
    # v1 retained as historical record. New runs use v2 (set by
    # schemas.PROMPT_VERSION). Old summaries under v1 stay queryable
    # and can be cross-referenced against the v1 prompt body.
    ("v1", "da_form_1"): Template(
        "da_form_1", SYSTEM_PROMPT_V1, _USER_DA_FORM_1_V1,
        "v1: Covers DA Form 1 + Amended DA Form 1",
    ),
    ("v1", "decision_notice"): Template(
        "decision_notice", SYSTEM_PROMPT_V1, _USER_DECISION_NOTICE_V1,
        "v1: Covers Signed Decision Notice + Delegated Report (Gold Coast EDA)",
    ),
    ("v1", "specialist"): Template(
        "specialist", SYSTEM_PROMPT_V1, _USER_SPECIALIST_V1,
        "v1: Covers Specialist Reports / Town-planning Reports",
    ),
    ("v1", "plans"): Template(
        "plans", SYSTEM_PROMPT_V1, _USER_PLANS_V1,
        "v1: Covers Plans + Stamped Approved Plans (title-block extraction)",
    ),
    ("v1", "supporting"): Template(
        "supporting", SYSTEM_PROMPT_V1, _USER_SUPPORTING_V1,
        "v1: Covers Supporting Documents + Cover Letter",
    ),
    ("v1", "generic"): Template(
        "generic", SYSTEM_PROMPT_V1, _USER_GENERIC_V1,
        "v1: Catch-all for unmatched doc_types",
    ),
    # v2 adds entity parsing (ACN/ABN/Pty Ltd boundary/c/- agent) via
    # the new schema fields. User templates unchanged from v1.
    ("v2", "da_form_1"): Template(
        "da_form_1", SYSTEM_PROMPT_V2, _USER_DA_FORM_1_V1,
        "v2: + entity parsing (ACN/ABN/Pty Ltd boundary/c/- agent)",
    ),
    ("v2", "decision_notice"): Template(
        "decision_notice", SYSTEM_PROMPT_V2, _USER_DECISION_NOTICE_V1,
        "v2: + entity parsing",
    ),
    ("v2", "specialist"): Template(
        "specialist", SYSTEM_PROMPT_V2, _USER_SPECIALIST_V1,
        "v2: + entity parsing",
    ),
    ("v2", "plans"): Template(
        "plans", SYSTEM_PROMPT_V2, _USER_PLANS_V1,
        "v2: + entity parsing",
    ),
    ("v2", "supporting"): Template(
        "supporting", SYSTEM_PROMPT_V2, _USER_SUPPORTING_V1,
        "v2: + entity parsing",
    ),
    ("v2", "generic"): Template(
        "generic", SYSTEM_PROMPT_V2, _USER_GENERIC_V1,
        "v2: + entity parsing",
    ),
    # v3 — same user templates as v2, system prompt rewritten to remove the
    # 'Great Southern Men Developments' worked example (which the 3B model
    # was copying verbatim into unrelated DAs).
    ("v3", "da_form_1"): Template(
        "da_form_1", SYSTEM_PROMPT_V3, _USER_DA_FORM_1_V1,
        "v3: example placeholders + 'do not invent names' guard",
    ),
    ("v3", "decision_notice"): Template(
        "decision_notice", SYSTEM_PROMPT_V3, _USER_DECISION_NOTICE_V1,
        "v3: example placeholders + 'do not invent names' guard",
    ),
    ("v3", "specialist"): Template(
        "specialist", SYSTEM_PROMPT_V3, _USER_SPECIALIST_V1,
        "v3: example placeholders + 'do not invent names' guard",
    ),
    ("v3", "plans"): Template(
        "plans", SYSTEM_PROMPT_V3, _USER_PLANS_V1,
        "v3: example placeholders + 'do not invent names' guard",
    ),
    ("v3", "supporting"): Template(
        "supporting", SYSTEM_PROMPT_V3, _USER_SUPPORTING_V1,
        "v3: example placeholders + 'do not invent names' guard",
    ),
    ("v3", "generic"): Template(
        "generic", SYSTEM_PROMPT_V3, _USER_GENERIC_V1,
        "v3: example placeholders + 'do not invent names' guard",
    ),
}


def select_template_key(doc_type: str | None) -> str:
    """Map a council-side `doc_type` string to a registered template_key."""
    dt = (doc_type or "").lower()
    # 'Forms' (Gold Coast generic label) almost always wraps a DA Form 1
    # plus owner's consent etc. — so route it through the DA Form 1
    # template, which knows how to read the AcroForm fields.
    if "da form 1" in dt or dt == "forms":
        return "da_form_1"
    # Gold Coast 'Delegated Report' is the EDA fast-track equivalent of a
    # Decision Notice — same authority over dwelling_count.
    if "decision notice" in dt or "delegated report" in dt:
        return "decision_notice"
    if "specialist" in dt:
        return "specialist"
    if "stamped approved plan" in dt or dt == "plans":
        return "plans"
    if "supporting" in dt or "cover letter" in dt:
        return "supporting"
    return "generic"


def get_template(*, prompt_version: str, doc_type: str | None) -> Template:
    key = select_template_key(doc_type)
    tpl = TEMPLATES.get((prompt_version, key))
    if tpl is None:
        # Fall back to generic for the same version (raises if v1/generic missing)
        tpl = TEMPLATES[(prompt_version, "generic")]
    return tpl


def render(
    *,
    prompt_version: str,
    doc_type: str | None = None,
    template_key: str | None = None,
    text: str,
    app_id: str,
) -> tuple[Template, str]:
    """Render a prompt. Caller can pass `template_key` directly (preferred —
    upstream classifier picks the template based on PDF features), or
    `doc_type` for legacy doc-type-based routing.

    Returns (Template, rendered_user).
    """
    if template_key is not None:
        tpl = TEMPLATES.get((prompt_version, template_key))
        if tpl is None:
            tpl = TEMPLATES[(prompt_version, "generic")]
    else:
        tpl = get_template(prompt_version=prompt_version, doc_type=doc_type)
    # str.format() — escape any stray braces in `text`.
    safe_text = text.replace("{", "{{").replace("}", "}}")
    rendered_user = tpl.user_template.format(text=safe_text, app_id=app_id)
    return tpl, rendered_user


def register_templates(prompt_version: str | None = None) -> int:
    """INSERT IGNORE every template in TEMPLATES into `prompt_templates`.

    Templates are write-once per (version, key). If a row already exists,
    the in-code template is *not* used to overwrite it — that's the
    point. Returns the number of new rows inserted.

    Uses INSERT IGNORE for portability (the alternative
    `INSERT … AS new ON DUPLICATE KEY UPDATE` syntax requires
    MySQL 8.0.20+ which we can't assume).
    """
    now = datetime.utcnow()
    inserted = 0
    sql = sql_text("""
        INSERT IGNORE INTO prompt_templates
          (prompt_version, template_key, system_prompt, user_template, notes, first_used_at)
        VALUES
          (:prompt_version, :template_key, :system_prompt, :user_template, :notes, :first_used_at)
    """)
    with session_scope() as s:
        for (ver, key), tpl in TEMPLATES.items():
            if prompt_version and ver != prompt_version:
                continue
            res = s.execute(sql, {
                "prompt_version": ver,
                "template_key": tpl.template_key,
                "system_prompt": tpl.system_prompt,
                "user_template": tpl.user_template,
                "notes": tpl.notes,
                "first_used_at": now,
            })
            if res.rowcount == 1:
                inserted += 1
    if inserted:
        logger.info("registered %d new prompt template(s)", inserted)
    return inserted


# ---------- back-compat: build_prompt() returns (system, user) ----------


def build_prompt(*, doc_type: str | None, text: str, app_id: str) -> tuple[str, str]:
    """Legacy entrypoint kept as a thin wrapper. New code should call
    `render()` directly to get the template_key for persistence."""
    tpl, user = render(
        prompt_version="v1", doc_type=doc_type, text=text, app_id=app_id
    )
    return tpl.system_prompt, user
