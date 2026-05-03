"""Classify a council_application_documents row into a DA-stage bucket.

The DA workflow is uniform across COGC applications:

    submission   → original Forms / Plans / Supporting Documents
    amendment    → "Amended X" — replaces a submission doc mid-flight
    ir_council   → council asks for more info ("Information Request",
                   "Action notice")
    ir_response  → applicant replies, often with revised plans
    further_info → secondary clarifications (typically .msg files)
    decision     → council issues outcome (Confirmation Notice early,
                   Decision Notice + Delegated Planner Report at the end,
                   plus Stamped Approved Plans, Infrastructure Charge
                   Notice, etc.)
    other        → didn't match anything

The matching is **ordered** — more specific patterns must come first
(e.g., "Response to IR Cover Letter" must be ir_response, not decision,
even though it contains "Cover Letter").
"""
from __future__ import annotations


def classify_doc_kind(doc_type: str | None) -> str:
    """Map a raw `doc_type` string to its DA workflow stage.

    Returns one of: 'submission', 'amendment', 'ir_council',
    'ir_response', 'further_info', 'decision', 'other'.
    """
    dt = (doc_type or "").lower().strip()
    if not dt:
        return "other"

    # 1. Response to IR — must come before any "decision" / cover-letter
    #    rule because "Response to IR Cover Letter" must NOT be decision.
    if "response to information" in dt or "response to ir" in dt:
        return "ir_response"

    # 2. Further info — distinct from primary IR loop.
    if "further information" in dt or "further info" in dt:
        return "further_info"

    # 3. Council's information request (NOT a response — already caught above).
    if "information request" in dt or "action notice" in dt:
        return "ir_council"

    # 4. Decision-stage docs.
    decision_markers = (
        "confirmation notice",
        "decision notice",
        "decision_notice",
        "decision cover letter",
        "cover letter decision",
        "decision notice cover",
        "decision notice to applicant",
        "delegated planner",
        "planner delegated",
        "planners delegated",
        "delegated report",
        "stamped approved",
        "final icn",
        "infrastructure charge",
        "completion certificate",
    )
    if any(m in dt for m in decision_markers):
        return "decision"

    # 5. Amended/revised versions of submission docs.
    if dt.startswith("amended") or "amended " in dt or "revised" in dt:
        return "amendment"

    # 6. Original submission docs — broad net last so it doesn't
    #    accidentally swallow IR or decision rows. "Forms", "Plans",
    #    "Supporting Documents", "Specialist Reports", "Code Assessment",
    #    "Statement of Landscape Intent", "Attachment X".
    submission_markers = (
        "form",
        "supporting document",
        "plan", "drawing",
        "specialist report",
        "code assessment",
        "landscape intent",
        "attachment",
        "section 1", "section 2", "section 3", "section 4",
        "owner consent", "owners consent", "owner's consent",
        "engineers certificate",
        "qleave",
        "application package",
        "operational works",
        "notice of intention",
        "notice of compliance",
    )
    if any(m in dt for m in submission_markers):
        return "submission"

    # 7. Cover Letter / Delegated Report variants are decision-y by
    #    default in COGC's vocabulary (when not "Response to IR Cover
    #    Letter" — already caught above as ir_response).
    if "cover letter" in dt or "delegated" in dt:
        return "decision"

    return "other"


# Canonical display order (for timeline visualisation).
DOC_KIND_ORDER = [
    "submission",
    "amendment",
    "ir_council",
    "ir_response",
    "further_info",
    "decision",
    "other",
]


# Per-kind glyph + label for terminal output.
DOC_KIND_LABEL = {
    "submission":   "submission",
    "amendment":    "amendment",
    "ir_council":   "ir-request (council)",
    "ir_response":  "ir-response (applicant)",
    "further_info": "further-info",
    "decision":     "decision",
    "other":        "other",
}
