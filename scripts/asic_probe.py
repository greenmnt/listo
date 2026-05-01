"""Probe ASIC Connect Online registry search.

The page is Oracle ADF — native select_option / button.click don't
fire ADF's event listeners, so we drive the AdfPage component API
directly from the page context.
"""
from __future__ import annotations

import sys
from pathlib import Path

from patchright.sync_api import sync_playwright


LANDING_URL = "https://connectonline.asic.gov.au/RegistrySearch/"

# Main panel form ids (not the header search). Dropdown value "1" =
# "Organisation & Business Names" on the main panel.
DROPDOWN_ID = "bnConnectionTemplate:r1:0:searchPanelLanding:dc1:s1:searchTypesLovId"
TEXTBOX_ID = "bnConnectionTemplate:r1:0:searchPanelLanding:dc1:s1:searchForTextId"
BUTTON_ID = "bnConnectionTemplate:r1:0:searchPanelLanding:dc1:s1:searchButtonId"


def main(query: str = "602735446") -> None:
    out_dir = Path("data/asic_probe")
    out_dir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as pw:
        # Attach to the user's real Chrome on :9222 instead of launching a
        # fresh patchright chromium. Real fingerprint + warmed cookies =
        # invisible reCAPTCHA passes silently.
        browser = pw.chromium.connect_over_cdp("http://localhost:9222")
        ctx = browser.contexts[0]
        page = ctx.new_page()
        page.goto(LANDING_URL, wait_until="networkidle")

        # Wait for ADF to be ready before poking it.
        page.wait_for_function(
            "() => typeof AdfPage !== 'undefined' && AdfPage.PAGE && "
            "AdfPage.PAGE.findComponent('%s') !== null" % DROPDOWN_ID,
            timeout=15_000,
        )

        # patchright defaults to an isolated execution context; ADF lives
        # in the page's main world, so opt out.
        page.evaluate(
            "(id) => AdfPage.PAGE.findComponent(id).setValue('1')",
            DROPDOWN_ID,
            isolated_context=False,
        )
        # Dropdown has autoSubmit=true — it fires a PPR that re-renders
        # the form. Wait for it to settle before typing, otherwise the
        # re-render eats keystrokes.
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(1500)

        # Type into the visible input the way a user would — focus,
        # then keystrokes, then blur. ADF's inputText commits the value
        # to its model on blur (or on a real change event from the user
        # input pipeline), which a programmatic .fill() bypasses.
        page.click(f"[id='{TEXTBOX_ID}::content']")
        page.keyboard.type(query, delay=20)
        page.keyboard.press("Tab")  # blur → ADF commits to model

        # Snapshot just before submit.
        (out_dir / "05_pre_submit.html").write_text(page.content(), encoding="utf-8")
        page.screenshot(path=str(out_dir / "05_pre_submit.png"), full_page=True)

        page.locator(f"[id='{BUTTON_ID}']").click()
        # Snapshot every 5s for 30s so we can see what state we're in.
        for i in range(6):
            page.wait_for_timeout(5_000)
            (out_dir / f"06_t{i*5+5}s.html").write_text(page.content(), encoding="utf-8")
            page.screenshot(path=str(out_dir / f"06_t{i*5+5}s.png"), full_page=True)
            print(f"t={i*5+5}s url={page.url} title={page.title()}")
        (out_dir / "06_results.html").write_text(page.content(), encoding="utf-8")
        page.screenshot(path=str(out_dir / "06_results.png"), full_page=True)
        print("final url:", page.url)
        print("title:", page.title())

        page.close()
        browser.close()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "602735446")
