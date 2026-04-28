from pathlib import Path

from listo.parse.realestate import parse, peek_pagination

FIXTURES = Path(__file__).resolve().parents[1] / "scrape" / "fixtures"


def test_parse_extracts_listings_from_fixture():
    html = (FIXTURES / "realestate").read_text()
    page = parse(html)

    assert page.listings, "expected at least one listing"
    assert page.total_results and page.total_results > 0
    assert page.current_page == 1

    # Every parsed listing should have core identification fields
    for ls in page.listings:
        assert ls.source_listing_id
        assert ls.url
        assert ls.suburb
        assert ls.postcode
        assert ls.listing_kind in ("buy", "sold", "rent")


def test_known_listing_fields():
    """The fixture contains a known sold listing at 1003/472 Pacific Highway."""
    html = (FIXTURES / "realestate").read_text()
    page = parse(html)
    by_id = {ls.source_listing_id: ls for ls in page.listings}
    target = by_id.get("148888960")
    assert target is not None, "expected listing 148888960 in fixture"
    assert target.listing_kind == "sold"
    assert "Pacific Highway" in target.full_address
    assert target.suburb == "St Leonards"
    assert target.postcode == "2065"
    assert target.beds == 3
    assert target.baths == 2
    assert target.parking == 2
    assert "realestate.com.au" in target.url


def test_search_results_listings_have_urls():
    """Listings drawn from search results (not exclusiveShowcase) must have URLs."""
    html = (FIXTURES / "realestate").read_text()
    page = parse(html)
    # All listings should now have non-empty URL since we fall back to trackedCanonical
    assert all(ls.url for ls in page.listings), [
        (ls.source_listing_id, ls.url) for ls in page.listings if not ls.url
    ]


def test_peek_pagination_returns_page_info():
    html = (FIXTURES / "realestate").read_text()
    cur, mx = peek_pagination(html)
    assert cur == 1
    assert mx and mx > 1
