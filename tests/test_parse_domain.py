import json
from pathlib import Path

from listo.parse.domain import parse_next_data

FIXTURES = Path(__file__).resolve().parents[1] / "scrape" / "fixtures"


def _load() -> dict:
    return json.loads((FIXTURES / "domain.json").read_text())


def test_parse_extracts_listings():
    nd = _load()
    page = parse_next_data(nd)
    assert page.listings, "expected listings"
    assert page.current_page == 1
    assert page.total_pages and page.total_pages > 1
    assert page.total_results and page.total_results > 0


def test_known_burleigh_listing():
    """Fixture has '802/29 Hill Avenue, Burleigh Heads' sold by private treaty 04 Dec 2025."""
    nd = _load()
    page = parse_next_data(nd)
    by_id = {ls.source_listing_id: ls for ls in page.listings}
    target = by_id.get("2020299115")
    assert target is not None, "expected listing 2020299115 in fixture"
    assert target.full_address == "802/29 Hill Avenue"
    assert target.suburb.lower() == "burleigh heads"
    assert target.postcode == "4220"
    assert target.state == "QLD"
    assert target.listing_kind == "sold"
    assert target.beds == 2
    assert target.baths == 1
    assert target.parking == 1
    assert target.price_min == 915_000
    assert target.sold_price == 915_000
    assert target.sale_method == "private treaty"
    assert target.sold_date is not None
    assert target.sold_date.year == 2025
    assert target.sold_date.month == 12
    assert target.sold_date.day == 4
    assert target.url == "https://www.domain.com.au/802-29-hill-avenue-burleigh-heads-qld-4220-2020299115"


def test_listings_have_urls():
    nd = _load()
    page = parse_next_data(nd)
    assert all(ls.url and ls.url.startswith("http") for ls in page.listings)
