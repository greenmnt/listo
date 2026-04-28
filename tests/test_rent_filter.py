"""Verify rental listings get skipped in buy/sold parsing."""
from listo.parse.domain import _looks_like_rental as dom_rent_check


def test_per_week_detected():
    assert dom_rent_check("$980 per week")
    assert dom_rent_check("$1,200/wk")
    assert dom_rent_check("$1,200 / week")
    assert dom_rent_check("$1500 p.w.")
    assert dom_rent_check("$1500 PW")
    assert dom_rent_check("$5,000 weekly rent")
    assert dom_rent_check("$3500 pcm")


def test_sale_prices_not_treated_as_rent():
    assert not dom_rent_check("$980,000")
    assert not dom_rent_check("$1.5M")
    assert not dom_rent_check("Offers above $750,000")
    assert not dom_rent_check("Contact agent")
    assert not dom_rent_check(None)
    assert not dom_rent_check("")
