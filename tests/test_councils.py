from listo.councils import (
    council_for_postcode,
    councils,
    extract_approved_units,
    extract_internal_property_id,
    extract_type_code,
)


def test_council_loaded():
    cs = councils()
    assert "gold_coast" in cs
    gc = cs["gold_coast"]
    assert gc.state == "QLD"
    assert "4221" in gc.covers_postcodes  # Palm Beach
    assert gc.da_portal.system == "infor_epathway"


def test_council_for_postcode_gold_coast():
    c = council_for_postcode("4220")  # Burleigh Heads
    assert c is not None and c.slug == "gold_coast"
    assert council_for_postcode("9999") is None


def test_extract_approved_units_dual_occupancy():
    desc = "MATERIAL CHANGE OF USE CODE MCU201700973 PN62487/01/DA2 DUAL OCCUPANCY"
    assert extract_approved_units(desc) == 2


def test_extract_approved_units_triplex():
    assert extract_approved_units("Triplex development on Lot 21") == 3


def test_extract_approved_units_explicit_count():
    assert extract_approved_units("Multi-unit residential (3 units)") == 3
    assert extract_approved_units("MCU - 4 dwellings on small lot") == 4
    assert extract_approved_units("Approval for 8 townhouses") == 8


def test_extract_approved_units_none():
    assert extract_approved_units("Operational works - landscaping") is None
    assert extract_approved_units(None) is None
    assert extract_approved_units("") is None
    # "multi-unit" with no count → None (we don't fabricate)
    assert extract_approved_units("Multi-unit residential") is None


def test_extract_internal_property_id_gold_coast():
    desc = "MATERIAL CHANGE OF USE CODE MCU201700973 PN62487/01/DA2 DUAL OCCUPANCY"
    assert extract_internal_property_id(desc) == "PN62487"
    # spaces tolerated
    assert extract_internal_property_id("Reference PN 12345 something") == "PN12345"
    assert extract_internal_property_id("no property id here") is None


def test_extract_type_code():
    assert extract_type_code("MCU/2017/973") == "MCU"
    assert extract_type_code("OPW/2018/1200") == "OPW"
    assert extract_type_code("BWK/2019/55") == "BWK"
    assert extract_type_code("MCU/2007/2700139") == "MCU"
    assert extract_type_code(None) is None
    assert extract_type_code("notavalidformat") is None
