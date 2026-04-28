from listo.address import normalize_address


def test_house_and_unit_share_match_key():
    house = normalize_address("17 Third Avenue", "Palm Beach", "4221")
    unit = normalize_address("2/17 Third Avenue", "Palm Beach", "4221")

    assert house.match_key == unit.match_key
    assert house.unit_number == ""
    assert unit.unit_number == "2"
    assert house.street_number == "17"
    assert unit.street_number == "17"


def test_suffix_abbreviation_canonicalizes():
    a = normalize_address("12 King Street", "Southport", "4215")
    b = normalize_address("12 King St", "Southport", "4215")
    assert a.match_key == b.match_key
    assert a.street_norm == "king st"


def test_suffix_table_coverage():
    cases = [
        ("Avenue", "ave"),
        ("Road", "rd"),
        ("Drive", "dr"),
        ("Court", "ct"),
        ("Parade", "pde"),
        ("Terrace", "tce"),
        ("Crescent", "cres"),
        ("Boulevard", "bvd"),
        ("Highway", "hwy"),
        ("Place", "pl"),
        ("Lane", "ln"),
        ("Close", "cl"),
        ("Way", "way"),
        ("Circuit", "cct"),
    ]
    for full, abbr in cases:
        addr = normalize_address(f"5 Sample {full}", "Carrara", "4211")
        assert addr.street_norm.endswith(abbr), f"{full} -> {addr.street_norm} (expected ends with {abbr})"


def test_alpha_street_suffix_treated_as_unit_indicator():
    """In AU practice '17A' is equivalent to '1/17' — half of a duplex on lot 17.
    Both notations should produce the SAME match_key so they cluster together
    with the original house at '17'."""
    house = normalize_address("17 Third Avenue", "Palm Beach", "4221")
    a = normalize_address("17A Third Avenue", "Palm Beach", "4221")
    b = normalize_address("17B Third Avenue", "Palm Beach", "4221")
    assert house.match_key == a.match_key == b.match_key  # same lot
    assert house.unit_number == ""
    assert a.unit_number == "a"
    assert b.unit_number == "b"
    assert a.street_number == "17"
    assert b.street_number == "17"


def test_alpha_suffix_clusters_with_slash_notation():
    """'17A' and '1/17' both refer to the same redev half — same match_key."""
    a = normalize_address("17A Third Avenue", "Palm Beach", "4221")
    slash = normalize_address("1/17 Third Avenue", "Palm Beach", "4221")
    assert a.match_key == slash.match_key
    # unit_numbers differ ('a' vs '1') but both point at lot 17


def test_unit_with_alpha_street_number_combined():
    """'3/17A' is rare but possible — combine the two unit indicators."""
    addr = normalize_address("3/17a Third Avenue", "Palm Beach", "4221")
    assert addr.unit_number == "3a"
    assert addr.street_number == "17"


def test_all_caps_suburb_normalizes():
    a = normalize_address("17 Third Avenue", "PALM BEACH", "4221")
    b = normalize_address("17 Third Avenue", "Palm Beach", "4221")
    assert a.match_key == b.match_key
    assert a.suburb_norm == "palm beach"


def test_punctuation_stripped():
    a = normalize_address("17 Third Avenue,", "Palm Beach.", "4221")
    b = normalize_address("17 Third Avenue", "Palm Beach", "4221")
    assert a.match_key == b.match_key


def test_multi_word_street():
    addr = normalize_address("88 Hooker Boulevard", "Broadbeach Waters", "4218")
    assert addr.street_norm == "hooker bvd"
    assert addr.suburb_norm == "broadbeach waters"
