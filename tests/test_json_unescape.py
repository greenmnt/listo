from pathlib import Path

from listo.parse.json_unescape import recursively_parse_json
from listo.parse.realestate import extract_argonaut_root, extract_urql_cache

FIXTURES = Path(__file__).resolve().parents[1] / "scrape" / "fixtures"


def test_extract_argonaut_root_from_real_html():
    html = (FIXTURES / "realestate").read_text()
    root = extract_argonaut_root(html)
    assert isinstance(root, dict)
    assert "resi-property_listing-experience-web" in root


def test_urql_cache_resolves_to_dict_tree():
    html = (FIXTURES / "realestate").read_text()
    cache = extract_urql_cache(html)
    assert isinstance(cache, dict)
    # The cache is keyed by query hash IDs; at least one entry has 'data'
    assert any(isinstance(v, dict) and "data" in v for v in cache.values())


def test_recursively_parse_json_idempotent():
    nested_str = '{"outer":"{\\"inner\\":[1,2,3]}"}'
    import json as _j

    once = recursively_parse_json(_j.loads(nested_str))
    twice = recursively_parse_json(once)
    assert once == twice
    assert once == {"outer": {"inner": [1, 2, 3]}}
