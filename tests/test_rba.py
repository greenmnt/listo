"""Verify RBA F5 CSV parser handles the real header structure."""
from datetime import date

from listo.rba import _parse_csv


# Mini fixture mirroring the real RBA F5 header structure (only 3 series, 4 months).
_FIXTURE_CSV = """﻿F5  INDICATOR LENDING RATES
Title,Lending rates; Housing loans; Banks; Variable; Standard; Owner-occupier,Lending rates; Housing loans; Banks; 3-year fixed; Owner-occupier,Lending rates; Personal loans; Term loans (unsecured); Variable
Description,desc1,desc2,desc3
Frequency,Monthly,Monthly,Monthly
Type,Original,Original,Original
Units,Per cent per annum,Per cent per annum,Per cent per annum

Source,RBA,RBA,RBA
Publication date,01-Jan-2026,01-Jan-2026,01-Jan-2026
Series ID,FILRHLBVS,FILRHL3YF,FILRPLTUV

31/01/1959,5.00,,
30/04/2020,4.52,3.20,15.30
31/12/2023,7.95,6.95,16.50
30/06/2024,8.40,6.85,16.50
"""


def test_parse_csv_extracts_series_and_data():
    series_ids, series_labels, data = _parse_csv(_FIXTURE_CSV)
    assert series_ids == ["FILRHLBVS", "FILRHL3YF", "FILRPLTUV"]
    assert "Owner-occupier" in series_labels[0]
    assert len(data) == 4

    # First row: 1959 has only the variable rate
    d, vals = data[0]
    assert d == date(1959, 1, 31)
    assert vals[0] == "5.00"
    assert vals[1] == ""
    assert vals[2] == ""

    # 2020 has all three
    d, vals = data[1]
    assert d == date(2020, 4, 30)
    assert vals == ["4.52", "3.20", "15.30"]


def test_parse_csv_handles_short_data_rows():
    """A data row with fewer cells than series should pad with empty strings."""
    short_csv = _FIXTURE_CSV.rstrip() + "\n31/07/2024,8.45\n"  # only 1 of 3 values
    series_ids, _, data = _parse_csv(short_csv)
    last = data[-1]
    assert last[0] == date(2024, 7, 31)
    assert len(last[1]) == len(series_ids)
    assert last[1][0] == "8.45"
    assert last[1][1] == ""
    assert last[1][2] == ""
