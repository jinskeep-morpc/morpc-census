"""Tests for GeoIDFQ.

Note: morpc.SUMLEVEL_DESCRIPTIONS geoidfq_format strings are inaccurate for
sumlevels 140 (tract) and 150 (block group) — they omit the COUNTY(3) component
that appears in real GEOIDFQs.  Tests here use only sumlevels with accurate
format strings: 040 (state), 050 (county), 160 (place), 310 (CBSA).
"""
import pytest
from morpc_census.geos import GeoIDFQ


COUNTY_GEOIDFQ    = "0500000US39049"          # Franklin County, Ohio
STATE_GEOIDFQ     = "0400000US39"             # Ohio
PLACE_GEOIDFQ     = "1600000US3918000"        # Columbus city, Ohio
CBSA_GEOIDFQ      = "3100000US18140"          # Columbus CBSA
CD_GEOIDFQ        = "5001900US3912"           # Ohio 12th congressional district, 119th Congress


class TestParse:
    def test_county_fields(self):
        g = GeoIDFQ.parse(COUNTY_GEOIDFQ)
        assert g.sumlevel == "050"
        assert g.variant == "00"
        assert g.geocomp == "00"
        assert g.parts == {"state": "39", "county": "049"}

    def test_state_fields(self):
        g = GeoIDFQ.parse(STATE_GEOIDFQ)
        assert g.sumlevel == "040"
        assert g.parts == {"state": "39"}

    def test_place_fields(self):
        g = GeoIDFQ.parse(PLACE_GEOIDFQ)
        assert g.sumlevel == "160"
        assert g.parts == {"state": "39", "place": "18000"}

    def test_cbsa_fields(self):
        g = GeoIDFQ.parse(CBSA_GEOIDFQ)
        assert g.sumlevel == "310"
        assert g.parts == {"cbsa": "18140"}

    def test_congressional_district_variant(self):
        g = GeoIDFQ.parse(CD_GEOIDFQ)
        assert g.sumlevel == "500"
        assert g.variant == "19"        # 119th Congress (19 + 100)
        assert g.parts == {"state": "39", "cd": "12"}

    def test_unknown_sumlevel_raises(self):
        with pytest.raises((KeyError, ValueError)):
            GeoIDFQ.parse("9990000US99")


class TestBuild:
    def test_county(self):
        g = GeoIDFQ.build("050", {"state": "39", "county": "049"})
        assert g.sumlevel == "050"
        assert g.variant == "00"
        assert g.parts == {"state": "39", "county": "049"}

    def test_variant_passthrough(self):
        g = GeoIDFQ.build("500", {"state": "39", "cd": "12"}, variant="19")
        assert g.variant == "19"

    def test_geocomp_passthrough(self):
        g = GeoIDFQ.build("050", {"state": "39", "county": "049"}, geocomp="H0")
        assert g.geocomp == "H0"

    def test_wrong_keys_raises(self):
        with pytest.raises(ValueError):
            GeoIDFQ.build("050", {"state": "39"})           # missing county

    def test_extra_keys_raises(self):
        with pytest.raises(ValueError):
            GeoIDFQ.build("050", {"state": "39", "county": "049", "tract": "000100"})

    def test_morpc_sumlevel_raises(self):
        with pytest.raises(ValueError):
            GeoIDFQ.build("M10", {"jurisid": "12345"})


class TestStrAndGeoid:
    def test_str_county_roundtrip(self):
        assert str(GeoIDFQ.parse(COUNTY_GEOIDFQ)) == COUNTY_GEOIDFQ

    def test_str_state_roundtrip(self):
        assert str(GeoIDFQ.parse(STATE_GEOIDFQ)) == STATE_GEOIDFQ

    def test_str_place_roundtrip(self):
        assert str(GeoIDFQ.parse(PLACE_GEOIDFQ)) == PLACE_GEOIDFQ

    def test_str_cbsa_roundtrip(self):
        assert str(GeoIDFQ.parse(CBSA_GEOIDFQ)) == CBSA_GEOIDFQ

    def test_str_cd_roundtrip(self):
        assert str(GeoIDFQ.parse(CD_GEOIDFQ)) == CD_GEOIDFQ

    def test_geoid_county(self):
        assert GeoIDFQ.parse(COUNTY_GEOIDFQ).geoid == "39049"

    def test_geoid_state(self):
        assert GeoIDFQ.parse(STATE_GEOIDFQ).geoid == "39"

    def test_geoid_cbsa(self):
        assert GeoIDFQ.parse(CBSA_GEOIDFQ).geoid == "18140"

    def test_build_str_matches_parse(self):
        built = GeoIDFQ.build("050", {"state": "39", "county": "049"})
        parsed = GeoIDFQ.parse(COUNTY_GEOIDFQ)
        assert str(built) == str(parsed)
