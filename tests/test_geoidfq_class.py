import pytest
from morpc_census.geos import GeoIDFQ, SumLevel


COUNTY_GEOIDFQ   = "0500000US39049"               # Franklin County, Ohio
STATE_GEOIDFQ    = "0400000US39"                  # Ohio
PLACE_GEOIDFQ    = "1600000US3918000"             # Columbus city, Ohio
CBSA_GEOIDFQ     = "3100000US18140"               # Columbus CBSA
CD_GEOIDFQ       = "5001900US3912"                # Ohio 12th CD, 119th Congress
TRACT_GEOIDFQ    = "1400000US39041010100"         # Census tract 101, Delaware County
BLKGRP_GEOIDFQ   = "1500000US390410101001"        # Block group 1, tract 101, Delaware County
BLOCK_GEOIDFQ    = "1000000US390410101001000"     # Block 1000, block group 1, tract 101


class TestParse:
    def test_county_fields(self):
        g = GeoIDFQ.parse(COUNTY_GEOIDFQ)
        assert g.sumlevel == SumLevel("050")
        assert g.variant == "00"
        assert g.geocomp == "00"
        assert g.parts == {"state": "39", "county": "049"}

    def test_state_fields(self):
        g = GeoIDFQ.parse(STATE_GEOIDFQ)
        assert g.sumlevel == SumLevel("040")
        assert g.parts == {"state": "39"}

    def test_place_fields(self):
        g = GeoIDFQ.parse(PLACE_GEOIDFQ)
        assert g.sumlevel == SumLevel("160")
        assert g.parts == {"state": "39", "place": "18000"}

    def test_cbsa_fields(self):
        g = GeoIDFQ.parse(CBSA_GEOIDFQ)
        assert g.sumlevel == SumLevel("310")
        assert g.parts == {"cbsa": "18140"}

    def test_congressional_district_variant(self):
        g = GeoIDFQ.parse(CD_GEOIDFQ)
        assert g.sumlevel == SumLevel("500")
        assert g.variant == "19"        # 119th Congress (19 + 100)
        assert g.parts == {"state": "39", "cd": "12"}

    def test_tract_fields(self):
        g = GeoIDFQ.parse(TRACT_GEOIDFQ)
        assert g.sumlevel == SumLevel("140")
        assert g.parts == {"state": "39", "county": "041", "tract": "010100"}

    def test_block_group_fields(self):
        g = GeoIDFQ.parse(BLKGRP_GEOIDFQ)
        assert g.sumlevel == SumLevel("150")
        assert g.parts == {"state": "39", "county": "041", "tract": "010100", "blkgrp": "1"}

    def test_block_fields(self):
        g = GeoIDFQ.parse(BLOCK_GEOIDFQ)
        assert g.sumlevel == SumLevel("100")
        assert g.parts == {"state": "39", "county": "041", "tract": "010100", "blkgrp": "1", "block": "000"}

    def test_unknown_sumlevel_raises(self):
        with pytest.raises((KeyError, ValueError)):
            GeoIDFQ.parse("9990000US99")


class TestBuild:
    def test_county(self):
        g = GeoIDFQ.build("050", {"state": "39", "county": "049"})
        assert g.sumlevel == "050"
        assert g.variant == "00"
        assert g.parts == {"state": "39", "county": "049"}

    def test_tract(self):
        g = GeoIDFQ.build("140", {"state": "39", "county": "041", "tract": "010100"})
        assert g.parts == {"state": "39", "county": "041", "tract": "010100"}
        assert g.sumlevel == "140"

    def test_block_group(self):
        g = GeoIDFQ.build("150", {"state": "39", "county": "041", "tract": "010100", "blkgrp": "1"})
        assert g.sumlevel == "150"

    def test_block(self):
        g = GeoIDFQ.build("100", {"state": "39", "county": "041", "tract": "010100", "blkgrp": "1", "block": "000"})
        assert g.sumlevel == "100"

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

    def test_str_tract_roundtrip(self):
        assert str(GeoIDFQ.parse(TRACT_GEOIDFQ)) == TRACT_GEOIDFQ

    def test_str_block_group_roundtrip(self):
        assert str(GeoIDFQ.parse(BLKGRP_GEOIDFQ)) == BLKGRP_GEOIDFQ

    def test_str_block_roundtrip(self):
        assert str(GeoIDFQ.parse(BLOCK_GEOIDFQ)) == BLOCK_GEOIDFQ

    def test_geoid_county(self):
        assert GeoIDFQ.parse(COUNTY_GEOIDFQ).geoid == "39049"

    def test_geoid_state(self):
        assert GeoIDFQ.parse(STATE_GEOIDFQ).geoid == "39"

    def test_geoid_cbsa(self):
        assert GeoIDFQ.parse(CBSA_GEOIDFQ).geoid == "18140"

    def test_geoid_tract(self):
        assert GeoIDFQ.parse(TRACT_GEOIDFQ).geoid == "39041010100"

    def test_geoid_block_group(self):
        assert GeoIDFQ.parse(BLKGRP_GEOIDFQ).geoid == "390410101001"

    def test_geoid_block(self):
        assert GeoIDFQ.parse(BLOCK_GEOIDFQ).geoid == "390410101001000"

    def test_build_str_matches_parse(self):
        built = GeoIDFQ.build("050", {"state": "39", "county": "049"})
        assert str(built) == str(GeoIDFQ.parse(COUNTY_GEOIDFQ))

    def test_build_tract_str_matches_parse(self):
        built = GeoIDFQ.build("140", {"state": "39", "county": "041", "tract": "010100"})
        assert str(built) == TRACT_GEOIDFQ
