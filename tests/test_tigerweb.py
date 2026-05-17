import pytest
from morpc_census.geos import SumLevel, Scope
from morpc_census.tigerweb import get_layer_url, get_tigerweb_layers_map, current_endpoints


class TestSumLevelParts:
    def test_county_parts(self):
        assert SumLevel("county").parts == ["state", "county"]

    def test_tract_parts(self):
        assert SumLevel("tract").parts == ["state", "county", "tract"]

    def test_block_group_parts(self):
        assert SumLevel("block group").parts == ["state", "county", "tract", "blkgrp"]

    def test_block_parts(self):
        assert SumLevel("block").parts == ["state", "county", "tract", "blkgrp", "block"]

    def test_state_parts(self):
        assert SumLevel("state").parts == ["state"]

    def test_place_parts(self):
        assert SumLevel("place").parts == ["state", "place"]

    def test_parts_from_code(self):
        assert SumLevel("050").parts == SumLevel("county").parts


class TestGetLayerUrl:
    def test_accepts_sumlevel_instance(self):
        sl = SumLevel("county")
        url = get_layer_url(sl)
        assert "MapServer" in url
        assert str(sl.tigerweb_name) in url.lower() or "82" in url

    def test_current_county_url(self):
        url = get_layer_url("counties")
        assert url.endswith("/82")

    def test_current_tracts_url(self):
        url = get_layer_url("tracts")
        assert url.endswith("/8")

    def test_invalid_survey_raises(self):
        with pytest.raises(ValueError):
            get_layer_url("counties", survey="XYZ")

    def test_invalid_layer_raises(self):
        with pytest.raises((ValueError, KeyError)):
            get_layer_url("bogus_layer_xyz")


class TestGetTigerwebLayersMap:
    def test_invalid_survey_raises(self):
        with pytest.raises(ValueError):
            get_tigerweb_layers_map(survey='XYZ')

    def test_acs_without_year_raises(self):
        with pytest.raises(ValueError):
            get_tigerweb_layers_map(year=None, survey='ACS')

    def test_dec_invalid_year_raises(self):
        with pytest.raises(ValueError):
            get_tigerweb_layers_map(year=2019, survey='DEC')

    @pytest.mark.network
    def test_current_endpoints_match_live_api(self):
        live = get_tigerweb_layers_map(survey='current')
        mismatches = {}
        for name, layer_id in current_endpoints.items():
            if name not in live:
                mismatches[name] = {'expected': layer_id, 'actual': 'MISSING'}
            elif live[name] != layer_id:
                mismatches[name] = {'expected': layer_id, 'actual': live[name]}
        assert not mismatches, f"current_endpoints has drifted from live TIGERweb API: {mismatches}"


class TestScopeSql:
    def test_us_returns_all(self):
        assert Scope("us").sql == "1=1"

    def test_county_scope_contains_county(self):
        result = Scope("franklin").sql
        assert "COUNTY" in result
        assert "049" in result

    def test_county_scope_contains_state(self):
        result = Scope("franklin").sql
        assert "STATE" in result.upper()

    def test_str_and_instance_consistent(self):
        assert Scope("franklin").sql == Scope("franklin").sql
