import pytest
from morpc_census.geos import SumLevel, Scope, SCOPES


class TestScope:
    def test_params_without_in(self):
        s = Scope(name="us", for_param="us:1")
        assert s.params == {"for": "us:1"}

    def test_params_with_in(self):
        s = Scope(name="franklin", for_param="county:049", in_param="state:39")
        assert s.params == {"for": "county:049", "in": "state:39"}

    def test_in_param_defaults_to_none(self):
        s = Scope(name="us", for_param="us:1")
        assert s.in_param is None

    def test_name_stored(self):
        s = Scope(name="myregion", for_param="county:049,051", in_param="state:39")
        assert s.name == "myregion"

    def test_params_does_not_include_in_when_none(self):
        s = Scope(name="us", for_param="us:1")
        assert "in" not in s.params


class TestSumLevel:
    def test_fields(self):
        s = SumLevel(name="county", sumlevel="050")
        assert s.name == "county"
        assert s.sumlevel == "050"

    def test_frozen(self):
        s = SumLevel(name="county", sumlevel="050")
        with pytest.raises(Exception):
            s.name = "tract"

    def test_equality(self):
        assert SumLevel(name="county", sumlevel="050") == SumLevel(name="county", sumlevel="050")

    def test_inequality(self):
        assert SumLevel(name="county", sumlevel="050") != SumLevel(name="tract", sumlevel="140")


class TestScopesDict:
    def test_all_values_are_scope_instances(self):
        assert all(isinstance(v, Scope) for v in SCOPES.values())

    def test_us_scope_present(self):
        assert "us" in SCOPES
        assert SCOPES["us"].for_param == "us:1"

    def test_us_scope_has_no_in_param(self):
        assert SCOPES["us"].in_param is None

    def test_columbuscbsa_scope_present(self):
        assert "columbuscbsa" in SCOPES

    def test_region_scopes_have_in_param(self):
        for key in ["region15", "region10", "region7", "regioncbsa", "regionmpo"]:
            assert SCOPES[key].in_param == "state:39", f"{key} missing in_param"

    def test_county_scopes_have_in_param(self):
        assert SCOPES["franklin"].in_param == "state:39"

    def test_scope_params_returns_dict(self):
        assert isinstance(SCOPES["us"].params, dict)
        assert "for" in SCOPES["us"].params
