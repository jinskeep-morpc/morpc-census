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

    # --- lookup by name ---

    def test_from_name_fills_code(self):
        s = SumLevel("county")
        assert s.sumlevel == "050"
        assert s.name == "county"

    def test_from_name_tract(self):
        s = SumLevel("tract")
        assert s.sumlevel == "140"

    def test_from_name_equals_explicit(self):
        assert SumLevel("county") == SumLevel(name="county", sumlevel="050")

    def test_from_name_invalid_raises(self):
        with pytest.raises(ValueError):
            SumLevel("bogus")

    # --- lookup by three-digit code ---

    def test_from_code_fills_name(self):
        s = SumLevel("050")
        assert s.name == "county"
        assert s.sumlevel == "050"

    def test_from_code_tract(self):
        s = SumLevel("140")
        assert s.name == "tract"

    def test_from_code_equals_explicit(self):
        assert SumLevel("050") == SumLevel(name="county", sumlevel="050")

    def test_from_code_invalid_raises(self):
        with pytest.raises(ValueError):
            SumLevel("999")

    # --- optional metadata fields ---

    def test_optional_fields_default_to_none(self):
        s = SumLevel(name="county", sumlevel="050")
        assert s.singular is None
        assert s.plural is None
        assert s.hierarchy_string is None
        assert s.tigerweb_name is None

    def test_optional_fields_can_be_set(self):
        s = SumLevel(
            name="county", sumlevel="050",
            singular="county", plural="counties",
            hierarchy_string="COUNTY", tigerweb_name="counties",
        )
        assert s.singular == "county"
        assert s.plural == "counties"
        assert s.hierarchy_string == "COUNTY"
        assert s.tigerweb_name == "counties"

    def test_optional_fields_frozen(self):
        s = SumLevel(name="county", sumlevel="050", singular="county")
        with pytest.raises(Exception):
            s.singular = "parish"


class TestScopeFromName:
    def test_from_name_franklin(self):
        s = Scope("franklin")
        assert s.name == "franklin"
        assert s.for_param == SCOPES["franklin"].for_param
        assert s.in_param == SCOPES["franklin"].in_param

    def test_from_name_us(self):
        s = Scope("us")
        assert s.for_param == "us:1"
        assert s.in_param is None

    def test_from_name_region15(self):
        s = Scope("region15")
        assert s.in_param == "state:39"
        assert s.params == SCOPES["region15"].params

    def test_from_name_invalid_raises(self):
        with pytest.raises(ValueError):
            Scope("bogusplace")


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
