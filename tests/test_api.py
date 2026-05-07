"""
Tests for pure/offline functions in morpc_census.api.

Network-dependent functions are tested with mocked dependencies.
"""

import pytest
import pandas as pd
from unittest.mock import patch

from morpc_census.api import (
    valid_survey_table,
    get_params,
    censusapi_name,
    find_replace_variable_map,
    DimensionTable,
    valid_vintage,
    SurveyTable,
    Vintage,
    Group,
    CensusAPI,
    IMPLEMENTED_ENDPOINTS,
)


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------

def _make_long():
    """Minimal LONG DataFrame suitable for DimensionTable tests."""
    return pd.DataFrame({
        'variable': ['B01_001', 'B01_002', 'B01_003'],
        'variable_label': ['Total:', 'Total:!!Male:', 'Total:!!Female:'],
        'GEO_ID': ['0500000US39049'] * 3,
        'NAME': ['Franklin County, Ohio'] * 3,
        'concept': ['Test concept'] * 3,
        'universe': ['Population'] * 3,
        'survey': ['acs/acs5'] * 3,
        'reference_period': [2023] * 3,
        'estimate': [100, 50, 50],
        'moe': [5, 3, 3],
    })


# ---------------------------------------------------------------------------
# TestValidSurveyTable
# ---------------------------------------------------------------------------

class TestValidSurveyTable:
    def test_returns_true_for_acs5(self):
        assert valid_survey_table('acs/acs5') is True

    def test_returns_true_for_dec_pl(self):
        assert valid_survey_table('dec/pl') is True

    def test_returns_true_for_every_implemented_endpoint(self):
        for endpoint in IMPLEMENTED_ENDPOINTS:
            assert valid_survey_table(endpoint) is True

    def test_raises_for_unknown_endpoint(self):
        with pytest.raises(ValueError, match="not available or not yet implemented"):
            valid_survey_table('acs/acs99')

    def test_raises_for_empty_string(self):
        with pytest.raises(ValueError):
            valid_survey_table('')

    def test_raises_for_partial_match(self):
        with pytest.raises(ValueError):
            valid_survey_table('acs')


# ---------------------------------------------------------------------------
# TestGetParams
# ---------------------------------------------------------------------------

class TestGetParams:
    def test_no_variables_returns_group_string(self):
        assert get_params('B01001') == 'group(B01001)'

    def test_none_variables_returns_group_string(self):
        assert get_params('B01001', variables=None) == 'group(B01001)'

    def test_variable_list_returns_comma_joined(self):
        result = get_params('B01001', variables=['B01001_001E', 'B01001_002E'])
        assert result == 'B01001_001E,B01001_002E'

    def test_single_variable_no_trailing_comma(self):
        assert get_params('B01001', variables=['B01001_001E']) == 'B01001_001E'

    def test_group_string_contains_group_code(self):
        result = get_params('B25003')
        assert 'B25003' in result
        assert result.startswith('group(')


# ---------------------------------------------------------------------------
# TestCensusapiName
# ---------------------------------------------------------------------------

class TestCensusapiName:
    def test_no_sumlevel_no_variables(self):
        name = censusapi_name('acs/acs5', 2023, 'franklin', 'B01001')
        assert name == 'census-acs-acs5-2023-franklin-b01001'

    def test_with_sumlevel_tract(self):
        # HIERARCHY_STRING_FROM_CENSUSNAME['tract'] == 'COUNTY-TRACT'
        # 'COUNTY-TRACT'.replace('-', '').lower() == 'countytract'
        name = censusapi_name('acs/acs5', 2023, 'franklin', 'B01001', sumlevel='tract')
        assert name == 'census-acs-acs5-2023-countytract-franklin-b01001'

    def test_with_sumlevel_county(self):
        # HIERARCHY_STRING_FROM_CENSUSNAME['county'] == 'COUNTY'
        name = censusapi_name('acs/acs5', 2023, 'ohio', 'B01001', sumlevel='county')
        assert name == 'census-acs-acs5-2023-county-ohio-b01001'

    def test_with_variables_appends_suffix(self):
        name = censusapi_name(
            'acs/acs5', 2023, 'franklin', 'B01001',
            variables=['B01001_001E', 'B01001_002E'],
        )
        assert name.endswith('-select-variables')

    def test_no_variables_no_suffix(self):
        name = censusapi_name('acs/acs5', 2023, 'franklin', 'B01001')
        assert 'select-variables' not in name

    def test_sumlevel_and_variables_combined(self):
        name = censusapi_name(
            'acs/acs5', 2023, 'franklin', 'B01001',
            sumlevel='tract',
            variables=['B01001_001E'],
        )
        assert 'countytract' in name
        assert name.endswith('-select-variables')

    def test_result_is_lowercase(self):
        name = censusapi_name('acs/acs5', 2023, 'Franklin', 'B01001')
        assert name == name.lower()

    def test_dec_survey_table(self):
        name = censusapi_name('dec/pl', 2020, 'ohio', 'P1')
        assert name == 'census-dec-pl-2020-ohio-p1'

    def test_accepts_scope_instance(self):
        from morpc_census.geos import Scope
        name = censusapi_name('acs/acs5', 2023, Scope('franklin'), 'B01001')
        assert name == 'census-acs-acs5-2023-franklin-b01001'

    def test_accepts_sumlevel_instance(self):
        from morpc_census.geos import SumLevel
        name = censusapi_name('acs/acs5', 2023, 'ohio', 'B01001', sumlevel=SumLevel('county'))
        assert name == 'census-acs-acs5-2023-county-ohio-b01001'

    def test_scope_instance_matches_string(self):
        from morpc_census.geos import Scope
        assert (
            censusapi_name('acs/acs5', 2023, Scope('franklin'), 'B01001')
            == censusapi_name('acs/acs5', 2023, 'franklin', 'B01001')
        )

    def test_sumlevel_instance_matches_string(self):
        from morpc_census.geos import SumLevel
        assert (
            censusapi_name('acs/acs5', 2023, 'ohio', 'B01001', sumlevel=SumLevel('county'))
            == censusapi_name('acs/acs5', 2023, 'ohio', 'B01001', sumlevel='county')
        )


# ---------------------------------------------------------------------------
# TestFindReplaceVariableMap
# ---------------------------------------------------------------------------

class TestFindReplaceVariableMap:
    def test_basic_replacement(self):
        labels = ['Total!!Male', 'Total!!Female']
        variables = ['B01001_002E', 'B01001_026E']
        new_labels, _ = find_replace_variable_map(labels, variables, {'Male': 'Men', 'Female': 'Women'})
        assert new_labels == ['Total!!Men', 'Total!!Women']

    def test_new_variable_codes_are_sequential(self):
        labels = ['Total!!Male', 'Total!!Female']
        variables = ['B01001_002E', 'B01001_026E']
        _, new_vars = find_replace_variable_map(labels, variables, {'Male': 'Men', 'Female': 'Women'})
        assert new_vars == ['B01001_M00', 'B01001_M01']

    def test_unmatched_label_unchanged(self):
        labels = ['Total:', 'Total:!!Male:']
        variables = ['B01001_001E', 'B01001_002E']
        new_labels, _ = find_replace_variable_map(labels, variables, {'Female': 'Women'})
        assert new_labels == ['Total:', 'Total:!!Male:']

    def test_duplicate_new_labels_share_variable_code(self):
        labels = ['Total!!Male!!Under5', 'Total!!Male!!5to9']
        variables = ['B01001_003E', 'B01001_004E']
        new_labels, new_vars = find_replace_variable_map(
            labels, variables, {'Under5': 'Youth', '5to9': 'Youth'}
        )
        assert new_labels == ['Total!!Male!!Youth', 'Total!!Male!!Youth']
        assert new_vars[0] == new_vars[1]

    def test_var_id_prefix_comes_from_first_variable(self):
        labels = ['A', 'B']
        variables = ['C17002_001E', 'C17002_002E']
        _, new_vars = find_replace_variable_map(labels, variables, {})
        assert all(v.startswith('C17002_M') for v in new_vars)


# ---------------------------------------------------------------------------
# TestDimensionTableDescriptionTable
# ---------------------------------------------------------------------------

class TestDimensionTableDescriptionTable:
    def test_returns_dataframe(self):
        desc = DimensionTable(_make_long()).create_description_table()
        assert isinstance(desc, pd.DataFrame)

    def test_indexed_by_variable(self):
        long = _make_long()
        desc = DimensionTable(long).create_description_table()
        assert desc.index.name == 'variable'
        assert set(long['variable'].unique()).issubset(set(desc.index))

    def test_splits_double_bang_into_columns(self):
        desc = DimensionTable(_make_long()).create_description_table()
        assert desc.shape[1] >= 2

    def test_row_count_matches_unique_variables(self):
        long = _make_long()
        desc = DimensionTable(long).create_description_table()
        assert len(desc) == long['variable'].nunique()

    def test_single_level_label_lands_in_first_column(self):
        desc = DimensionTable(_make_long()).create_description_table()
        assert desc.loc['B01_001'].iloc[0] != ''


# ---------------------------------------------------------------------------
# TestValidVintage (mocked)
# ---------------------------------------------------------------------------

class TestValidVintage:
    _endpoints = {'acs/acs5': [2022, 2023]}

    def test_valid_year_int_returns_true(self):
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=self._endpoints):
            assert valid_vintage('acs/acs5', 2023) is True

    def test_valid_year_string_returns_true(self):
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=self._endpoints):
            assert valid_vintage('acs/acs5', '2022') is True

    def test_invalid_year_raises_value_error(self):
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=self._endpoints):
            with pytest.raises(ValueError, match="not an available vintage"):
                valid_vintage('acs/acs5', 2019)

    def test_unknown_survey_table_raises_value_error(self):
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=self._endpoints):
            with pytest.raises(ValueError):
                valid_vintage('acs/acs1', 2023)


# ---------------------------------------------------------------------------
# TestCensusAPIClassNormalization
# ---------------------------------------------------------------------------

class TestCensusAPIClassNormalization:
    """Test that CensusAPI normalizes scope/sumlevel strings to class instances."""

    _fake_vars = {'B01001_001E': {'label': 'Total:'}}
    _fake_groups = {'B01001': {'description': 'Sex by Age', 'variables': ''}}
    _fake_data = pd.DataFrame({'GEO_ID': ['0500000US39049'], 'NAME': ['Franklin County']})

    _fake_endpoints = {'acs/acs5': [2022, 2023]}

    def _make(self, scope, sumlevel=None):
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=self._fake_endpoints), \
             patch('morpc_census.api.get_table_groups', return_value=self._fake_groups), \
             patch('morpc_census.api.get_group_universe', return_value='All people'), \
             patch('morpc_census.api.get_group_variables', return_value=self._fake_vars), \
             patch('morpc_census.api.get_api_request', return_value={'url': 'http://x', 'params': {'get': 'group(B01001)', 'for': 'county:049'}}), \
             patch('morpc_census.api.fetch', return_value=self._fake_data):
            return CensusAPI('acs/acs5', 2023, 'B01001', scope, sumlevel=sumlevel, return_long=False)

    def test_scope_string_stored_as_scope_instance(self):
        from morpc_census.geos import Scope
        api = self._make('franklin')
        assert isinstance(api.SCOPE, Scope)

    def test_scope_instance_passed_through(self):
        from morpc_census.geos import Scope
        sc = Scope('franklin')
        api = self._make(sc)
        assert api.SCOPE is sc

    def test_scope_name_is_correct(self):
        api = self._make('franklin')
        assert api.SCOPE.name == 'franklin'

    def test_sumlevel_string_stored_as_sumlevel_instance(self):
        from morpc_census.geos import SumLevel
        api = self._make('franklin', sumlevel='county')
        assert isinstance(api.SUMLEVEL, SumLevel)

    def test_sumlevel_none_stays_none(self):
        api = self._make('franklin')
        assert api.SUMLEVEL is None

    def test_sumlevel_instance_passed_through(self):
        from morpc_census.geos import SumLevel
        sl = SumLevel('county')
        api = self._make('franklin', sumlevel=sl)
        assert api.SUMLEVEL is sl

    def test_sumlevel_name_is_correct(self):
        from morpc_census.geos import SumLevel
        api = self._make('franklin', sumlevel='county')
        assert api.SUMLEVEL.name == 'county'
        assert isinstance(api.SUMLEVEL, SumLevel)

    def test_scope_obj_returns_scope_instance(self):
        from morpc_census.geos import Scope
        api = self._make('franklin')
        assert isinstance(api.scope_obj, Scope)
        assert api.scope_obj is api.SCOPE

    def test_create_resource_title_uses_sumlevel_plural_and_scope_name(self):
        import frictionless
        api = self._make('franklin', sumlevel='county')
        api.CONCEPT = 'Sex by Age'
        api.FILENAME = 'test.csv'
        api.SCHEMA_FILENAME = 'test.schema.yaml'
        captured = {}
        with patch.object(frictionless.Resource, 'from_descriptor', side_effect=lambda d: captured.update(d)):
            api.create_resource()
        assert 'franklin' in captured['title']
        assert 'counties' in captured['title']  # SumLevel('county').plural


# ---------------------------------------------------------------------------
# TestSurveyTable
# ---------------------------------------------------------------------------

class TestSurveyTable:
    def test_valid_construction(self):
        st = SurveyTable('acs/acs5')
        assert st.name == 'acs/acs5'

    def test_invalid_raises_value_error(self):
        with pytest.raises(ValueError, match="not available or not yet implemented"):
            SurveyTable('acs/acs99')

    def test_repr(self):
        assert repr(SurveyTable('acs/acs5')) == "SurveyTable('acs/acs5')"

    def test_equality(self):
        assert SurveyTable('acs/acs5') == SurveyTable('acs/acs5')
        assert SurveyTable('acs/acs5') != SurveyTable('dec/pl')

    def test_hashable(self):
        s = {SurveyTable('acs/acs5'), SurveyTable('acs/acs5')}
        assert len(s) == 1

    def test_vintages_returns_list(self):
        fake = {'acs/acs5': [2021, 2022, 2023]}
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=fake):
            st = SurveyTable('acs/acs5')
            assert st.vintages == [2021, 2022, 2023]

    def test_vintages_unknown_survey_returns_empty(self):
        with patch('morpc_census.api.get_all_avail_endpoints', return_value={}):
            st = SurveyTable('acs/acs5')
            assert st.vintages == []


# ---------------------------------------------------------------------------
# TestVintage
# ---------------------------------------------------------------------------

class TestVintage:
    _fake_endpoints = {'acs/acs5': [2022, 2023]}
    _fake_groups = {'B01001': {'description': 'Sex by Age', 'variables': ''}}

    def _make_vintage(self, survey='acs/acs5', year=2023):
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=self._fake_endpoints):
            return Vintage(survey, year)

    def test_string_survey_normalized_to_survey_table(self):
        v = self._make_vintage()
        assert isinstance(v.survey, SurveyTable)

    def test_survey_table_instance_passed_through(self):
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=self._fake_endpoints):
            st = SurveyTable('acs/acs5')
            v = Vintage(st, 2023)
        assert v.survey is st

    def test_year_stored_as_int(self):
        v = self._make_vintage(year='2023')
        assert v.year == 2023
        assert isinstance(v.year, int)

    def test_invalid_year_raises_value_error(self):
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=self._fake_endpoints):
            with pytest.raises(ValueError, match="not an available vintage"):
                Vintage('acs/acs5', 2019)

    def test_url_property(self):
        v = self._make_vintage()
        assert v.url == 'https://api.census.gov/data/2023/acs/acs5?'

    def test_repr(self):
        v = self._make_vintage()
        assert repr(v) == "Vintage('acs/acs5', 2023)"

    def test_equality(self):
        v1 = self._make_vintage()
        v2 = self._make_vintage()
        assert v1 == v2

    def test_hashable(self):
        v1 = self._make_vintage()
        v2 = self._make_vintage()
        assert hash(v1) == hash(v2)

    def test_groups_delegates_to_get_table_groups(self):
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=self._fake_endpoints), \
             patch('morpc_census.api.get_table_groups', return_value=self._fake_groups) as mock_gtg:
            v = Vintage('acs/acs5', 2023)
            groups = v.groups
        assert groups == self._fake_groups
        mock_gtg.assert_called_once_with('acs/acs5', 2023)


# ---------------------------------------------------------------------------
# TestGroup
# ---------------------------------------------------------------------------

class TestGroup:
    _fake_endpoints = {'acs/acs5': [2022, 2023]}
    _fake_groups = {'B01001': {'description': 'Sex by Age', 'variables': ''}}
    _fake_vars = {'B01001_001E': {'label': 'Total:'}}

    def _make_group(self, code='B01001'):
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=self._fake_endpoints), \
             patch('morpc_census.api.get_table_groups', return_value=self._fake_groups):
            v = Vintage('acs/acs5', 2023)
            return Group(v, code)

    def test_code_uppercased(self):
        g = self._make_group('b01001')
        assert g.code == 'B01001'

    def test_invalid_code_raises_value_error(self):
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=self._fake_endpoints), \
             patch('morpc_census.api.get_table_groups', return_value=self._fake_groups):
            v = Vintage('acs/acs5', 2023)
            with pytest.raises(ValueError, match="not a valid group"):
                Group(v, 'BOGUS')

    def test_non_vintage_raises_type_error(self):
        with pytest.raises(TypeError, match="vintage must be a Vintage instance"):
            Group('not-a-vintage', 'B01001')

    def test_description_from_vintage_groups(self):
        g = self._make_group()
        assert g.description == 'Sex by Age'

    def test_repr(self):
        g = self._make_group()
        assert repr(g) == "Group('acs/acs5', 2023, 'B01001')"

    def test_equality(self):
        g1 = self._make_group()
        g2 = self._make_group()
        assert g1 == g2

    def test_hashable(self):
        g1 = self._make_group()
        g2 = self._make_group()
        assert hash(g1) == hash(g2)

    def test_variables_delegates_to_get_group_variables(self):
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=self._fake_endpoints), \
             patch('morpc_census.api.get_table_groups', return_value=self._fake_groups), \
             patch('morpc_census.api.get_group_variables', return_value=self._fake_vars) as mock_ggv:
            v = Vintage('acs/acs5', 2023)
            g = Group(v, 'B01001')
            variables = g.variables
        assert variables == self._fake_vars
        mock_ggv.assert_called_once_with('acs/acs5', 2023, 'B01001')

    def test_universe_delegates_to_get_group_universe(self):
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=self._fake_endpoints), \
             patch('morpc_census.api.get_table_groups', return_value=self._fake_groups), \
             patch('morpc_census.api.get_group_universe', return_value='All people') as mock_ggu:
            v = Vintage('acs/acs5', 2023)
            g = Group(v, 'B01001')
            universe = g.universe
        assert universe == 'All people'
        mock_ggu.assert_called_once_with('acs/acs5', 2023, 'B01001')
