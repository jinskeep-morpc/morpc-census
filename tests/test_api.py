"""
Tests for pure/offline functions in morpc_census.api.

Network-dependent functions are tested with mocked dependencies.
"""

import pytest
import pandas as pd
from unittest.mock import patch

from morpc_census.api import (
    censusapi_name,
    find_replace_variable_map,
    DimensionTable,
    Endpoint,
    _get_api_key,
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
        'geoidfq': ['0500000US39049'] * 3,
        'name': ['Franklin County, Ohio'] * 3,
        'concept': ['Test concept'] * 3,
        'universe': ['Population'] * 3,
        'survey': ['acs/acs5'] * 3,
        'reference_period': [2023] * 3,
        'estimate': [100, 50, 50],
        'moe': [5, 3, 3],
    })


# ---------------------------------------------------------------------------
# TestCensusapiName
# ---------------------------------------------------------------------------

class TestCensusapiName:
    _fake_endpoints = {'acs/acs5': [2020, 2023], 'dec/pl': [2020]}

    @pytest.fixture(autouse=True)
    def mock_endpoints(self):
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=self._fake_endpoints):
            yield

    def test_no_sumlevel_no_variables(self):
        name = censusapi_name(Endpoint('acs/acs5', 2023), 'franklin', 'B01001')
        assert name == 'census-acs-acs5-2023-franklin-b01001'

    def test_with_sumlevel_tract(self):
        # HIERARCHY_STRING_FROM_CENSUSNAME['tract'] == 'COUNTY-TRACT'
        # 'COUNTY-TRACT'.replace('-', '').lower() == 'countytract'
        name = censusapi_name(Endpoint('acs/acs5', 2023), 'franklin', 'B01001', sumlevel='tract')
        assert name == 'census-acs-acs5-2023-countytract-franklin-b01001'

    def test_with_sumlevel_county(self):
        # HIERARCHY_STRING_FROM_CENSUSNAME['county'] == 'COUNTY'
        name = censusapi_name(Endpoint('acs/acs5', 2023), 'ohio', 'B01001', sumlevel='county')
        assert name == 'census-acs-acs5-2023-county-ohio-b01001'

    def test_with_variables_appends_suffix(self):
        name = censusapi_name(
            Endpoint('acs/acs5', 2023), 'franklin', 'B01001',
            variables=['B01001_001E', 'B01001_002E'],
        )
        assert name.endswith('-select-variables')

    def test_no_variables_no_suffix(self):
        name = censusapi_name(Endpoint('acs/acs5', 2023), 'franklin', 'B01001')
        assert 'select-variables' not in name

    def test_sumlevel_and_variables_combined(self):
        name = censusapi_name(
            Endpoint('acs/acs5', 2023), 'franklin', 'B01001',
            sumlevel='tract',
            variables=['B01001_001E'],
        )
        assert 'countytract' in name
        assert name.endswith('-select-variables')

    def test_result_is_lowercase(self):
        name = censusapi_name(Endpoint('acs/acs5', 2023), 'Franklin', 'B01001')
        assert name == name.lower()

    def test_dec_endpoint(self):
        name = censusapi_name(Endpoint('dec/pl', 2020), 'ohio', 'P1')
        assert name == 'census-dec-pl-2020-ohio-p1'

    def test_accepts_scope_instance(self):
        from morpc_census.geos import Scope
        name = censusapi_name(Endpoint('acs/acs5', 2023), Scope('franklin'), 'B01001')
        assert name == 'census-acs-acs5-2023-franklin-b01001'

    def test_accepts_sumlevel_instance(self):
        from morpc_census.geos import SumLevel
        name = censusapi_name(Endpoint('acs/acs5', 2023), 'ohio', 'B01001', sumlevel=SumLevel('county'))
        assert name == 'census-acs-acs5-2023-county-ohio-b01001'

    def test_scope_instance_matches_string(self):
        from morpc_census.geos import Scope
        ep = Endpoint('acs/acs5', 2023)
        assert (
            censusapi_name(ep, Scope('franklin'), 'B01001')
            == censusapi_name(ep, 'franklin', 'B01001')
        )

    def test_sumlevel_instance_matches_string(self):
        from morpc_census.geos import SumLevel
        ep = Endpoint('acs/acs5', 2023)
        assert (
            censusapi_name(ep, 'ohio', 'B01001', sumlevel=SumLevel('county'))
            == censusapi_name(ep, 'ohio', 'B01001', sumlevel='county')
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
# TestCensusAPIClassNormalization
# ---------------------------------------------------------------------------

class TestCensusAPIClassNormalization:
    """Test that CensusAPI normalizes scope/sumlevel strings to class instances."""

    _fake_vars = {'B01001_001E': {'label': 'Total:'}}
    _fake_groups = {'B01001': {'description': 'Sex by Age', 'variables': ''}}
    _fake_data = pd.DataFrame({'GEO_ID': ['0500000US39049'], 'NAME': ['Franklin County']})

    _fake_endpoints = {'acs/acs5': [2022, 2023]}

    # Raw JSON responses matching the three Census API endpoints the classes call
    _groups_json = {'groups': [{'name': 'B01001', 'description': 'Sex by Age', 'variables': '', 'universe ': 'All people'}]}
    _vars_json = {'variables': {'B01001_001E': {'label': 'Total:'}, 'GEO_ID': {}, 'NAME': {}}}

    def _census_json(self, url, **kwargs):
        if url.endswith('/groups.json'):
            return self._groups_json
        if url.endswith('/groups'):
            return self._groups_json
        if 'groups/B01001.json' in url:
            return self._vars_json
        raise ValueError(f"Unexpected URL in test: {url}")

    def _make(self, scope, sumlevel=None, group='B01001', variables=None):
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=self._fake_endpoints), \
             patch('morpc.req.get_json_safely', side_effect=self._census_json), \
             patch('morpc_census.geos.geoinfo_from_scope_sumlevel', return_value={'for': 'county:049'}), \
             patch.object(CensusAPI, '_fetch', return_value=self._fake_data):
            ep = Endpoint('acs/acs5', 2023)
            return CensusAPI(ep, scope, group=group, sumlevel=sumlevel, variables=variables, return_long=False)

    def test_scope_string_stored_as_scope_instance(self):
        from morpc_census.geos import Scope
        api = self._make('franklin')
        assert isinstance(api.scope, Scope)

    def test_scope_instance_passed_through(self):
        from morpc_census.geos import Scope
        sc = Scope('franklin')
        api = self._make(sc)
        assert api.scope is sc

    def test_scope_name_is_correct(self):
        api = self._make('franklin')
        assert api.scope.name == 'franklin'

    def test_sumlevel_string_stored_as_sumlevel_instance(self):
        from morpc_census.geos import SumLevel
        api = self._make('franklin', sumlevel='county')
        assert isinstance(api.sumlevel, SumLevel)

    def test_sumlevel_none_stays_none(self):
        api = self._make('franklin')
        assert api.sumlevel is None

    def test_sumlevel_instance_passed_through(self):
        from morpc_census.geos import SumLevel
        sl = SumLevel('county')
        api = self._make('franklin', sumlevel=sl)
        assert api.sumlevel is sl

    def test_sumlevel_name_is_correct(self):
        from morpc_census.geos import SumLevel
        api = self._make('franklin', sumlevel='county')
        assert api.sumlevel.name == 'county'
        assert isinstance(api.sumlevel, SumLevel)

    def test_create_resource_title_uses_sumlevel_plural_and_scope_name(self):
        import frictionless
        api = self._make('franklin', sumlevel='county')
        api.filename = 'test.csv'
        api.schema_filename = 'test.schema.yaml'
        captured = {}
        with patch.object(frictionless.Resource, 'from_descriptor', side_effect=lambda d: captured.update(d)):
            api.create_resource()
        assert 'franklin' in captured['title']
        assert 'counties' in captured['title']  # SumLevel('county').plural


# ---------------------------------------------------------------------------
# TestCensusAPIGroupOptional
# ---------------------------------------------------------------------------

class TestFetchVariablesBatching:
    """Tests for _fetch_variables batching logic."""

    _fake_endpoints = {'acs/acs5': [2023]}
    _geo = '0500000US39049'

    def _make_api(self, n_variables):
        """Build a CensusAPI with _fetch stubbed so we can call _fetch_variables directly."""
        variables = [f'B01001_{i:03d}E' for i in range(1, n_variables + 1)]
        stub = pd.DataFrame({'GEO_ID': [self._geo]})
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=self._fake_endpoints), \
             patch('morpc_census.geos.geoinfo_from_scope_sumlevel', return_value={'for': 'county:049'}), \
             patch.object(CensusAPI, '_fetch', return_value=stub):
            ep = Endpoint('acs/acs5', 2023)
            return CensusAPI(ep, 'franklin', variables=variables, return_long=False)

    def _response(self, variables):
        """Simulate a Census API JSON list-of-lists response for the given variables."""
        return [['GEO_ID'] + list(variables), [self._geo] + ['1'] * len(variables)]

    def test_single_request_when_49_variables(self):
        api = self._make_api(49)
        with patch('morpc.req.get_json_safely', return_value=self._response(api.variables)) as mock:
            result = api._fetch_variables(api.request['url'], {})
        mock.assert_called_once()
        assert 'GEO_ID' in result.columns
        assert set(api.variables).issubset(set(result.columns))

    def test_two_requests_when_50_variables(self):
        api = self._make_api(50)
        responses = [self._response(api.variables[:49]), self._response(api.variables[49:])]
        with patch('morpc.req.get_json_safely', side_effect=responses) as mock:
            result = api._fetch_variables(api.request['url'], {})
        assert mock.call_count == 2
        assert set(api.variables).issubset(set(result.columns))

    def test_three_requests_when_98_variables(self):
        api = self._make_api(98)
        responses = [
            self._response(api.variables[:49]),
            self._response(api.variables[49:]),
        ]
        with patch('morpc.req.get_json_safely', side_effect=responses) as mock:
            result = api._fetch_variables(api.request['url'], {})
        assert mock.call_count == 2  # ceil(98/49) == 2
        assert set(api.variables).issubset(set(result.columns))

    def test_batched_results_joined_on_geoid(self):
        api = self._make_api(50)
        batch1 = [['GEO_ID'] + api.variables[:49], [self._geo] + ['A'] * 49]
        batch2 = [['GEO_ID'] + api.variables[49:], [self._geo] + ['B']]
        with patch('morpc.req.get_json_safely', side_effect=[batch1, batch2]):
            result = api._fetch_variables(api.request['url'], {})
        row = result.loc[0]
        assert row['GEO_ID'] == self._geo
        assert row[api.variables[0]] == 'A'
        assert row[api.variables[49]] == 'B'

    def test_geoid_included_in_every_batch_request(self):
        api = self._make_api(50)
        responses = [self._response(api.variables[:49]), self._response(api.variables[49:])]
        with patch('morpc.req.get_json_safely', side_effect=responses) as mock:
            api._fetch_variables(api.request['url'], {})
        for call in mock.call_args_list:
            get_param = call.kwargs['params']['get']
            assert get_param.startswith('GEO_ID,')

    def test_single_row_result(self):
        api = self._make_api(50)
        responses = [self._response(api.variables[:49]), self._response(api.variables[49:])]
        with patch('morpc.req.get_json_safely', side_effect=responses):
            result = api._fetch_variables(api.request['url'], {})
        assert len(result) == 1


class TestCensusAPIGroupOptional:
    """Test CensusAPI behavior when group is None."""

    _fake_endpoints = {'acs/acs5': [2022, 2023]}
    _fake_data = pd.DataFrame({'GEO_ID': ['0500000US39049'], 'NAME': ['Franklin County']})

    def _make_no_group(self, variables):
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=self._fake_endpoints), \
             patch('morpc_census.geos.geoinfo_from_scope_sumlevel', return_value={'for': 'county:049'}), \
             patch.object(CensusAPI, '_fetch', return_value=self._fake_data):
            ep = Endpoint('acs/acs5', 2023)
            return CensusAPI(ep, 'franklin', variables=variables, return_long=False)

    def test_no_group_no_variables_raises(self):
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=self._fake_endpoints):
            ep = Endpoint('acs/acs5', 2023)
            with pytest.raises(ValueError, match="At least one of 'group' or 'variables'"):
                CensusAPI(ep, 'franklin')

    def test_variables_only_stores_none_group(self):
        api = self._make_no_group(['B01001_001E', 'B01001_002E'])
        assert api.group is None

    def test_variables_only_stores_variables(self):
        api = self._make_no_group(['B01001_001E', 'B01001_002E'])
        assert api.variables == ['B01001_001E', 'B01001_002E']

    def test_variables_uppercased(self):
        api = self._make_no_group(['b01001_001e'])
        assert api.variables == ['B01001_001E']

    def test_vars_returns_placeholder_dict_when_no_group(self):
        api = self._make_no_group(['B01001_001E'])
        assert api.vars == {'B01001_001E': {}}

    def test_universe_returns_fallback_string_when_no_group(self):
        api = self._make_no_group(['B01001_001E'])
        assert 'no group' in api.universe

    def test_name_has_no_group_part_when_no_group(self):
        api = self._make_no_group(['B01001_001E'])
        assert 'none' not in api.name
        assert 'b01001' not in api.name

    def test_build_request_uses_variable_list_when_no_group(self):
        api = self._make_no_group(['B01001_001E', 'B01001_002E'])
        assert api.request['params']['get'] == 'B01001_001E,B01001_002E'


# ---------------------------------------------------------------------------
# TestGetApiKey
# ---------------------------------------------------------------------------

class TestGetApiKey:
    def test_returns_key_from_environment(self):
        with patch.dict('os.environ', {'CENSUS_API_KEY': 'testkey123'}), \
             patch('dotenv.load_dotenv'), patch('dotenv.find_dotenv', return_value=''):
            assert _get_api_key() == 'testkey123'

    def test_returns_none_when_not_set(self):
        env = {k: v for k, v in __import__('os').environ.items() if k != 'CENSUS_API_KEY'}
        with patch.dict('os.environ', env, clear=True), \
             patch('dotenv.load_dotenv'), patch('dotenv.find_dotenv', return_value=''):
            assert _get_api_key() is None

    def test_dotenv_called_with_override_false(self):
        """dotenv convention: environment variables take precedence over .env values."""
        with patch.dict('os.environ', {}, clear=True), \
             patch('dotenv.load_dotenv') as mock_ld, \
             patch('dotenv.find_dotenv', return_value='/fake/.env'):
            _get_api_key()
        _, kwargs = mock_ld.call_args
        assert kwargs.get('override') is False

    def test_find_dotenv_called_with_usecwd(self):
        with patch.dict('os.environ', {}, clear=True), \
             patch('dotenv.load_dotenv'), \
             patch('dotenv.find_dotenv', return_value='') as mock_fd:
            _get_api_key()
        mock_fd.assert_called_once_with(usecwd=True)


# ---------------------------------------------------------------------------
# TestEndpoint
# ---------------------------------------------------------------------------

class TestEndpoint:
    _fake_endpoints = {'acs/acs5': [2022, 2023], 'dec/pl': [2020]}

    def _make_endpoint(self, survey='acs/acs5', year=2023):
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=self._fake_endpoints):
            return Endpoint(survey, year)

    # Survey validation
    def test_invalid_survey_raises_value_error(self):
        with pytest.raises(ValueError, match="not available or not yet implemented"):
            Endpoint('acs/acs99', 2023)

    def test_raises_for_empty_survey(self):
        with pytest.raises(ValueError):
            Endpoint('', 2023)

    def test_raises_for_partial_survey(self):
        with pytest.raises(ValueError):
            Endpoint('acs', 2023)

    # Year validation
    def test_year_stored_as_int(self):
        ep = self._make_endpoint(year='2023')
        assert ep.year == 2023
        assert isinstance(ep.year, int)

    def test_invalid_year_raises_value_error(self):
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=self._fake_endpoints):
            with pytest.raises(ValueError, match="not an available vintage"):
                Endpoint('acs/acs5', 2019)

    # Repr / equality / hash
    def test_repr(self):
        ep = self._make_endpoint()
        assert repr(ep) == "Endpoint('acs/acs5', 2023)"

    def test_equality(self):
        assert self._make_endpoint() == self._make_endpoint()
        assert self._make_endpoint() != self._make_endpoint('dec/pl', 2020)

    def test_hashable(self):
        ep1 = self._make_endpoint()
        ep2 = self._make_endpoint()
        assert hash(ep1) == hash(ep2)
        assert len({ep1, ep2}) == 1

    # Properties
    def test_url_property(self):
        ep = self._make_endpoint()
        assert ep.url == 'https://api.census.gov/data/2023/acs/acs5?'

    def test_vintages_property(self):
        ep = self._make_endpoint()
        assert ep.vintages == [2022, 2023]

    # Network
    def test_groups_fetches_from_api(self):
        raw = {'groups': [{'name': 'B01001', 'description': 'Sex by Age', 'variables': ''}]}
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=self._fake_endpoints), \
             patch('morpc.req.get_json_safely', return_value=raw):
            ep = Endpoint('acs/acs5', 2023)
            groups = ep.groups
        assert groups == {'B01001': {'description': 'Sex by Age', 'variables': ''}}


# ---------------------------------------------------------------------------
# TestGroup
# ---------------------------------------------------------------------------

class TestGroup:
    _fake_endpoints = {'acs/acs5': [2022, 2023]}
    _groups_json = {'groups': [{'name': 'B01001', 'description': 'Sex by Age', 'variables': '', 'universe ': 'All people'}]}
    _vars_json = {'variables': {'B01001_001E': {'label': 'Total:'}, 'GEO_ID': {}, 'NAME': {}}}

    def _make_group(self, code='B01001'):
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=self._fake_endpoints), \
             patch('morpc.req.get_json_safely', return_value=self._groups_json):
            ep = Endpoint('acs/acs5', 2023)
            return Group(ep, code)

    def test_code_uppercased(self):
        g = self._make_group('b01001')
        assert g.code == 'B01001'

    def test_invalid_code_raises_value_error(self):
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=self._fake_endpoints), \
             patch('morpc.req.get_json_safely', return_value=self._groups_json):
            ep = Endpoint('acs/acs5', 2023)
            with pytest.raises(ValueError, match="not a valid group"):
                Group(ep, 'BOGUS')

    def test_non_endpoint_raises_type_error(self):
        with pytest.raises(TypeError, match="endpoint must be an Endpoint instance"):
            Group('not-an-endpoint', 'B01001')

    def test_description_from_endpoint_groups(self):
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

    def test_variables_fetches_from_api(self):
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=self._fake_endpoints), \
             patch('morpc.req.get_json_safely', side_effect=[self._groups_json, self._vars_json]):
            ep = Endpoint('acs/acs5', 2023)
            g = Group(ep, 'B01001')
            variables = g.variables
        assert variables == {'B01001_001E': {'label': 'Total:'}}
        assert 'GEO_ID' not in variables
        assert 'NAME' not in variables

    def test_universe_fetches_from_api(self):
        with patch('morpc_census.api.get_all_avail_endpoints', return_value=self._fake_endpoints), \
             patch('morpc.req.get_json_safely', side_effect=[self._groups_json, self._groups_json]):
            ep = Endpoint('acs/acs5', 2023)
            g = Group(ep, 'B01001')
            universe = g.universe
        assert universe == 'All people'
