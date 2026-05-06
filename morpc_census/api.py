"""
Connects to the US Census Bureau API, retrieves survey data, and structures it
as long-format tables backed by frictionless metadata.

Census API root: https://api.census.gov/data/
"""

import json
import logging
import os
import re
from collections import OrderedDict
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Census subject-area constants
# ---------------------------------------------------------------------------

HIGHLEVEL_GROUP_DESC = {
    "01": "Sex, Age, and Population",
    "02": "Race",
    "03": "Ethnicity",
    "04": "Ancestry",
    "05": "Nativity and Citizenship",
    "06": "Place of Birth",
    "07": "Geographic Mobility",
    "08": "Transportation to Work",
    "09": "Children",
    "10": "Grandparents and Grandchildren",
    "11": "Household Type",
    "12": "Marriage and Marital Status",
    "13": "Mothers and Births",
    "14": "School Enrollment",
    "15": "Educational Attainment",
    "16": "Language Spoken at Home",
    "17": "Poverty",
    "18": "Disability",
    "19": "Household Income",
    "20": "Earnings",
    "21": "Veterans",
    "22": "Food Stamps/SNAP",
    "23": "Workers and Employment Status",
    "24": "Occupation, Industry, Class",
    "25": "Housing Units, Tenure, Housing Costs",
    "26": "Group Quarters",
    "27": "Health Insurance",
    "28": "Computers and Internet",
    "29": "Voting-Age",
    "98": "Coverage Rates and Allocation Rates",
    "99": "Allocations",
}

HIGHLEVEL_DESC_FROM_ID = {v: k for k, v in HIGHLEVEL_GROUP_DESC.items()}

MISSING_VALUES = [
    "", "-222222222", "-333333333", "-555555555",
    "-666666666", "-888888888", "-999999999", "*****",
]

VARIABLE_TYPES = {
    "E": "estimate",
    "M": "moe",
    "PE": "percent_estimate",
    "PM": "percent_moe",
    "N": "total",
}

AGEGROUP_MAP = {
    'Under 5 years': 'Under 5 years',
    '5 to 9 years': '5 to 9 years',
    '10 to 14 years': '10 to 14 years',
    '15 to 17 years': '15 to 19 years',
    '18 and 19 years': '15 to 19 years',
    '20 years': '20 to 24 years',
    '21 years': '20 to 24 years',
    '22 to 24 years': '20 to 24 years',
    '25 to 29 years': '25 to 29 years',
    '30 to 34 years': '30 to 34 years',
    '35 to 39 years': '35 to 39 years',
    '40 to 44 years': '40 to 44 years',
    '45 to 49 years': '45 to 49 years',
    '50 to 54 years': '50 to 54 years',
    '55 to 59 years': '55 to 59 years',
    '60 and 61 years': '60 to 64 years',
    '62 to 64 years': '60 to 64 years',
    '65 and 66 years': '65 to 69 years',
    '67 to 69 years': '65 to 69 years',
    '70 to 74 years': '70 to 74 years',
    '75 to 79 years': '75 to 79 years',
    '80 to 84 years': '80 to 84 years',
    '85 years and over': '85 years and over',
}

AGEGROUP_SORT_ORDER = {
    'Total': 1,
    'Under 5 years': 2,
    '5 to 9 years': 3,
    '10 to 14 years': 4,
    '15 to 19 years': 5,
    '20 to 24 years': 6,
    '25 to 29 years': 7,
    '30 to 34 years': 8,
    '35 to 39 years': 9,
    '40 to 44 years': 10,
    '45 to 49 years': 11,
    '50 to 54 years': 12,
    '55 to 59 years': 13,
    '60 to 64 years': 14,
    '65 to 69 years': 15,
    '70 to 74 years': 16,
    '75 to 79 years': 17,
    '80 to 84 years': 18,
    '85 years and over': 19,
}

RACE_TABLE_MAP = {
    'A': 'White Alone',
    'B': 'Black or African American Alone',
    'C': 'American Indian and Alaska Native Alone',
    'D': 'Asian Alone',
    'E': 'Native Hawaiian and Other Pacific Islander Alone',
    'F': 'Some Other Race Alone',
    'G': 'Two or More Races',
    'H': 'White Alone, Not Hispanic or Latino',
    'I': 'Hispanic or Latino',
}

EDUCATION_ATTAIN_MAP = {
    'No schooling completed': 'No high school diploma',
    'Nursery school': 'No high school diploma',
    'Kindergarten': 'No high school diploma',
    '1st grade': 'No high school diploma',
    '2nd grade': 'No high school diploma',
    '3rd grade': 'No high school diploma',
    '4th grade': 'No high school diploma',
    '5th grade': 'No high school diploma',
    '6th grade': 'No high school diploma',
    '7th grade': 'No high school diploma',
    '8th grade': 'No high school diploma',
    '9th grade': 'No high school diploma',
    '10th grade': 'No high school diploma',
    '11th grade': 'No high school diploma',
    '12th grade, no diploma': 'No high school diploma',
    'Regular high school diploma': 'High school diploma or equivalent',
    'GED or alternative credential': 'High school diploma or equivalent',
    'Some college, less than 1 year': 'High school diploma or equivalent',
    'Some college, 1 or more years, no degree': 'High school diploma or equivalent',
    "Associate's degree": "Associate's degree",
    "Bachelor's degree": "Bachelor's degree",
    "Master's degree": "More than Bachelor's",
    'Professional school degree': "More than Bachelor's",
    'Doctorate degree': "More than Bachelor's",
}

EDUCATION_ATTAIN_SORT_ORDER = {
    'Total': 1,
    'No high school diploma': 2,
    'High school diploma or equivalent': 3,
    "Associate's degree": 4,
    "Bachelor's degree": 5,
    "More than Bachelor's": 6,
}

INCOME_TO_POVERTY_MAP = {
    'Under .50': 'Under .50',
    '.50 to .74': '.50 to .99',
    '.75 to .99': '.50 to .99',
    '1.00 to 1.24': '1.00 to 1.99',
    '1.25 to 1.49': '1.00 to 1.99',
    '1.50 to 1.74': '1.00 to 1.99',
    '1.75 to 1.84': '1.00 to 1.99',
    '1.85 to 1.99': '1.00 to 1.99',
    '2.00 to 2.99': '2.00 to 2.99',
    '3.00 to 3.99': '3.00 to 3.99',
    '4.00 to 4.99': '4.00 and over',
    '5.00 and over': '4.00 and over',
}

INCOME_TO_POVERTY_SORT_ORDER = {
    'Total': 0,
    'Under .50': 1,
    '.50 to .99': 2,
    '1.00 to 1.99': 3,
    '2.00 to 2.99': 4,
    '3.00 to 3.99': 5,
    '4.00 and over': 6,
}

NTD_AGEMAP = {
    'Under 5 years': '18 years and under',
    '5 to 9 years': '18 years and under',
    '10 to 14 years': '18 years and under',
    '15 to 17 years': '18 years and under',
    '18 and 19 years': '18 years and under',  # apportion half here, half in 19 years
    '20 years': '19 to 64 years',
    '21 years': '19 to 64 years',
    '22 to 24 years': '19 to 64 years',
    '25 to 29 years': '19 to 64 years',
    '30 to 34 years': '19 to 64 years',
    '35 to 39 years': '19 to 64 years',
    '40 to 44 years': '19 to 64 years',
    '45 to 49 years': '19 to 64 years',
    '50 to 54 years': '19 to 64 years',
    '55 to 59 years': '19 to 64 years',
    '60 and 61 years': '19 to 64 years',
    '62 to 64 years': '19 to 64 years',
    '65 and 66 years': '65 years and over',
    '67 to 69 years': '65 years and over',
    '70 to 74 years': '65 years and over',
    '75 to 79 years': '65 years and over',
    '80 to 84 years': '65 years and over',
    '85 years and over': '65 years and over',
}

NTD_AGEMAP_ORDER = {
    'Total': 0,
    '18 years and under': 1,
    '20 to 64 years': 2,
    '65 years and over': 3,
}

# Schema field definitions for each value column type that can appear in LONG
_VALUE_FIELD_DEFS = {
    'estimate': {
        'name': 'estimate', 'type': 'number',
        'description': 'Estimate value for the variable',
    },
    'moe': {
        'name': 'moe', 'type': 'number',
        'description': 'Margin of error for the estimate',
    },
    'percent_estimate': {
        'name': 'percent_estimate', 'type': 'number',
        'description': 'Percent estimate value for the variable',
    },
    'percent_moe': {
        'name': 'percent_moe', 'type': 'number',
        'description': 'Margin of error for the percent estimate',
    },
    'total': {
        'name': 'total', 'type': 'number',
        'description': 'Total value for the variable',
    },
}

# ---------------------------------------------------------------------------
# API discovery — fetched lazily so import does not make network calls
# ---------------------------------------------------------------------------

CENSUS_DATA_BASE_URL = 'https://api.census.gov/data'

IMPLEMENTED_ENDPOINTS = [
    'acs/acs1',
    'acs/acs1/profile',
    'acs/acs1/subject',
    'acs/acs5',
    'acs/acs5/profile',
    'acs/acs5/subject',
    'dec/pl',
    'dec/dhc',
    'dec/ddhca',
    'dec/ddhcb',
    'dec/sf1',
    'dec/sf2',
    'dec/sf3',
    'geoinfo',
]

_avail_endpoints_cache = None


def get_all_avail_endpoints():
    """Return {endpoint: [vintage, ...]} for every dataset the Census API exposes.

    Result is cached after the first call so subsequent calls are free.
    """
    global _avail_endpoints_cache
    if _avail_endpoints_cache is None:
        from morpc.req import get_json_safely
        result = {}
        for dataset in get_json_safely(CENSUS_DATA_BASE_URL)['dataset']:
            if 'c_vintage' in dataset:
                endpoint = "/".join(dataset['c_dataset'])
                result.setdefault(endpoint, []).append(dataset['c_vintage'])
        _avail_endpoints_cache = dict(sorted(result.items()))
    return _avail_endpoints_cache


# ---------------------------------------------------------------------------
# Parameter validation helpers
# ---------------------------------------------------------------------------

def valid_survey_table(survey_table: str) -> bool:
    """Validate survey_table against IMPLEMENTED_ENDPOINTS. Raises ValueError if not recognized."""
    if survey_table in IMPLEMENTED_ENDPOINTS:
        logger.info(f"{survey_table} is valid.")
        return True
    logger.error(f"{survey_table} not available or not yet implemented.")
    raise ValueError(f"{survey_table} not available or not yet implemented.")


def valid_vintage(survey_table: str, year: int) -> bool:
    """Validate that *year* is an available vintage for *survey_table*.

    Makes a network call to the Census discovery endpoint on first use
    (result is cached).
    """
    year = int(year)
    avail = get_all_avail_endpoints()
    if year in avail.get(survey_table, []):
        logger.info(f"{year} is valid for {survey_table}.")
        return True
    logger.error(f"{year} is not an available vintage for {survey_table}.")
    raise ValueError(f"{year} is not an available vintage for {survey_table}.")


def get_query_url(survey_table: str, year: int) -> str:
    """Build the base Census API query URL for a survey and vintage year."""
    url = f"{CENSUS_DATA_BASE_URL}/{year}/{survey_table}?"
    logger.info(f"Base URL: {url}")
    return url


def get_table_groups(survey_table: str, year: int) -> dict:
    """Return {group_name: {description, variables}} for all groups in the survey."""
    from morpc.req import get_json_safely
    logger.debug(f"Fetching groups for {year} {survey_table}")
    data = get_json_safely(f"{CENSUS_DATA_BASE_URL}/{year}/{survey_table}/groups.json")
    groups = {
        g['name']: {'description': g['description'], 'variables': g['variables']}
        for g in data['groups']
    }
    return dict(sorted(groups.items()))


def valid_group(group: str, survey_table: str, year: int) -> bool:
    """Validate that group exists in survey_table for year. Raises ValueError if not found."""
    groups = get_table_groups(survey_table, year)
    if group in groups:
        logger.info(f"{group} is valid for {year} {survey_table}.")
        return True
    logger.error(f"{group} is not a valid group in {year} {survey_table}.")
    raise ValueError(f"{group} is not a valid group in {year} {survey_table}.")


def get_group_variables(survey_table: str, year: int, group: str) -> dict:
    """Return variable metadata dict for *group*, sorted by variable name."""
    from morpc.req import get_json_safely
    data = get_json_safely(
        f"{CENSUS_DATA_BASE_URL}/{year}/{survey_table}/groups/{group}.json"
    )
    return {
        k: data['variables'][k]
        for k in sorted(data['variables'])
        if k not in ('GEO_ID', 'NAME')
    }


def get_group_universe(survey_table: str, year: int, group: str) -> str:
    """Return the universe string for a variable group from the Census API."""
    from morpc.req import get_json_safely
    data = get_json_safely(f"{CENSUS_DATA_BASE_URL}/{year}/{survey_table}/groups")
    match = [x for x in data['groups'] if x['name'] == group.upper()]
    if not match:
        raise ValueError(f"Group {group} not found in {year} {survey_table}.")
    return match[0]['universe ']  # trailing space is present in the Census API response


def valid_variables(survey_table: str, year: int, group: str, variables: list[str]) -> bool:
    """Validate that all variables exist in group. Raises ValueError on first missing variable."""
    avail = get_group_variables(survey_table, year, group)
    for var in variables:
        if var not in avail:
            logger.error(f"{var} is not a valid variable in {group} {survey_table}.")
            raise ValueError(f"{var} is not a valid variable in {group} {survey_table}.")
    return True


# ---------------------------------------------------------------------------
# Request building
# ---------------------------------------------------------------------------

def get_params(group: str, variables: list[str] | None = None) -> str:
    """Build the Census API 'get' parameter string for a group or variable list."""
    if variables is not None:
        return ",".join(variables)
    return f"group({group})"


def get_api_request(survey_table: str, year: int, group: str, scope: str, variables: list[str] | None = None, sumlevel: str | None = None) -> dict:
    """Build the Census API request dict (url + params) for a survey, scope, and optional sumlevel."""
    from morpc_census.geos import geoinfo_from_scope_sumlevel

    url = get_query_url(survey_table, year)
    get_param = get_params(group, variables=variables)
    geo_param = geoinfo_from_scope_sumlevel(scope, sumlevel, output='params')

    params = {'get': get_param}
    params.update(geo_param)

    logger.info(f"Request — url: {url}  params: {params}")
    return {'url': url, 'params': params}


# ---------------------------------------------------------------------------
# Low-level fetch
# ---------------------------------------------------------------------------

def fetch(url: str, params: dict, var_batch_size: int = 20) -> pd.DataFrame:
    """Fetch from the Census API and return a DataFrame indexed by GEO_ID.

    Automatically batches requests when more than *var_batch_size* variables
    are requested, to stay within the API's 50-variable limit.

    Parameters
    ----------
    url : str
        Base Census API endpoint URL, e.g.
        ``https://api.census.gov/data/2022/acs/acs5?``
    params : dict
        Query parameters in ``requests`` format, including ``get``, ``for``,
        and optionally ``in``.
    var_batch_size : int
        Variables per request batch.  Capped at 49 to leave one slot for
        GEO_ID.  Defaults to 20.
    """
    from morpc.req import get_json_safely, get_text_safely
    is_group_query = bool(re.findall(r'group\((.+)\)', params['get']))

    if is_group_query:
        group = re.findall(r'group\((.+)\)', params['get'])[0]
        logger.info(f"group({group}) query — bypassing variable-limit batching.")

        params_string = "&".join(f"{k}={v}" for k, v in params.items())
        text = get_text_safely(f"{url}{params_string}")

        try:
            census_data = pd.read_csv(
                StringIO(text.replace('[', '').replace(']', '').rstrip(',')),
                sep=',',
                quotechar='"',
            )
            census_data = census_data.drop(
                columns=[c for c in census_data.columns if c.startswith('Unnamed')]
            )
        except Exception as e:
            logger.error(f"Failed to parse group response: {e}")
            raise RuntimeError("Failed to parse Census API group response.") from e

        return census_data

    # Variable-list query — may need batching
    if var_batch_size > 49:
        logger.warning("var_batch_size exceeds API limit; capping at 49.")
        var_batch_size = 49

    all_vars = params['get'].split(',')
    logger.info(f"Total variables requested: {len(all_vars)}")

    remaining = all_vars
    batch_num = 1
    census_data = None

    while remaining:
        logger.info(f"Batch #{batch_num}: {len(remaining)} variables remaining.")

        batch = remaining[:var_batch_size - 2]
        if 'GEO_ID' not in batch:
            batch.append('GEO_ID')
            remaining = remaining[var_batch_size - 2:]
        else:
            try:
                batch.append(remaining[var_batch_size - 2])
            except IndexError:
                pass
            remaining = remaining[var_batch_size - 1:]

        batch_params = json.loads(json.dumps(params))
        batch_params['get'] = ','.join(batch)

        records = get_json_safely(url, params=batch_params)
        columns = records.pop(0)
        df = pd.DataFrame.from_records(records, columns=columns).filter(
            items=batch, axis='columns'
        )

        if census_data is None:
            census_data = df.set_index('GEO_ID').copy()
        else:
            census_data = census_data.join(df.set_index('GEO_ID')).reset_index()

        batch_num += 1

    return census_data


# ---------------------------------------------------------------------------
# Naming helper
# ---------------------------------------------------------------------------

def censusapi_name(survey_table: str, year: int, scope: str, group: str, sumlevel: str | None = None, variables: list[str] | None = None) -> str:
    """Construct a canonical, machine-readable name for a CensusAPI dataset."""
    from morpc import HIERARCHY_STRING_FROM_CENSUSNAME

    sumlevel_part = (
        f"{HIERARCHY_STRING_FROM_CENSUSNAME[sumlevel].replace('-', '').lower()}-"
        if sumlevel is not None
        else ''
    )
    var_part = '-select-variables' if variables is not None else ''
    return (
        f"census-{survey_table.replace('/', '-')}-{year}"
        f"-{sumlevel_part}{scope}-{group}{var_part}"
    ).lower()


# ---------------------------------------------------------------------------
# Variable-mapping helper (used by DimensionTable)
# ---------------------------------------------------------------------------

def find_replace_variable_map(labels: list[str], variables: list[str], map: dict) -> tuple[list[str], list[str]]:
    """Apply label substitutions and return updated labels and new sequential variable codes."""
    labels = list(labels)
    variables = list(variables)

    new_labels = [
        next(
            (label.replace(key, val) for key, val in map.items() if key in label),
            label,
        )
        for label in labels
    ]

    var_id = variables[0].split('_')[0]
    variable_map = {}
    for i, label in enumerate(new_labels):
        if label not in variable_map:
            variable_map[label] = f"{var_id}_M{len(variable_map):02d}"

    new_variables = [variable_map[label] for label in new_labels]
    return new_labels, new_variables


# ---------------------------------------------------------------------------
# CensusAPI
# ---------------------------------------------------------------------------

class CensusAPI:
    """Fetches Census API survey data and exposes it as a long-format DataFrame.

    Parameters
    ----------
    survey_table : str
        Dataset endpoint, e.g. ``'acs/acs5'``, ``'dec/pl'``.
        See :data:`IMPLEMENTED_ENDPOINTS`.
    year : int
        Vintage year, e.g. ``2023``.
    group : str
        Variable group code, e.g. ``'B01001'``.
    scope : str
        Geographic scope key, e.g. ``'region15'``, ``'ohio'``.
        See ``morpc_census.geos.SCOPES``.
    sumlevel : str, optional
        Geographic summary level query name, e.g. ``'county'``, ``'tract'``.
        See ``morpc_census.geos.valid_sumlevel``.
    variables : list of str, optional
        Specific variables to retrieve.  If ``None`` all variables in the
        group are retrieved.
    return_long : bool
        If ``True`` (default) compute ``self.LONG`` immediately after fetch.
    """

    def __init__(
        self,
        survey_table: str,
        year: int,
        group: str,
        scope: str,
        sumlevel: str | None = None,
        variables: list[str] | None = None,
        return_long: bool = True,
    ):
        self.SURVEY = survey_table
        self.YEAR = year
        self.GROUP = group.upper()
        self.SCOPE = scope.lower()
        self.SUMLEVEL = sumlevel.lower() if sumlevel is not None else None
        self.VARIABLES = (
            [v.upper() for v in variables] if variables is not None else None
        )

        self.NAME = censusapi_name(survey_table, year, scope, group, sumlevel, variables)
        self.logger = (
            logging.getLogger(__name__)
            .getChild(self.__class__.__name__)
            .getChild(self.NAME)
        )
        self.logger.info(f"Initializing CensusAPI for {self.NAME}.")

        self.validate()
        self._fetch_metadata()

        self.logger.info("Building request URL and parameters.")
        self.REQUEST = get_api_request(
            survey_table=self.SURVEY,
            year=self.YEAR,
            group=self.GROUP,
            scope=self.SCOPE,
            variables=self.VARIABLES,
            sumlevel=self.SUMLEVEL,
        )

        self.logger.info(
            f"Fetching data from {self.REQUEST['url']} "
            f"with params {self.REQUEST['params']}."
        )
        try:
            self.DATA = fetch(self.REQUEST['url'], self.REQUEST['params']).reset_index()
        except Exception as e:
            self.logger.error(f"Failed to retrieve data: {e}")
            raise RuntimeError("Failed to retrieve data from Census API.") from e

        n_dupes = self.DATA.duplicated().sum()
        if n_dupes:
            self.logger.warning(
                f"Removing {n_dupes} duplicate rows "
                "(can occur when ucgid=pseudo() is used for geographies)."
            )
            self.DATA = self.DATA.loc[~self.DATA.duplicated()].reset_index(drop=True)

        if return_long:
            self.LONG = self.melt()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_metadata(self):
        """Populate CONCEPT, UNIVERSE, and VARS from the Census API."""
        self.CONCEPT = get_table_groups(self.SURVEY, self.YEAR)[self.GROUP]['description']

        try:
            universe_year = self.YEAR if int(self.YEAR) >= 2023 else 2023
            self.UNIVERSE = get_group_universe(self.SURVEY, universe_year, self.GROUP)
        except Exception as e:
            self.UNIVERSE = (
                'Not defined in API — see CensusAPI.REQUEST for endpoint details'
            )
            self.logger.warning(
                f"Universe not defined for {self.SURVEY}/{self.GROUP}: {e}"
            )

        self.VARS = get_group_variables(self.SURVEY, self.YEAR, self.GROUP)
        if self.VARIABLES is not None:
            self.VARS = {k: v for k, v in self.VARS.items() if k in self.VARIABLES}

    def validate(self) -> None:
        """Validate all parameters, raising ValueError on the first failure."""
        from morpc_census.geos import valid_scope, valid_sumlevel

        self.logger.info("Validating parameters.")
        valid_survey_table(self.SURVEY)
        valid_vintage(self.SURVEY, self.YEAR)
        valid_group(self.GROUP, self.SURVEY, self.YEAR)
        if self.VARIABLES is not None:
            valid_variables(self.SURVEY, self.YEAR, self.GROUP, self.VARIABLES)

    # ------------------------------------------------------------------
    # Convenience properties
    # ------------------------------------------------------------------

    @property
    def scope_obj(self):
        """Return the Scope object for this dataset's geographic scope."""
        from morpc_census.geos import SCOPES, Scope
        return SCOPES[self.SCOPE]

    @property
    def geoidfqs(self):
        """Return the GEO_ID column parsed as a list of GeoIDFQ objects."""
        from morpc_census.geos import GeoIDFQ
        return [GeoIDFQ.parse(g) for g in self.DATA['GEO_ID']]

    # ------------------------------------------------------------------
    # Data transformation
    # ------------------------------------------------------------------

    def melt(self):
        """Transform the wide API response into a long-format DataFrame.

        Returns
        -------
        pandas.DataFrame
            Columns: GEO_ID, [NAME,] reference_period, survey, concept,
            universe, variable_label, variable, and one or more of
            estimate / moe / percent_estimate / percent_moe / total.
        """
        self.logger.info("Melting data to long format.")

        id_vars = ['GEO_ID', 'NAME'] if 'NAME' in self.DATA.columns else ['GEO_ID']
        long = self.DATA.melt(id_vars=id_vars, var_name='variable', value_name='value')

        # Keep only the requested variables
        long = long.loc[long['variable'].isin(self.VARS)]

        # Determine the type suffix (E, M, PE, PM, N)
        def _type_code(var):
            match = re.findall(r'_[0-9]+([A-Z]{1,2})$', var)
            if match:
                return match[0]
            # Fall back to the first label segment for non-standard codes
            return self.VARS[var]['label'].split('!!')[0].lower()

        long['variable_type'] = long['variable'].map(
            lambda v: VARIABLE_TYPES.get(_type_code(v), _type_code(v))
        )
        long = long.loc[long['variable_type'].isin(VARIABLE_TYPES.values())]

        # Human-readable label (everything after the first '!!')
        long['variable_label'] = long['variable'].map(
            lambda v: (
                re.split('!!', self.VARS[v]['label'], maxsplit=1)[1]
                if '!!' in self.VARS[v]['label']
                else self.VARS[v]['label']
            )
        )

        # Strip the type suffix to get the base variable code (B01001_001E → B01001_001)
        long['variable'] = long['variable'].map(
            lambda v: (
                re.findall(r'([A-Z0-9_]+[0-9]+)[A-Z]+$', v)[0]
                if v[-1].isalpha() and re.findall(r'([A-Z0-9_]+[0-9]+)[A-Z]+$', v)
                else v
            )
        )

        long['reference_period'] = self.YEAR
        long['universe'] = self.UNIVERSE
        long['survey'] = self.SURVEY
        long['concept'] = self.CONCEPT.capitalize()

        pivot_index = id_vars + [
            'reference_period', 'survey', 'concept', 'universe',
            'variable_label', 'variable',
        ]
        try:
            long = (
                long.pivot(index=pivot_index, columns='variable_type', values='value')
                .reset_index()
                .rename_axis(None, axis=1)
            )
        except ValueError as e:
            self.logger.error(f"Pivot failed: {e}")
            raise

        long = long.sort_values(by=['GEO_ID', 'variable', 'reference_period'])

        for col in long.columns:
            if col in VARIABLE_TYPES.values():
                long[col] = pd.to_numeric(
                    long[col].where(~long[col].isin(MISSING_VALUES), other=np.nan),
                    errors='coerce',
                )

        return long

    # ------------------------------------------------------------------
    # Frictionless metadata
    # ------------------------------------------------------------------

    def define_schema(self):
        """Build a frictionless Schema for the long-format data.

        Returns
        -------
        frictionless.Schema
        """
        import frictionless
        from frictionless import errors

        if not hasattr(self, 'LONG'):
            raise RuntimeError(
                "define_schema() requires LONG data. "
                "Either call melt() first or construct with return_long=True."
            )

        self.logger.info(f"Defining schema for {self.GROUP} / {self.SURVEY} / {self.YEAR}.")

        id_vars = ['GEO_ID', 'NAME'] if 'NAME' in self.DATA.columns else ['GEO_ID']

        fixed_fields = [
            {'name': 'GEO_ID', 'type': 'string', 'description': 'Census geography identifier'},
            {'name': 'reference_period', 'type': 'integer', 'description': 'Reference year'},
            {'name': 'survey', 'type': 'string', 'description': 'Census survey endpoint'},
            {'name': 'concept', 'type': 'string', 'description': 'Table concept description'},
            {'name': 'universe', 'type': 'string', 'description': 'Universe for the table'},
            {'name': 'variable_label', 'type': 'string', 'description': 'Human-readable variable label'},
            {'name': 'variable', 'type': 'string', 'description': 'Base variable code'},
        ]
        if 'NAME' in id_vars:
            fixed_fields.insert(1, {'name': 'NAME', 'type': 'string', 'description': 'Geography name'})

        fixed_names = {f['name'] for f in fixed_fields}
        value_fields = []
        for col in self.LONG.columns:
            if col in fixed_names:
                continue
            if col not in _VALUE_FIELD_DEFS:
                self.logger.error(f"Unexpected column '{col}' in LONG data.")
                raise errors.SchemaError(note=f"Unknown column: {col}")
            value_fields.append(_VALUE_FIELD_DEFS[col])

        descriptor = {
            'fields': fixed_fields + value_fields,
            'missingValues': MISSING_VALUES,
            'primaryKey': ['GEO_ID', 'reference_period', 'variable'],
        }

        result = frictionless.Schema.validate_descriptor(descriptor)
        if not result.valid:
            self.logger.error(f"Schema invalid: {result}")
            raise errors.SchemaError(note=str(result))

        return frictionless.Schema.from_descriptor(descriptor)

    def create_resource(self):
        """Build a frictionless Resource for this dataset.

        Requires :meth:`save` to have been called first (or for
        ``self.SCHEMA_FILENAME`` and ``self.FILENAME`` to be set).

        Returns
        -------
        frictionless.Resource
        """
        import frictionless

        sumlevel_str = f'{self.SUMLEVEL}s in ' if self.SUMLEVEL else ''
        title = f"{self.YEAR} {self.CONCEPT} for {sumlevel_str}{self.SCOPE}"
        description = (
            f"Census API data for {self.GROUP}: {self.CONCEPT} "
            f"from {self.SURVEY} in {self.YEAR} for {sumlevel_str}{self.SCOPE}."
        )

        descriptor = {
            'name': self.NAME,
            'title': title,
            'description': description,
            'path': self.FILENAME,
            'schema': self.SCHEMA_FILENAME,
            'sources': [
                {
                    'title': 'US Census Bureau API',
                    'path': self.REQUEST['url'],
                    '_params': self.REQUEST['params'],
                }
            ],
        }
        return frictionless.Resource.from_descriptor(descriptor)

    def save(self, output_path):
        """Write data, schema, and resource files to *output_path*.

        Produces three files:
        - ``{name}.long.csv``       — long-format data
        - ``{name}.schema.yaml``    — frictionless schema
        - ``{name}.resource.yaml``  — frictionless resource descriptor

        Parameters
        ----------
        output_path : str or path-like
        """
        import frictionless

        if not hasattr(self, 'LONG'):
            raise RuntimeError(
                "save() requires LONG data. "
                "Construct with return_long=True or call melt() first."
            )

        output = Path(output_path)
        output.mkdir(parents=True, exist_ok=True)

        self.DATAPATH = output
        self.FILENAME = f"{self.NAME}.long.csv"
        self.SCHEMA_FILENAME = f"{self.NAME}.schema.yaml"

        # Data
        self.logger.info(f"Writing data to {output / self.FILENAME}.")
        self.LONG.to_csv(output / self.FILENAME, index=False)

        # Schema
        self.logger.info(f"Writing schema to {output / self.SCHEMA_FILENAME}.")
        self.SCHEMA = self.define_schema()
        self.SCHEMA.to_yaml(str(output / self.SCHEMA_FILENAME))

        # Resource — frictionless resolves relative paths from CWD
        resource = self.create_resource()
        resource_filename = f"{self.NAME}.resource.yaml"
        self.logger.info(f"Writing resource to {output / resource_filename}.")

        cwd = os.getcwd()
        try:
            os.chdir(output)
            resource.to_yaml(resource_filename)
            result = frictionless.Resource(resource_filename).validate()
        finally:
            os.chdir(cwd)

        if not result.valid:
            self.logger.error(f"Resource validation failed: {result.stats}")
            raise RuntimeError("Resource validation failed after save.")

        self.logger.info("Save complete and resource validated.")


# ---------------------------------------------------------------------------
# DimensionTable
# ---------------------------------------------------------------------------

class DimensionTable:
    """Creates wide and percentage tables from CensusAPI long-format data.

    Parameters
    ----------
    long_data : pandas.DataFrame
        The ``LONG`` DataFrame produced by :class:`CensusAPI`.
    variable_map : dict, optional
        Mapping of existing variable labels to new collapsed labels.
        Must be accompanied by *variable_order*.
    variable_order : dict, optional
        Sort-order mapping for the collapsed labels.
    """

    def __init__(self, long_data, variable_map=None, variable_order=None):
        self.logger = logging.getLogger(__name__).getChild(self.__class__.__name__)
        self.LONG = long_data.copy()

        self.variable_type = [
            c for c in self.LONG.columns
            if c not in (
                'concept', 'universe', 'survey', 'GEO_ID', 'NAME',
                'reference_period', 'variable_label', 'variable',
            )
        ]

        if variable_map is not None:
            if variable_order is None:
                raise ValueError("variable_order is required when variable_map is provided.")
            self.logger.info(
                f"Applying variable map: {list(variable_map)} → {list(variable_order)}."
            )
            self.LONG['variable_label'], self.LONG['variable'] = find_replace_variable_map(
                self.LONG['variable_label'], self.LONG['variable'], map=variable_map
            )
            # TODO: propagate MOE correctly through aggregation
            # https://github.com/morpc/morpc-py/issues/113
            self.LONG = (
                self.LONG
                .groupby([
                    'concept', 'universe', 'GEO_ID', 'NAME',
                    'reference_period', 'variable_label', 'variable',
                ])
                .sum()
                .reset_index()
            )

    def wide(self, droplevels=None):
        """Pivot LONG data to wide format.

        Parameters
        ----------
        droplevels : int or list of int, optional
            Index levels to collapse (see below).

        Returns
        -------
        pandas.DataFrame
            Wide-format DataFrame with a MultiIndex on columns (GEO_ID × value type).
        """
        self.DESC_TABLE = self.create_description_table()

        long = self.LONG.copy()
        for col in long.columns:
            long[col] = [np.nan if v in MISSING_VALUES else v for v in long[col]]

        non_value_cols = [
            c for c in long.columns
            if c not in ('variable', 'estimate', 'total', 'variable_label', 'moe')
        ]
        wide = long.pivot(
            index='variable',
            columns=non_value_cols,
            values=self.variable_type,
        )

        col_level_names = wide.columns.names
        wide.columns = wide.columns.to_list()
        wide = wide.join(self.DESC_TABLE)
        wide = wide.set_index(list(self.DESC_TABLE.columns))
        wide.columns = pd.MultiIndex.from_tuples(wide.columns)
        wide.columns.names = col_level_names
        wide = wide.sort_index(level='GEO_ID', axis=1).drop_duplicates()

        if droplevels is None:
            return wide

        index_names = list(wide.index.names)
        wide = wide.reset_index()

        if len(index_names) == 1:
            self.logger.error("Cannot drop the only remaining index level.")
            raise RuntimeError("Cannot drop the only remaining index level.")

        if not isinstance(droplevels, list):
            droplevels = [droplevels]

        for level in droplevels:
            if level not in index_names:
                raise ValueError(f"Level {level} not in index {index_names}.")

            if level == index_names[-1]:
                wide = wide.loc[wide[level] == ''].drop(columns=[level])

            elif level == 0:
                self.logger.warning(
                    "Dropping the Total level may cause issues with percentage calculations."
                )
                wide = wide.loc[wide[index_names[1]] != ''].drop(columns=[level])

            else:
                wide = pd.concat([
                    wide.loc[wide[level] == ''],
                    wide.loc[wide[index_names[index_names.index(level) + 1]] != ''],
                ])
                wide = (
                    wide.groupby([c for c in index_names if c != level])
                    .sum()
                    .reset_index()
                    .drop(columns=[level])
                )

            index_names.remove(level)

        return wide.set_index([c for c in index_names if c not in droplevels])

    def percent(self, droplevels=None, decimals=2):
        """Compute column percentages relative to the Total row.

        Returns
        -------
        pandas.DataFrame
        """
        self.WIDE = self.wide(droplevels=droplevels)

        total = self.WIDE.T.iloc[:, 0].copy()
        pct = self.WIDE.T.iloc[:, 1:].copy()
        for col in pct:
            pct[col] = (pct[col].astype(float) / total.astype(float) * 100).round(decimals)

        pct.columns = pct.columns.droplevel(0)
        pct = pct.reset_index()
        pct['universe'] = [f'% of {u.lower()}' for u in pct['universe']]

        non_value_cols = [
            c for c in self.WIDE.T.reset_index().columns
            if c not in self.variable_type
        ]
        return pct.set_index(non_value_cols).T

    def create_description_table(self):
        """Build a structured label table from variable_label strings.

        Splits ``!!``-delimited labels into columns and assigns each
        label fragment to the column where it appears most frequently,
        producing a tidy dimension table for use as a wide-format index.

        Returns
        -------
        pandas.DataFrame
        """
        var_df = (
            self.LONG[['variable', 'variable_label']]
            .drop_duplicates()
            .set_index('variable')
        )
        var_df = var_df.join(
            var_df['variable_label'].str.split('!!', expand=True)
        ).drop(columns='variable_label')

        # Collect every unique fragment and the columns it appears in
        all_values = [
            v for col in var_df.columns
            for v in var_df[col].dropna().unique()
        ]

        col_freq = {}
        for val in set(all_values):
            col_freq[val] = {
                col: var_df[col].value_counts().get(val, 0)
                for col in var_df.columns
            }

        # Map each fragment to the column where it appears most often
        dominant_col = {val: max(freq, key=freq.get) for val, freq in col_freq.items()}

        # Rebuild var_df placing each fragment in its dominant column
        var_df_out = pd.DataFrame('', index=var_df.index, columns=var_df.columns)
        for idx, row in var_df.iterrows():
            for fragment in row.dropna():
                if fragment in dominant_col:
                    var_df_out.loc[idx, dominant_col[fragment]] = fragment

        return var_df_out
