"""
Connects to the US Census Bureau API, retrieves survey data, and structures it
as long-format tables backed by frictionless metadata.

Census API root: https://api.census.gov/data/
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections import OrderedDict
from functools import cached_property
from io import StringIO
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from morpc_census.geos import Scope, SumLevel

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Domain lookup tables (defined in constants.py, re-exported for backwards compat)
# ---------------------------------------------------------------------------

from morpc_census.constants import (  # noqa: E402
    HIGHLEVEL_GROUP_DESC,
    HIGHLEVEL_DESC_FROM_ID,
    AGEGROUP_MAP,
    AGEGROUP_SORT_ORDER,
    RACE_TABLE_MAP,
    EDUCATION_ATTAIN_MAP,
    EDUCATION_ATTAIN_SORT_ORDER,
    INCOME_TO_POVERTY_MAP,
    INCOME_TO_POVERTY_SORT_ORDER,
    NTD_AGEMAP,
    NTD_AGEMAP_ORDER,
)

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
# Network helpers — called by class cached_properties; also public for direct use
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Census API endpoint classes
# ---------------------------------------------------------------------------

class SurveyTable:
    """A Census API survey/table endpoint (e.g. ``'acs/acs5'``, ``'dec/pl'``).

    Validates against :data:`IMPLEMENTED_ENDPOINTS` at construction.
    No network call is made until :attr:`vintages` is accessed.
    """

    def __init__(self, name: str) -> None:
        if name not in IMPLEMENTED_ENDPOINTS:
            raise ValueError(
                f"{name!r} is not available or not yet implemented. "
                f"See IMPLEMENTED_ENDPOINTS."
            )
        self.name = name

    def __repr__(self) -> str:
        return f"SurveyTable({self.name!r})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, SurveyTable) and self.name == other.name

    def __hash__(self) -> int:
        return hash(self.name)

    @cached_property
    def vintages(self) -> list[int]:
        """Available vintage years for this survey (fetched once, then cached)."""
        return get_all_avail_endpoints().get(self.name, [])


class Vintage:
    """A Census API survey at a specific vintage year.

    Parameters
    ----------
    survey : str or SurveyTable
        Survey/table endpoint. Strings are normalized to a :class:`SurveyTable`.
    year : int
        Vintage year. Validated against the survey's available years at construction.
    """

    def __init__(self, survey: str | SurveyTable, year: int) -> None:
        self.survey = survey if isinstance(survey, SurveyTable) else SurveyTable(survey)
        year = int(year)
        if year not in self.survey.vintages:
            raise ValueError(
                f"{year} is not an available vintage for {self.survey.name!r}. "
                f"Available: {self.survey.vintages}"
            )
        self.year = year

    def __repr__(self) -> str:
        return f"Vintage({self.survey.name!r}, {self.year})"

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, Vintage)
            and self.survey == other.survey
            and self.year == other.year
        )

    def __hash__(self) -> int:
        return hash((self.survey.name, self.year))

    @property
    def url(self) -> str:
        """Base Census API query URL for this vintage."""
        return f"{CENSUS_DATA_BASE_URL}/{self.year}/{self.survey.name}?"

    @cached_property
    def groups(self) -> dict:
        """All variable groups for this vintage, keyed by group code (fetched once, then cached)."""
        return get_table_groups(self.survey.name, self.year)


class Group:
    """A variable group within a Census API survey vintage (e.g. ``'B01001'``).

    Parameters
    ----------
    vintage : Vintage
        The survey vintage this group belongs to.
    code : str
        Group code (e.g. ``'B01001'``). Case-insensitive; stored upper-cased.
        Validated against :attr:`Vintage.groups` at construction.
    """

    def __init__(self, vintage: Vintage, code: str) -> None:
        if not isinstance(vintage, Vintage):
            raise TypeError(
                f"vintage must be a Vintage instance, got {type(vintage).__name__!r}."
            )
        self.vintage = vintage
        code = code.upper()
        if code not in self.vintage.groups:
            raise ValueError(
                f"{code!r} is not a valid group in "
                f"{self.vintage.survey.name!r} {self.vintage.year}."
            )
        self.code = code

    def __repr__(self) -> str:
        return f"Group({self.vintage.survey.name!r}, {self.vintage.year}, {self.code!r})"

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, Group)
            and self.vintage == other.vintage
            and self.code == other.code
        )

    def __hash__(self) -> int:
        return hash((self.vintage.survey.name, self.vintage.year, self.code))

    @property
    def description(self) -> str:
        """Group description (read from :attr:`Vintage.groups` — no extra network call)."""
        return self.vintage.groups[self.code]['description']

    @property
    def universe(self) -> str:
        """Universe description string."""
        return get_group_universe(self.vintage.survey.name, self.vintage.year, self.code)

    @cached_property
    def variables(self) -> dict:
        """Variable metadata dict for this group (fetched once, then cached)."""
        return get_group_variables(self.vintage.survey.name, self.vintage.year, self.code)


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

def censusapi_name(survey_table: str, year: int, scope: str | Scope, group: str, sumlevel: str | SumLevel | None = None, variables: list[str] | None = None) -> str:
    """Construct a canonical, machine-readable name for a CensusAPI dataset."""
    from morpc_census.geos import Scope as _Scope, SumLevel as _SumLevel

    scope_name = scope.name if isinstance(scope, _Scope) else scope

    if sumlevel is not None:
        sl = sumlevel if isinstance(sumlevel, _SumLevel) else _SumLevel(sumlevel)
        sumlevel_part = f"{(sl.hierarchy_string or sl.name).replace('-', '').lower()}-"
    else:
        sumlevel_part = ''

    var_part = '-select-variables' if variables is not None else ''
    return (
        f"census-{survey_table.replace('/', '-')}-{year}"
        f"-{sumlevel_part}{scope_name}-{group}{var_part}"
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
    scope : str or Scope
        Geographic scope key (e.g. ``'region15'``) or a ``Scope`` instance.
        See ``morpc_census.geos.SCOPES`` for available keys.
    sumlevel : str or SumLevel, optional
        Geographic summary level query name (e.g. ``'county'``, ``'tract'``)
        or a ``SumLevel`` instance.  See ``morpc_census.geos.SumLevel``.
    variables : list of str, optional
        Specific variables to retrieve.  If ``None`` all variables in the
        group are retrieved.
    return_long : bool
        If ``True`` (default) compute ``self.LONG`` immediately after fetch.
    """

    def __init__(
        self,
        survey_table: str | SurveyTable,
        year: int,
        group: str | Group,
        scope: str | Scope,
        sumlevel: str | SumLevel | None = None,
        variables: list[str] | None = None,
        return_long: bool = True,
    ):
        from morpc_census.geos import Scope as _Scope, SumLevel as _SumLevel

        self.SCOPE = scope if isinstance(scope, _Scope) else _Scope(scope.lower())
        self.SUMLEVEL = (
            None if sumlevel is None
            else sumlevel if isinstance(sumlevel, _SumLevel)
            else _SumLevel(sumlevel.lower())
        )
        self.VARIABLES = (
            [v.upper() for v in variables] if variables is not None else None
        )

        # Normalize to a Group instance — validates survey, year, and group code.
        if isinstance(group, Group):
            self.VARIABLE_GROUP = group
        else:
            self.VARIABLE_GROUP = Group(Vintage(survey_table, int(year)), group.upper())

        self.SURVEY = self.VARIABLE_GROUP.vintage.survey.name
        self.YEAR = self.VARIABLE_GROUP.vintage.year
        self.GROUP = self.VARIABLE_GROUP.code

        if self.VARIABLES is not None:
            invalid = [v for v in self.VARIABLES if v not in self.VARIABLE_GROUP.variables]
            if invalid:
                raise ValueError(f"Variables not found in {self.GROUP}: {invalid}")

        self.NAME = censusapi_name(self.SURVEY, self.YEAR, self.SCOPE, self.GROUP, self.SUMLEVEL, variables)
        self.logger = (
            logging.getLogger(__name__)
            .getChild(self.__class__.__name__)
            .getChild(self.NAME)
        )
        self.logger.info(f"Initializing CensusAPI for {self.NAME}.")

        self._fetch_metadata()

        self.logger.info("Building request URL and parameters.")
        self.REQUEST = self._build_request()

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
        self.CONCEPT = self.VARIABLE_GROUP.description

        try:
            if self.YEAR >= 2023:
                self.UNIVERSE = self.VARIABLE_GROUP.universe
            else:
                self.UNIVERSE = Group(Vintage(self.SURVEY, 2023), self.GROUP).universe
        except Exception as e:
            self.UNIVERSE = (
                'Not defined in API — see CensusAPI.REQUEST for endpoint details'
            )
            self.logger.warning(
                f"Universe not defined for {self.SURVEY}/{self.GROUP}: {e}"
            )

        self.VARS = dict(self.VARIABLE_GROUP.variables)
        if self.VARIABLES is not None:
            self.VARS = {k: v for k, v in self.VARS.items() if k in self.VARIABLES}

    def _build_request(self) -> dict:
        """Build the Census API request dict from already-normalized instance attributes."""
        from morpc_census.geos import geoinfo_from_scope_sumlevel
        get_param = (
            ','.join(self.VARIABLES) if self.VARIABLES is not None
            else f"group({self.GROUP})"
        )
        geo_param = geoinfo_from_scope_sumlevel(self.SCOPE, self.SUMLEVEL, output='params')
        params = {'get': get_param}
        params.update(geo_param)
        return {'url': self.VARIABLE_GROUP.vintage.url, 'params': params}

    def validate(self) -> None:
        """No-op — validation now happens during Group construction in __init__."""

    # ------------------------------------------------------------------
    # Convenience properties
    # ------------------------------------------------------------------

    @property
    def scope_obj(self):
        """Return the Scope object for this dataset's geographic scope."""
        return self.SCOPE

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

        sumlevel_str = f'{self.SUMLEVEL.plural} in ' if self.SUMLEVEL is not None else ''
        title = f"{self.YEAR} {self.CONCEPT} for {sumlevel_str}{self.SCOPE.name}"
        description = (
            f"Census API data for {self.GROUP}: {self.CONCEPT} "
            f"from {self.SURVEY} in {self.YEAR} for {sumlevel_str}{self.SCOPE.name}."
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
