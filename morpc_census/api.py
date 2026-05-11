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
        kw = {'params': {'key': k}} if (k := _get_api_key()) else {}
        result = {}
        for dataset in get_json_safely(CENSUS_DATA_BASE_URL, **kw)['dataset']:
            if 'c_vintage' in dataset:
                endpoint = "/".join(dataset['c_dataset'])
                result.setdefault(endpoint, []).append(dataset['c_vintage'])
        _avail_endpoints_cache = dict(sorted(result.items()))
    return _avail_endpoints_cache


def _get_api_key() -> str | None:
    """Return CENSUS_API_KEY from environment, with .env file as fallback.

    dotenv convention (override=False): environment variables already set take
    precedence over values in the .env file. Searches for .env starting from
    the current working directory and walking up toward the filesystem root.
    """
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv(usecwd=True), override=False)
    return os.environ.get('CENSUS_API_KEY')


# ---------------------------------------------------------------------------
# Census API endpoint classes
# ---------------------------------------------------------------------------

class Endpoint:
    """A Census API survey at a specific vintage year (e.g. ``'acs/acs5'``, 2023).

    Validates the survey name against :data:`IMPLEMENTED_ENDPOINTS` and the year
    against the Census API's available vintages at construction.

    Parameters
    ----------
    survey : str
        Survey/table name, e.g. ``'acs/acs5'``, ``'dec/pl'``.
        See :data:`IMPLEMENTED_ENDPOINTS`.
    year : int
        Vintage year. Validated against the survey's available years.
    """

    def __init__(self, survey: str, year: int) -> None:
        if survey not in IMPLEMENTED_ENDPOINTS:
            raise ValueError(
                f"{survey!r} is not available or not yet implemented. "
                f"See IMPLEMENTED_ENDPOINTS."
            )
        self.survey = survey
        year = int(year)
        if year not in self.vintages:
            raise ValueError(
                f"{year} is not an available vintage for {self.survey!r}. "
                f"Available: {self.vintages}"
            )
        self.year = year

    def __repr__(self) -> str:
        return f"Endpoint({self.survey!r}, {self.year})"

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, Endpoint)
            and self.survey == other.survey
            and self.year == other.year
        )

    def __hash__(self) -> int:
        return hash((self.survey, self.year))

    @cached_property
    def vintages(self) -> list[int]:
        """Available vintage years for this survey (fetched once, then cached)."""
        return get_all_avail_endpoints().get(self.survey, [])

    @property
    def url(self) -> str:
        """Base Census API query URL for this endpoint."""
        return f"{CENSUS_DATA_BASE_URL}/{self.year}/{self.survey}?"

    @cached_property
    def groups(self) -> dict:
        """All variable groups for this endpoint, keyed by group code (fetched once, then cached)."""
        from morpc.req import get_json_safely
        logger.debug(f"Fetching groups for {self.year} {self.survey}")
        kw = {'params': {'key': k}} if (k := _get_api_key()) else {}
        data = get_json_safely(
            f"{CENSUS_DATA_BASE_URL}/{self.year}/{self.survey}/groups.json", **kw
        )
        return dict(sorted({
            g['name']: {'description': g['description'], 'variables': g['variables']}
            for g in data['groups']
        }.items()))


class Group:
    """A variable group within a Census API endpoint (e.g. ``'B01001'``).

    Parameters
    ----------
    endpoint : Endpoint
        The survey endpoint this group belongs to.
    code : str
        Group code (e.g. ``'B01001'``). Case-insensitive; stored upper-cased.
        Validated against :attr:`Endpoint.groups` at construction.
    """

    def __init__(self, endpoint: Endpoint, code: str) -> None:
        if not isinstance(endpoint, Endpoint):
            raise TypeError(
                f"endpoint must be an Endpoint instance, got {type(endpoint).__name__!r}."
            )
        self.endpoint = endpoint
        code = code.upper()
        if code not in self.endpoint.groups:
            raise ValueError(
                f"{code!r} is not a valid group in "
                f"{self.endpoint.survey!r} {self.endpoint.year}."
            )
        self.code = code

    def __repr__(self) -> str:
        return f"Group({self.endpoint.survey!r}, {self.endpoint.year}, {self.code!r})"

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, Group)
            and self.endpoint == other.endpoint
            and self.code == other.code
        )

    def __hash__(self) -> int:
        return hash((self.endpoint.survey, self.endpoint.year, self.code))

    @property
    def description(self) -> str:
        """Group description (read from :attr:`Endpoint.groups` — no extra network call)."""
        return self.endpoint.groups[self.code]['description']

    @property
    def universe(self) -> str:
        """Universe description string."""
        from morpc.req import get_json_safely
        kw = {'params': {'key': k}} if (k := _get_api_key()) else {}
        data = get_json_safely(
            f"{CENSUS_DATA_BASE_URL}/{self.endpoint.year}/{self.endpoint.survey}/groups",
            **kw,
        )
        match = [x for x in data['groups'] if x['name'] == self.code]
        if not match:
            raise ValueError(
                f"Group {self.code!r} not found in "
                f"{self.endpoint.year} {self.endpoint.survey}."
            )
        return match[0]['universe ']  # trailing space is present in the Census API response

    @cached_property
    def variables(self) -> dict:
        """Variable metadata dict for this group (fetched once, then cached)."""
        from morpc.req import get_json_safely
        kw = {'params': {'key': k}} if (k := _get_api_key()) else {}
        data = get_json_safely(
            f"{CENSUS_DATA_BASE_URL}/{self.endpoint.year}/{self.endpoint.survey}"
            f"/groups/{self.code}.json",
            **kw,
        )
        return {
            k: data['variables'][k]
            for k in sorted(data['variables'])
            if k not in ('GEO_ID', 'NAME')
        }


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
    key = _get_api_key()
    if key:
        params = {**params, 'key': key}
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

def censusapi_name(endpoint: Endpoint, scope: str | Scope, group: str | Group | None = None, sumlevel: str | SumLevel | None = None, variables: list[str] | None = None) -> str:
    """Construct a canonical, machine-readable name for a CensusAPI dataset."""
    from morpc_census.geos import Scope as _Scope, SumLevel as _SumLevel

    scope_name = scope.name if isinstance(scope, _Scope) else scope
    group_code = group.code if isinstance(group, Group) else group  # None when no group

    if sumlevel is not None:
        sl = sumlevel if isinstance(sumlevel, _SumLevel) else _SumLevel(sumlevel)
        sumlevel_part = f"{(sl.hierarchy_string or sl.name).replace('-', '').lower()}-"
    else:
        sumlevel_part = ''

    group_part = f"-{group_code}" if group_code is not None else ''
    var_part = '-select-variables' if variables is not None else ''
    return (
        f"census-{endpoint.survey.replace('/', '-')}-{endpoint.year}"
        f"-{sumlevel_part}{scope_name}{group_part}{var_part}"
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
    endpoint : Endpoint
        Survey and vintage year, e.g. ``Endpoint('acs/acs5', 2023)``.
    scope : str or Scope
        Geographic scope key (e.g. ``'region15'``) or a ``Scope`` instance.
        See ``morpc_census.geos.SCOPES`` for available keys.
    group : str, Group, or None
        Variable group code, e.g. ``'B01001'``. Required if *variables* is
        not provided. When omitted, *variables* must be given and are fetched
        directly without group validation.
    sumlevel : str or SumLevel, optional
        Geographic summary level query name (e.g. ``'county'``, ``'tract'``)
        or a ``SumLevel`` instance.  See ``morpc_census.geos.SumLevel``.
    variables : list of str, optional
        Specific variables to retrieve. Required when *group* is not provided.
        When both are given, variables must be a subset of the group's variables.
    return_long : bool
        If ``True`` (default) compute ``self.long`` immediately after fetch.
    """

    def __init__(
        self,
        endpoint: Endpoint,
        scope: str | Scope,
        group: str | Group | None = None,
        sumlevel: str | SumLevel | None = None,
        variables: list[str] | None = None,
        return_long: bool = True,
    ):
        if group is None and variables is None:
            raise ValueError("At least one of 'group' or 'variables' must be provided.")

        from morpc_census.geos import Scope as _Scope, SumLevel as _SumLevel

        self.scope = scope if isinstance(scope, _Scope) else _Scope(scope.lower())
        self.sumlevel = (
            None if sumlevel is None
            else sumlevel if isinstance(sumlevel, _SumLevel)
            else _SumLevel(sumlevel.lower())
        )
        self.variables = (
            [v.upper() for v in variables] if variables is not None else None
        )

        if group is not None:
            self.group = group if isinstance(group, Group) else Group(endpoint, group.upper())
        else:
            self.group = None
        self.endpoint = self.group.endpoint if self.group is not None else endpoint

        if self.variables is not None and self.group is not None:
            invalid = [v for v in self.variables if v not in self.group.variables]
            if invalid:
                raise ValueError(f"Variables not found in {self.group.code}: {invalid}")

        self.logger = (
            logging.getLogger(__name__)
            .getChild(self.__class__.__name__)
            .getChild(self.name)
        )
        self.logger.info(f"Initializing CensusAPI for {self.name}.")

        self.logger.info("Building request URL and parameters.")
        self.request = self._build_request()

        self.logger.info(
            f"Fetching data from {self.request['url']} "
            f"with params {self.request['params']}."
        )
        try:
            self.data = fetch(self.request['url'], self.request['params']).reset_index()
        except Exception as e:
            self.logger.error(f"Failed to retrieve data: {e}")
            raise RuntimeError("Failed to retrieve data from Census API.") from e

        n_dupes = self.data.duplicated().sum()
        if n_dupes:
            self.logger.warning(
                f"Removing {n_dupes} duplicate rows "
                "(can occur when ucgid=pseudo() is used for geographies)."
            )
            self.data = self.data.loc[~self.data.duplicated()].reset_index(drop=True)

        if return_long:
            self.long = self.melt()

    # ------------------------------------------------------------------
    # Derived properties
    # ------------------------------------------------------------------

    @cached_property
    def universe(self) -> str:
        """Universe description string. Falls back to 2023 vintage when year < 2023."""
        if self.group is None:
            return 'Not defined — no group specified'
        try:
            source = (
                self.group if self.endpoint.year >= 2023
                else Group(Endpoint(self.endpoint.survey, 2023), self.group.code)
            )
            return source.universe
        except Exception as e:
            self.logger.warning(
                f"Universe not defined for {self.endpoint.survey}/{self.group.code}: {e}"
            )
            return 'Not defined in API — see CensusAPI.request for endpoint details'

    @cached_property
    def vars(self) -> dict:
        """Variable metadata dict. When group is set, includes label metadata and respects
        the variables filter. Without a group, returns placeholder entries keyed by variable code."""
        if self.group is not None:
            all_vars = dict(self.group.variables)
            if self.variables is not None:
                return {k: v for k, v in all_vars.items() if k in self.variables}
            return all_vars
        return {v: {} for v in self.variables}

    @cached_property
    def name(self) -> str:
        """Canonical, machine-readable dataset name."""
        return self._build_name()

    @property
    def geoidfqs(self):
        """Return the GEO_ID column parsed as a list of GeoIDFQ objects."""
        from morpc_census.geos import GeoIDFQ
        return [GeoIDFQ.parse(g) for g in self.data['GEO_ID']]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_name(self) -> str:
        return censusapi_name(
            self.endpoint,
            self.scope,
            self.group,
            sumlevel=self.sumlevel,
            variables=self.variables,
        )

    def _build_request(self) -> dict:
        """Build the Census API request dict from already-normalized instance attributes."""
        from morpc_census.geos import geoinfo_from_scope_sumlevel
        get_param = (
            ','.join(self.variables) if self.variables is not None
            else f"group({self.group.code})"
        )
        geo_param = geoinfo_from_scope_sumlevel(self.scope, self.sumlevel, output='params')
        params = {'get': get_param}
        params.update(geo_param)
        return {'url': self.endpoint.url, 'params': params}

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

        id_vars = ['GEO_ID', 'NAME'] if 'NAME' in self.data.columns else ['GEO_ID']
        long = self.data.melt(id_vars=id_vars, var_name='variable', value_name='value')

        # Keep only the requested variables
        long = long.loc[long['variable'].isin(self.vars)]

        # Determine the type suffix (E, M, PE, PM, N)
        def _type_code(var):
            match = re.findall(r'_[0-9]+([A-Z]{1,2})$', var)
            if match:
                return match[0]
            # Fall back to the first label segment for non-standard codes
            label = self.vars.get(var, {}).get('label', '')
            return label.split('!!')[0].lower() if label else ''

        long['variable_type'] = long['variable'].map(
            lambda v: VARIABLE_TYPES.get(_type_code(v), _type_code(v))
        )
        long = long.loc[long['variable_type'].isin(VARIABLE_TYPES.values())]

        # Human-readable label (everything after the first '!!')
        def _var_label(var):
            label = self.vars.get(var, {}).get('label', var)
            return re.split('!!', label, maxsplit=1)[1] if '!!' in label else label

        long['variable_label'] = long['variable'].map(_var_label)

        # Strip the type suffix to get the base variable code (B01001_001E → B01001_001)
        long['variable'] = long['variable'].map(
            lambda v: (
                re.findall(r'([A-Z0-9_]+[0-9]+)[A-Z]+$', v)[0]
                if v[-1].isalpha() and re.findall(r'([A-Z0-9_]+[0-9]+)[A-Z]+$', v)
                else v
            )
        )

        long['reference_period'] = self.endpoint.year
        long['universe'] = self.universe
        long['survey'] = self.endpoint.survey
        long['concept'] = self.group.description.capitalize() if self.group is not None else ''

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

        if not hasattr(self, 'long'):
            raise RuntimeError(
                "define_schema() requires LONG data. "
                "Either call melt() first or construct with return_long=True."
            )

        group_tag = f"{self.group.code} / " if self.group is not None else ''
        self.logger.info(f"Defining schema for {group_tag}{self.endpoint.survey} / {self.endpoint.year}.")

        id_vars = ['GEO_ID', 'NAME'] if 'NAME' in self.data.columns else ['GEO_ID']

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
        for col in self.long.columns:
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
        ``self.schema_filename`` and ``self.filename`` to be set).

        Returns
        -------
        frictionless.Resource
        """
        import frictionless

        sumlevel_str = f'{self.sumlevel.plural} in ' if self.sumlevel is not None else ''
        year = self.endpoint.year
        survey = self.endpoint.survey
        if self.group is not None:
            title = f"{year} {self.group.description} for {sumlevel_str}{self.scope.name}"
            description = (
                f"Census API data for {self.group.code}: {self.group.description} "
                f"from {survey} in {year} "
                f"for {sumlevel_str}{self.scope.name}."
            )
        else:
            vars_str = ', '.join(self.variables[:3])
            if len(self.variables) > 3:
                vars_str += f', ... ({len(self.variables)} total)'
            title = f"{year} selected variables for {sumlevel_str}{self.scope.name}"
            description = (
                f"Census API data for {vars_str} "
                f"from {survey} in {year} "
                f"for {sumlevel_str}{self.scope.name}."
            )

        descriptor = {
            'name': self.name,
            'title': title,
            'description': description,
            'path': self.filename,
            'schema': self.schema_filename,
            'sources': [
                {
                    'title': 'US Census Bureau API',
                    'path': self.request['url'],
                    '_params': self.request['params'],
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

        if not hasattr(self, 'long'):
            raise RuntimeError(
                "save() requires LONG data. "
                "Construct with return_long=True or call melt() first."
            )

        output = Path(output_path)
        output.mkdir(parents=True, exist_ok=True)

        self.datapath = output
        self.filename = f"{self.name}.long.csv"
        self.schema_filename = f"{self.name}.schema.yaml"

        # Data
        self.logger.info(f"Writing data to {output / self.filename}.")
        self.long.to_csv(output / self.filename, index=False)

        # Schema
        self.logger.info(f"Writing schema to {output / self.schema_filename}.")
        self.schema = self.define_schema()
        self.schema.to_yaml(str(output / self.schema_filename))

        # Resource — frictionless resolves relative paths from CWD
        resource = self.create_resource()
        resource_filename = f"{self.name}.resource.yaml"
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
        self.long = long_data.copy()

        self.variable_type = [
            c for c in self.long.columns
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
            self.long['variable_label'], self.long['variable'] = find_replace_variable_map(
                self.long['variable_label'], self.long['variable'], map=variable_map
            )
            # TODO: propagate MOE correctly through aggregation
            # https://github.com/morpc/morpc-py/issues/113
            self.long = (
                self.long
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
        self.desc_table = self.create_description_table()

        long = self.long.copy()
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
        wide = wide.join(self.desc_table)
        wide = wide.set_index(list(self.desc_table.columns))
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
        self._wide = self.wide(droplevels=droplevels)

        total = self._wide.T.iloc[:, 0].copy()
        pct = self._wide.T.iloc[:, 1:].copy()
        for col in pct:
            pct[col] = (pct[col].astype(float) / total.astype(float) * 100).round(decimals)

        pct.columns = pct.columns.droplevel(0)
        pct = pct.reset_index()
        pct['universe'] = [f'% of {u.lower()}' for u in pct['universe']]

        non_value_cols = [
            c for c in self._wide.T.reset_index().columns
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
            self.long[['variable', 'variable_label']]
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
