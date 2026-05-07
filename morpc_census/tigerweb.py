from __future__ import annotations

import logging
from os import PathLike
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from morpc_census.geos import Scope, SumLevel

logger = logging.getLogger(__name__)

current_endpoints: dict[str, int] = {
    'public use microdata areas': 0,
    'zip code tabulation areas': 2,
    'tribal tracts': 4,
    'tribal block groups': 6,
    'tracts': 8,
    'block groups': 10,
    'unified school districts': 14,
    'secondary school districts': 16,
    'elementary school districts': 18,
    'school district administrative areas': 84,
    'estates': 20,
    'county subdivisions': 22,
    'subbarrios': 24,
    'consolidated cities': 26,
    'incorporated places': 28,
    'designated places': 30,
    'alaska native regional corporations': 32,
    'tribal subdivisions': 34,
    'federal american indian reservations': 36,
    'off-reservation trust lands': 38,
    'state american indian reservations': 40,
    'hawaiian home lands': 42,
    'alaska native village statistical areas': 44,
    'oklahoma tribal statistical areas': 46,
    'state designated tribal statistical areas': 48,
    'tribal designated statistical areas': 50,
    'american indian joint-use areas': 52,
    'congressional districts': 54,
    'state legislative districts - upper': 56,
    'state legislative districts - lower': 58,
    'divisions': 60,
    'regions': 62,
    'states': 80,
    'counties': 82,
    'urban areas': 88,
    'combined statistical areas': 97,
    'metropolitan divisions': 95,
    'metropolitan statistical areas': 93,
    'micropolitan statistical areas': 91,
}


def get_tigerweb_layers_map(
    year: int = 2023,
    survey: Literal['ACS', 'DEC'] = 'ACS',
) -> dict[str, int]:
    """Return a mapping of layer names to MapServer IDs for a TIGERweb service.

    Parameters
    ----------
    year : int
        Vintage year of the TIGERweb service (e.g. ``2024``).
    survey : {'ACS', 'DEC'}
        Survey type. ``'ACS'`` requires 2012 or later; ``'DEC'`` accepts 2010 or 2020.

    Returns
    -------
    dict[str, int]
        Layer names (lower-cased, year/prefix stripped) mapped to their MapServer layer IDs.

    Examples
    --------
    >>> layers = get_tigerweb_layers_map(2024, survey='ACS')
    >>> layers['tracts']
    8
    """
    import pandas as pd
    import requests
    import re

    if survey not in ['ACS', 'DEC']:
        raise ValueError(f"Invalid survey type {survey!r}. Must be 'ACS' or 'DEC'.")
    if survey == 'DEC' and year not in [2010, 2020]:
        raise ValueError(f"Invalid year {year} for Decennial Census. Must be 2010 or 2020.")
    if survey == 'ACS' and pd.to_numeric(year) < 2012:
        raise ValueError(f"Invalid year {year} for ACS. Must be 2012 or later.")

    survey_slug = 'Census' if survey == 'DEC' else survey
    mapserver_url = f"https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_{survey_slug}{year}/MapServer/"

    logger.info(f"Fetching metadata from {mapserver_url}?f=pjson")
    r = requests.get(f"{mapserver_url}?f=pjson")
    if r.status_code != 200:
        raise RuntimeError(f"Failed to fetch data from {mapserver_url}: {r.status_code}")
    logger.info(f"Successful fetch from {r.url}")

    try:
        layers_json = r.json()
    except Exception:
        r.close()
        raise RuntimeError(f"Failed to parse JSON from {mapserver_url}")
    r.close()

    layers = pd.DataFrame(layers_json['layers'])[['id', 'name']]
    layers = layers.loc[~layers['name'].str.contains('Labels')]
    layer_map: dict[str, int] = layers.set_index('name')['id'].to_dict()

    layer_map = {k.lower(): v for k, v in layer_map.items()}
    layer_map = {k.replace('census ', ''): v for k, v in layer_map.items()}
    layer_map = {re.sub(r'^(19|20)\d{2} ', '', k): v for k, v in layer_map.items()}
    layer_map = {re.sub(r'^\d{3}(st|nd|rd|th) ', '', k): v for k, v in layer_map.items()}

    return layer_map


def get_layer_url(
    layer_name: str | SumLevel,
    year: int | None = None,
    survey: Literal['current', 'ACS', 'DEC'] = 'current',
) -> str:
    """Return the MapServer endpoint URL for a TIGERweb layer.

    Parameters
    ----------
    layer_name : str | SumLevel
        Layer name (e.g. ``'tracts'``) or a ``SumLevel`` instance whose
        ``tigerweb_name`` is used automatically.
    year : int, optional
        Vintage year. Required for ``'ACS'`` and ``'DEC'`` surveys.
    survey : {'current', 'ACS', 'DEC'}
        Survey type. Defaults to ``'current'`` (most recent geometries).

    Returns
    -------
    str
        MapServer endpoint URL for the requested layer.

    Examples
    --------
    >>> get_layer_url('tracts', year=2024, survey='ACS')
    'https://tigerweb.geo.census.gov/.../MapServer/8'
    """
    import pandas as pd
    from morpc_census.geos import SumLevel

    if isinstance(layer_name, SumLevel):
        layer_name = layer_name.tigerweb_name

    if survey not in ['ACS', 'DEC', 'current']:
        raise ValueError(f"Invalid survey type {survey!r}. Must be 'current', 'ACS', or 'DEC'.")
    if survey == 'DEC' and year not in [2010, 2020]:
        raise ValueError(f"Invalid year {year} for Decennial Census. Must be 2010 or 2020.")
    if survey == 'ACS' and pd.to_numeric(year) < 2012:
        raise ValueError(f"Invalid year {year} for ACS. Must be 2012 or later.")

    layer_name = layer_name.lower()
    baseurl = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/"

    if survey == 'current':
        if layer_name not in current_endpoints:
            raise ValueError(f"Layer {layer_name!r} not found in current endpoints. Available: {list(current_endpoints)}")
        url = f"{baseurl}tigerWMS_Current/MapServer/{current_endpoints[layer_name]}"
    else:
        survey_slug = 'Census' if survey == 'DEC' else survey
        layers = get_tigerweb_layers_map(year, survey)
        if layer_name not in layers:
            raise ValueError(f"Layer {layer_name!r} not found for {survey} {year}. Available: {list(layers)}")
        url = f"{baseurl}tigerWMS_{survey_slug}{year}/MapServer/{layers[layer_name]}"

    logger.info(f"Layer URL: {url}")
    return url


def resource_from_scope_sumlevel(
    scope: str | Scope,
    sumlevel: str | SumLevel,
    archive: PathLike | None = None,
    max_record_count: int = 20,
):
    """Build a morpc REST API resource for all geographies at *sumlevel* within *scope*.

    Parameters
    ----------
    scope : str | Scope
        Geographic scope (e.g. ``'franklin'`` or a ``Scope`` instance).
    sumlevel : str | SumLevel
        Summary level name or ``SumLevel`` instance (e.g. ``'tract'``).
    archive : path-like, optional
        If provided, the resource is serialised to YAML at this path.
    max_record_count : int
        Maximum records per API page. Defaults to 20.

    Returns
    -------
    morpc.rest_api.resource
        Configured resource ready for fetching.
    """
    from morpc.rest_api import resource
    from morpc_census.geos import Scope, SumLevel

    sc = scope if isinstance(scope, Scope) else Scope(scope)
    sl = sumlevel if isinstance(sumlevel, SumLevel) else SumLevel(sumlevel)

    url = get_layer_url(sl.tigerweb_name)
    where = sc.sql
    outfields = ",".join(['GEOID', 'NAME'] + [f.upper() for f in sl.parts])

    tigerweb_resource = resource(
        name=f"censustigerweb-{sc.name}-{sl.hierarchy_string.lower()}",
        url=url,
        where=where,
        outfields=outfields,
        max_record_count=max_record_count,
    )

    if archive is not None:
        tigerweb_resource.to_yaml(archive)

    return tigerweb_resource


def resource_from_geometry_sumlevel(
    geo,
    scopename: str,
    sumlevel: str | SumLevel,
    archive: PathLike | None = None,
    max_record_count: int = 20,
):
    """Build a morpc REST API resource for all geographies at *sumlevel* intersecting *geo*.

    Parameters
    ----------
    geo : GeoDataFrame | GeoSeries
        Geometry whose bounding box is used as the spatial filter.
    scopename : str
        Label used in the resource name (e.g. ``'franklin'``).
    sumlevel : str | SumLevel
        Summary level name or ``SumLevel`` instance (e.g. ``'tract'``).
    archive : path-like, optional
        If provided, the resource is serialised to YAML at this path.
    max_record_count : int
        Maximum records per API page. Defaults to 20.

    Returns
    -------
    morpc.rest_api.resource
        Configured resource ready for fetching.
    """
    from morpc.rest_api import resource
    from morpc_census.geos import SumLevel

    sl = sumlevel if isinstance(sumlevel, SumLevel) else SumLevel(sumlevel)

    url = get_layer_url(sl.tigerweb_name)
    outfields = ",".join(['GEOID', 'NAME'] + [f.upper() for f in sl.parts])

    params = {
        'geometry': ",".join(str(x) for x in geo.total_bounds),
        'geometryType': 'esriGeometryEnvelope',
        'inSR': geo.crs.to_epsg(),
        'spatialRel': 'esriSpatialRelContains',
        'returnGeometry': 'true',
        'f': 'geojson',
    }
    tigerweb_resource = resource(
        name=f"censustigerweb-{scopename}-{sl.hierarchy_string.lower()}",
        url=url,
        outfields=outfields,
        max_record_count=max_record_count,
        **params,
    )

    if archive is not None:
        tigerweb_resource.to_yaml(archive)

    return tigerweb_resource
