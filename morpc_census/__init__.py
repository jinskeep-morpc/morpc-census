__version__ = "0.1.0"

# Census API client and data structuring
from .api import (
    CensusAPI,
    DimensionTable,
    get_all_avail_endpoints,
    get_table_groups,
    get_group_variables,
    get_group_universe,
    fetch,
    CENSUS_DATA_BASE_URL,
    IMPLEMENTED_ENDPOINTS,
    HIGHLEVEL_GROUP_DESC,
    MISSING_VALUES,
    VARIABLE_TYPES,
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

# ACS variable and dimension utilities
from .census import (
    acs_label_to_dimensions,
    acs_generate_universe_table,
    acs_flatten_category,
)

# Geography query and translation utilities
from .geos import (
    Scale,
    Scope,
    SCOPES,
    PSEUDOS,
    valid_scale,
    valid_scope,
    geoinfo_from_scope_scale,
    geoinfo_from_params,
    geoids_from_scope,
    pseudos_from_scale_scope,
    geoinfo_for_hierarchical_geos,
    fetch_geos_from_geoids,
    fetch_geos_from_scale_scope,
    morpc_juris_part_to_full,
    census_geoid_to_morpc,
    morpc_geoid_to_census,
    geoidfq_to_columns,
    columns_to_geoidfq,
)

# TIGERweb REST API utilities
from .tigerweb import (
    get_tigerweb_layers_map,
    get_layer_url,
    outfields_from_scale,
    where_from_scope,
    resource_from_scope_scale,
    resource_from_geometry_scale,
)
