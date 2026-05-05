"""
Import-only smoke tests — verify the package and its submodules load without error.
No network access required.
"""

import morpc_census


def test_package_imports():
    assert morpc_census.__version__ == "0.1.0"


def test_api_module_imports():
    from morpc_census import api
    assert hasattr(api, "CensusAPI")
    assert hasattr(api, "DimensionTable")
    assert hasattr(api, "IMPLEMENTED_ENDPOINTS")


def test_census_module_imports():
    from morpc_census import census
    assert hasattr(census, "acs_label_to_dimensions")
    assert hasattr(census, "acs_generate_universe_table")
    assert hasattr(census, "acs_flatten_category")


def test_tigerweb_module_imports():
    from morpc_census import tigerweb
    assert hasattr(tigerweb, "get_layer_url")
    assert hasattr(tigerweb, "get_tigerweb_layers_map")
