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


def test_tigerweb_module_imports():
    from morpc_census import tigerweb
    assert hasattr(tigerweb, "get_layer_url")
    assert hasattr(tigerweb, "get_tigerweb_layers_map")


def test_geos_module_imports():
    from morpc_census import geos
    assert hasattr(geos, "SCOPES")
    assert hasattr(geos, "PSEUDOS")
    assert hasattr(geos, "GeoIDFQ")
    assert hasattr(geos, "SumLevel")
    assert hasattr(geos, "Scope")


def test_scopes_is_not_accessed_at_import():
    """SCOPES dict must not be populated until first access — no network at import time."""
    from morpc_census.geos import SCOPES
    # _loaded should still be False: import alone must not trigger the morpc network call
    assert not SCOPES._loaded
