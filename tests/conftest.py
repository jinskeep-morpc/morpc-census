"""
Shared pytest fixtures and configuration for morpc-census tests.

Markers
-------
network
    Tests marked with ``@pytest.mark.network`` make live HTTP requests.
    They are skipped by default; run with ``pytest -m network`` to include them.
"""

import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "network: mark test as requiring live network access (skipped by default)",
    )


def pytest_collection_modifyitems(config, items):
    skip_network = pytest.mark.skip(reason="network tests skipped by default; use -m network to run")
    for item in items:
        if "network" in item.keywords:
            item.add_marker(skip_network)
