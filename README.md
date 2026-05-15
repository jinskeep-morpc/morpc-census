# morpc-census

## Introduction

`morpc-census` is a Python package maintained by the MORPC data team for working with US Census Bureau data. It provides tools for connecting to the Census API, retrieving survey data, and structuring results as long-format tables with [frictionless](https://github.com/frictionlessdata/frictionless-py) metadata.

This package depends on [morpc-py](https://github.com/morpc/morpc-py) for shared MORPC utilities.

### Modules

- **morpc_census.api** — Connect to the Census API at `https://api.census.gov/data/`, retrieve survey data by group and geography, and structure results as long-format DataFrames with frictionless schema and resource files.
- **morpc_census.geos** — Geography utilities for building Census API queries, translating between Census GEOIDs and MORPC geography definitions, and fetching geographic metadata.
- **morpc_census.tigerweb** — Tools for interacting with the Census TIGERweb REST API to fetch geographic boundary data.

## Installation

```bash
pip install morpc-census
```

### Dev Install

To install an editable version for development:

```bash
git clone https://github.com/jinskeep-morpc/morpc-census.git
pip install -e /path/to/morpc-census/
```

Then import as:

```python
import morpc_census
```

## Usage

```python
from morpc_census import Endpoint, Group, CensusAPI, DimensionTable, SCOPES, SumLevel

ep  = Endpoint('acs/acs5', 2023)
grp = Group(ep, 'B01001')

# Fetch ACS 5-year age/sex data for counties in the 15-county region
api = CensusAPI(ep, SCOPES['region15'], group=grp, sumlevel=SumLevel('county'))

# Long-format DataFrame
print(api.long.head())

# Reshape into wide MultiIndex table and compute percentages
table = DimensionTable(api.long)
wide  = table.wide()
pct   = table.percent()

# Save data + frictionless schema + resource to disk
api.save('./output')
```

## Demos and Documentation

See [demos](https://jinskeep-morpc.github.io/morpc-census/) for examples and documentation.

---

## Roadmap — Code Improvements

The items below are targeted simplifications and correctness fixes to make the package cleaner before a stable release. They are ordered roughly by priority.

### 1. Fix Python 3.10/3.11 syntax bug in `geos.py`

`Scope.sql` uses same-quote nested f-strings (`f"...{",".join(...)}..."`) which only parses on Python 3.12+. The package declares `requires-python = ">=3.10"`, so this silently breaks on 3.10 and 3.11. Replace with a local variable for the join result before embedding it in the outer f-string.

### 2. Cache `_get_api_key()`

`_get_api_key()` is called before every API request. Each call invokes `find_dotenv()`, which walks the filesystem looking for a `.env` file. Decorate with `@functools.lru_cache(maxsize=None)` (or `@functools.cache`) so the filesystem walk happens at most once per process.

### 3. Replace the global `_avail_endpoints_cache` with `@functools.cache`

`get_all_avail_endpoints()` stores its result in a module-level mutable global. Replacing it with `@functools.cache` on the function eliminates the global and makes the caching intent explicit.

### 4. Avoid double-computing `wide()` inside `percent()`

`DimensionTable.percent()` calls `self.wide()` internally. If a caller wants both, the full pivot runs twice. Accept an optional `_wide` parameter in `percent()`, or cache `wide()` with `@functools.cache` on the result.

### 5. Rename `DimensionTable.variable_type` → `value_cols`

`variable_type` is a list of column names (e.g. `['estimate', 'moe']`), not a single type string. The name is misleading. Renaming to `value_cols` (or `value_columns`) makes the intent clear.

### 6. Rename `map` parameter in `find_replace_variable_map` → `label_map`

The parameter named `map` shadows the Python builtin. Rename to `label_map`.

### 7. Split `CensusAPI.melt()` into focused private helpers

At ~90 lines and 7 sequential steps, `melt()` is the most complex method in the package. Splitting it into helpers like `_drop_non_data_cols`, `_parse_variable_codes`, `_attach_metadata`, and `_pivot_value_types` would make each step independently testable and easier to read.

### 8. Pin `numpy` as an explicit dependency

`api.py` imports `numpy` directly but `numpy` is not listed in `pyproject.toml`. It arrives transitively through `pandas`/`geopandas`, but best practice is to declare direct dependencies explicitly with a minimum version.

### 9. Add minimum version pins to all dependencies

`pyproject.toml` lists dependencies without version constraints. Add minimum versions (e.g. `pandas>=2.0`, `geopandas>=0.14`, `frictionless>=5.0`) so that broken environments fail fast with a clear error instead of silently misbehaving.

### 10. Add module docstrings to `geos.py` and `tigerweb.py`

`api.py` has a module-level docstring explaining its purpose and the base URL it targets. `geos.py` and `tigerweb.py` have none. Adding short docstrings improves discoverability and consistency.

### 11. Validate or auto-fetch `tigerweb.py` `current_endpoints`

The `current_endpoints` dict in `tigerweb.py` is hand-maintained. TIGERweb layer IDs can change when new vintages are released. Either add a test that fetches the live map and compares it to the hardcoded values, or replace the hardcoded dict with a cached fetch from the `current` MapServer endpoint.

---

## Roadmap — Production Readiness & PyPI Release

Steps to go from the current development state to a stable, installable PyPI package.

### Phase 1 — Pre-release cleanup

- [ ] Apply all code improvement items above.
- [ ] Update `pyproject.toml` classifier from `Development Status :: 1 - Planning` to `4 - Beta` (or `5 - Production/Stable` when ready).
- [ ] Fix the README usage example (currently uses old parameter names `survey_table=`, `scale=`, `data.LONG`). *(Addressed in this README.)*
- [ ] Add a `CHANGELOG.md` file. Start with a `0.1.0` entry summarizing the current feature set.
- [ ] Add a `py.typed` marker file to `morpc_census/` so downstream type checkers know the package ships type information.
- [ ] Expand test coverage for offline paths: `DimensionTable.wide()`, `percent()`, `remap()`, `drop()`, and `melt()` can all run without network access using fixture DataFrames.

### Phase 2 — Dependency audit

- [ ] Assess whether `morpc` (the internal MORPC utility package) can be published to PyPI or replaced with the specific pieces it provides. As long as `morpc` is not on PyPI, `morpc-census` cannot be `pip install`ed from PyPI by external users without manual steps.
- [ ] If `morpc` is kept as a private dependency, document the installation order clearly and consider whether to publish `morpc-census` to a private registry (e.g. a GitHub Packages index or a self-hosted PyPI).
- [ ] Pin `morpc` to a minimum version in `pyproject.toml` once its versioning stabilizes.

### Phase 3 — CI/CD

- [ ] Add a GitHub Actions workflow (`.github/workflows/ci.yml`) that runs `pytest -m "not network"` on push and pull request for Python 3.10, 3.11, and 3.12.
- [ ] Add a workflow step that runs `python -m build` and `twine check dist/*` to verify the package builds cleanly on every push.
- [ ] Add a publish workflow triggered on GitHub release tags that builds and uploads to PyPI (or a private index) using a stored API token secret.

### Phase 4 — Versioning & release

- [ ] Switch from the hardcoded `__version__ = "0.1.0"` in `__init__.py` to dynamic versioning via `setuptools-scm` (already declared in `pyproject.toml` build requirements). This reads the version from git tags automatically.
- [ ] Tag `v0.1.0` in git once Phase 1–3 are complete and publish the first release.
- [ ] Decide on a versioning policy (SemVer recommended: `MAJOR.MINOR.PATCH`) and document breaking-change rules in `CONTRIBUTING.md`.

### Phase 5 — Documentation

- [ ] Auto-generate API reference docs from docstrings using `sphinx` + `autodoc` or `mkdocs` + `mkdocstrings`, and host on GitHub Pages alongside the existing demo notebooks.
- [ ] Add a `CONTRIBUTING.md` with setup instructions, the test command, and the PR process.
- [ ] Add usage examples to docstrings for the most commonly called functions (`CensusAPI`, `DimensionTable.wide`, `geoinfo_from_scope_sumlevel`, `fetch_geos_from_scope_sumlevel`).

---

This product uses the Census Bureau Data API but is not endorsed or certified by the Census Bureau.
