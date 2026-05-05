# morpc-census dev notes

## 2026-05-05 16:00 — Add type hints and docstrings to geos.py (closes #31)

Added type annotations and short docstrings to all public functions in `morpc_census/geos.py`:

- Added `from geopandas import GeoDataFrame` to top-level imports for use in return types
- `valid_scale`, `valid_scope` — added `str` param types, `SumLevel` / `bool | None` returns, one-line docstrings
- `get_query_req` — annotated `scale: str`, `year: str`, `-> dict`; added docstring
- `geoinfo_for_hierarchical_geos` — replaced empty multi-line docstring stub with signature `(str, str) -> DataFrame` and one-line docstring
- `geoinfo_from_scope_scale` — added `-> list | DataFrame | dict` return type; tightened existing docstring to standard NumPy style
- `geoids_from_scope` — added `scope: str`, `-> list | DataFrame`; added docstring
- `pseudos_from_scale_scope` — added `(str, str) -> list[str]`; added docstring
- `geoinfo_from_params` — corrected return type from `-> list` to `-> list | DataFrame`; collapsed verbose docstring to one line
- `fetch_geos_from_geoids` — added `geoidfqs: list[str]`, `chunk_size: int`, `-> GeoDataFrame`; collapsed docstring to one line
- `fetch_geos_from_scale_scope` — fully annotated `(str, str | None, int | None, Literal, int) -> GeoDataFrame`; collapsed docstring
- `morpc_juris_part_to_full`, `census_geoid_to_morpc`, `morpc_geoid_to_census` — added param types and `-> DataFrame` returns; preserved existing verbose docstrings
- `geoidfq_to_columns` — added `-> DataFrame | GeoDataFrame`; added docstring
- `columns_to_geoidfq` — added docstring

All 50 existing tests pass.

## 2026-05-05 15:30 — Rewrite 01-morpc-geos-demo.ipynb — add GeoIDFQ, usage-first framing (closes #29)

Rewrote `doc/01-morpc-geos-demo.ipynb` with a usage-first structure. Replaced the previous version (which led with dataclass field tables and internal implementation details) with a workflow-oriented narrative covering four topics:

- **Scopes** — lists `SCOPES.keys()`, looks up individual scopes, notes the 15-county region's multi-county `for_param`
- **Scales** — calls `valid_scale()` on recognized names, shows the returned `SumLevel` fields, demonstrates the `ValueError` for unrecognized names
- **Fetching geometries** — `fetch_geos_from_scale_scope(scope, scale)` with county and tract examples; `.plot()` calls; network note at section header
- **GEOIDFQs** — `GeoIDFQ.parse()`, `.parts`, `.geoid`, `GeoIDFQ.build()`, `str()`, and a worked example parsing the `GEO_ID` column from a prior fetch

Also added a `test_geoidfq_class.py` expansion (closes #27) covering sumlevels 100, 140, 150 in the same PR #28 cycle (previously noted at 2026-05-05 11:02).

## 2026-05-05 11:02 — Add GeoIDFQ class, refactor geoidfq_to_columns / columns_to_geoidfq (closes #25)

Added `GeoIDFQ` dataclass to `morpc_census/geos.py` to encapsulate GEOIDFQ parsing and construction:

- `GeoIDFQ.parse(geoidfq_str)` — slices a GEOIDFQ string into `sumlevel`, `variant`, `geocomp`, and `parts` using field widths from `SUMLEVEL_DESCRIPTIONS[sumlevel]["geoidfq_format"]`
- `GeoIDFQ.build(sumlevel, parts, variant="00", geocomp="00")` — constructs from components; raises `ValueError` for MORPC sumlevels (no `geoidfq_format`) or mismatched parts keys
- `__str__()` — reconstructs the full GEOIDFQ string
- `geoid` property — short-form ID after `"US"` (used in REST API queries)
- Variant codes documented in class docstring per Census geo-variant system (`"00"` default; `"01"`–`"59"` CDs; `"Ux"`/`"Lx"` SLDs; `"Mx"` CBSAs; `"Cx"` UAs; `"Px"` PUMAs; `"Zx"` ZCTAs)

Refactored `geoidfq_to_columns` to use `GeoIDFQ.parse()` instead of inline regex + slicing. Fixed `columns_to_geoidfq` — it referenced `SUMLEVEL_DESCRIPTIONS[sumlevel]['current_variant']` (a key that does not exist), causing a `KeyError` at runtime; replaced with `GeoIDFQ.build()` and an explicit `variant` parameter (default `"00"`).

34 new tests in `tests/test_geoidfq_class.py` covering sumlevels 040, 050, 100, 140, 150, 160, 310, and 500; all 54 tests pass.

## 2026-05-05 10:39 — Rename Scale class to SumLevel (closes #23)

Renamed the `Scale` dataclass to `SumLevel` everywhere it appeared:
- Class definition and `-> SumLevel` return type in `morpc_census/geos.py`
- Export in `morpc_census/__init__.py`
- `TestScale` → `TestSumLevel` and all constructor calls in `tests/test_geos_classes.py`
- Import and cell source in `doc/01-morpc-geos-demo.ipynb`

## 2026-05-05 10:15 — Rewrite 01-morpc-geos-demo.ipynb for Scale and Scope (closes #18)

Deleted the old notebook (which documented morpc-py's `load_spatial_data` / `assign_geo_identifiers` — unrelated to morpc-census) and replaced it with a focused demo of the new `Scope` and `Scale` classes. The new notebook covers:
- Constructing a `Scope` directly and reading `.params`
- Browsing the built-in `SCOPES` dict
- Constructing a `Scale` directly and using `valid_scale()`
- Using `fetch_geos_from_scale_scope()` with scope and scale name strings

Network-dependent cells are marked with a note. No new unit tests added — the classes are tested in `tests/test_geos_classes.py`.

## 2026-05-05 10:09 — Add Scale and Scope dataclasses to geos module (closes #16)

Replaced the plain `dict[str, dict]` pattern in `SCOPES` with two dataclasses:

- **`Scope(name, for_param, in_param=None)`** — represents a named Census API geography scope. The `.params` property returns the `{"for": ..., "in": ...}` dict consumed by Census API calls.
- **`Scale(name, sumlevel)`** — frozen dataclass pairing a Census query name (e.g. `"county"`) with its summary level code (e.g. `"050"`).

`STATE_SCOPES`, `COUNTY_SCOPES`, and `MORPC_REGION_SCOPES` now produce `list[Scope]` instead of lists of dicts. `SCOPES` is now typed `dict[str, Scope]`. All internal call sites that accessed `SCOPES[scope]` as a raw dict were updated to use `.params`.

`valid_scale()` now returns a `Scale` object instead of `True`, giving callers the resolved sumlevel without a second lookup into `morpc.SUMLEVEL_DESCRIPTIONS`.

Both classes exported from `morpc_census/__init__.py`. 16 new tests in `tests/test_geos_classes.py`; all pass.

## 2026-05-05 09:41 — dev_notes.md: add times to headers, reorder descending (closes #14)

Added times to all section headers (format `YYYY-MM-DD HH:MM — Title`).
Reordered sections descending so the most recent entry is always at the top.
Times for historical entries back-filled from git commit timestamps.

## 2026-05-05 08:54 — Rewrite README.md and doc/index.md

Both files were copied from morpc-py and described the full morpc-py package.
Rewrote to describe morpc-census: its purpose (Census API access, long-format
tables, frictionless metadata), its four modules (api, geos, census, tigerweb),
installation instructions using the correct package name and import path
(`morpc_census`), and links to the remaining notebooks.

## 2026-05-05 08:49 — Remove non-census notebooks from doc/

Deleted notebooks and log files from `doc/` that covered morpc-py features
unrelated to census (countylookup, varlookup, REST API, frictionless, plot,
color, and the general morpc-py demo log). Kept:
- `05-morpc-geos-demo.ipynb` — geos is part of morpc-census
- `07-morpc-census-demo.ipynb` and its rendered HTML

## 2026-05-04 18:00 — Split from morpc-py, refactor api module

### Context
morpc-census was a direct fork of morpc-py, meaning both repos contained identical
files. The goal is for morpc-census to be an independent package focused solely on
Census data tools. morpc-py remains the general-purpose MORPC utility library.
morpc-census may depend on morpc-py; morpc-py must not depend on morpc-census.

### Package split (step 1): remove morpc-py-specific files
Deleted from morpc-census everything that belongs to morpc-py and has nothing to
do with Census data:
- `morpc/morpc.py` — MORPC-specific constants (county IDs, region maps, etc.)
- `morpc/logs.py` — logging configuration
- `morpc/geocode.py` — Nominatim geocoding
- `morpc/color/` — MORPC branding colors
- `morpc/plot/` — MORPC plotting utilities
- `tests/test_utils.py` — tests for morpc-py utility functions

Updated `morpc/__init__.py` to remove imports of the deleted modules.
Updated `pyproject.toml`: removed `IPython`, `xlsxwriter`, `plotnine` dependencies
(only needed by the deleted modules).

### Package split (step 2): separate namespaces, remove all shared files
Moved census module to a dedicated `morpc_census/` top-level package so that
morpc-census and morpc-py no longer share the `morpc` namespace.

- Created `morpc_census/` with `api.py`, `census.py`, `geos.py`, `tigerweb.py`,
  `__init__.py`, and JSON data files moved from `morpc/census/`.
- Updated all internal cross-imports: `from morpc.census.X` → `from morpc_census.X`.
- Deleted the entire `morpc/` directory from morpc-census, including the shared
  utilities `req.py`, `utils.py`, `frictionless/`, `rest_api/` — these come from
  morpc-py at runtime via the declared dependency.
- Updated `app/app.py`: `from morpc.census import api` → `from morpc_census import api`.
- Updated `pyproject.toml`:
  - Package name: `morpc-census`
  - Added `morpc` (morpc-py) as a dependency
  - Version now sourced from `morpc_census.__version__`
  - `morpc_census.__init__` sets `__version__ = "0.1.0"`

External imports that reference `morpc.*` (constants, `morpc.req`, `morpc.frictionless`,
`morpc.rest_api`) are intentionally left as-is — they resolve through the installed
morpc-py package.

### api.py refactor
Rewrote `morpc_census/api.py` to fix correctness issues and clean up structure.

**Bugs fixed:**
- `ALL_AVAIL_ENDPOINTS` was referenced in `valid_vintage()` but never defined
  (the code that built it was commented out because it made a network call at
  import time). Replaced with `get_all_avail_endpoints()`, a lazy function that
  fetches once and caches the result.
- `from morpc.frictionless import ...` in `save()` and `create_resource()` replaced
  with direct frictionless calls — morpc-census no longer bundles those wrappers.

**Structural changes:**
- Removed unused imports: `from enum import unique`, `from types import NoneType`,
  `from numpy import var`.
- Consolidated imports: `json`, `re`, `os`, `numpy`, `pandas`, `StringIO` moved to
  top-level.
- `get()` renamed to `fetch()` to avoid shadowing the Python built-in.
- `CensusAPI._fetch_metadata()` extracted to group the three API calls that retrieve
  `CONCEPT`, `UNIVERSE`, and `VARS`.
- `CensusAPI.melt()` simplified: cleaner `id_vars` logic, more defensive regex.
- `CensusAPI.define_schema()` uses a `_VALUE_FIELD_DEFS` dict instead of a chain
  of `if column ==` blocks.
- `CensusAPI.save()` creates output directory automatically; uses `pathlib.Path`.
- `CensusAPI.create_resource()` builds a frictionless `Resource` descriptor directly.
- `DimensionTable`: fixed `!= None` → `is not None`; removed `wrapping_func` (text
  wrapping belongs in a presentation layer, not a data class);
  `create_description_table()` rewritten to avoid integer-index fragility.
- Added `_VALUE_FIELD_DEFS` module-level dict for schema field definitions.
