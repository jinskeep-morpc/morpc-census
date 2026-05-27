# API Reference

Key classes and functions exported from `morpc_census`.

---

## Census API — `morpc_census.api`

### `Endpoint`

```python
Endpoint(dataset: str, year: int)
```

Represents a Census API survey and vintage year. Lazy-loads available groups and variables on first access.

- `Endpoint('acs/acs5', 2023)` — ACS 5-year estimates, 2023 vintage
- `.groups` — `dict` of all variable groups for this endpoint (cached)
- `.vintages` — list of available vintage years (cached)

---

### `Group`

```python
Group(endpoint: Endpoint, code: str)
```

Wraps a Census variable group (e.g. `'B01001'`).

- `.variables` — `dict` of all variables in the group (cached)

---

### `CensusAPI`

```python
CensusAPI(endpoint, scope, group=None, sumlevel=None, variables=None)
```

Fetches survey data and reshapes it to a long-format DataFrame.

| Attribute | Description |
|-----------|-------------|
| `.data`   | Raw wide DataFrame from the Census API |
| `.long`   | Long-format DataFrame: one row per geography × variable |
| `.vars`   | Per-variable metadata dict |

```python
ep  = Endpoint('acs/acs5', 2023)
grp = Group(ep, 'B01001')
api = CensusAPI(ep, SCOPES['region15'], group=grp, sumlevel=SumLevel('county'))
api.long.head()
```

Save long data + frictionless schema to disk:

```python
api.save('./output')
```

---

### `DimensionTable`

```python
DimensionTable(long: DataFrame)
```

Reshapes a `CensusAPI.long` DataFrame into a MultiIndex wide table.

| Method | Description |
|--------|-------------|
| `.wide()` | Pivot to wide MultiIndex DataFrame |
| `.percent(_wide=None)` | Column percentages; MOE via Census Bureau derived proportion formula |
| `.remap(label_map)` | Collapse variable labels and aggregate estimates |
| `.drop(dim)` | Remove a dimension level by name, integer index, or list |

Column MultiIndex level order: `concept > universe > survey > geoidfq > name > reference_period > value_type`

---

### `RaceDimensionTable`

```python
RaceDimensionTable(long: DataFrame, race_map=None)
```

Subclass of `DimensionTable` for racial-iteration groups (e.g. B17020A–I). Adds an ordered `race` column level; `percent()` computes within-race percentages.

---

### `TimeSeries`

```python
TimeSeries(survey, years, scope, group=None, sumlevel=None, variables=None)
```

Fetches the same Census group across multiple vintage years and concatenates the results into a single long-format DataFrame. Each year is a separate `CensusAPI` call.

| Attribute | Description |
|-----------|-------------|
| `.calls`  | `dict[int, CensusAPI]` — one entry per year |
| `.long`   | Concatenated long-format DataFrame; `reference_period` distinguishes years |
| `.years`  | Sorted list of vintage years |

```python
ts = TimeSeries('acs/acs5', [2019, 2021, 2023], SCOPES['region15'], group='B01001', sumlevel='county')
ts.long.head()
ts.dimension_table().wide()
ts.save('./output')
```

The `.name` property encodes the full year range: `census-acs-acs5-2019-2023-county-region15-b01001`.

`save(output_path)` writes three files: `{name}.long.csv`, `{name}.schema.yaml` (frictionless Schema; primary key = `['geoidfq', 'reference_period', 'variable']`), and `{name}.resource.yaml` (frictionless Resource; sources list one entry per year).

`.dimension_table(**kwargs)` → `DimensionTable(self.long, **kwargs)`

---

### `RaceTable`

```python
RaceTable(endpoint, scope, group, sumlevel=None, race_codes=None)
```

Fetches racial iteration groups (e.g. `B17020A`–`B17020I`) for a base group code and concatenates them. Automatically discovers which race letter suffixes exist for the given endpoint and skips missing ones with a warning. Raises `ValueError` if no valid codes are found.

| Attribute | Description |
|-----------|-------------|
| `.calls`      | `dict[str, CensusAPI]` — one entry per race letter |
| `.long`       | Concatenated long-format DataFrame (race encoded in variable codes, e.g. `B17020A_001`) |
| `.base_code`  | Uppercase base group code without race letter (e.g. `'B17020'`) |

```python
ep  = Endpoint('acs/acs5', 2023)
rt  = RaceTable(ep, SCOPES['region15'], 'B17020', sumlevel='county')
rt.long.head()
rt.dimension_table().wide()   # returns RaceDimensionTable
rt.save('./output')
```

The `.name` property appends `-race`: `census-acs-acs5-2023-county-region15-b17020-race`.

`save(output_path)` writes three files (same structure as `CensusAPI.save`). The resource `sources` list includes one entry per race code with the human-readable race label, e.g. `'US Census Bureau API (B17020A: White Alone)'`.

`.dimension_table(**kwargs)` → `RaceDimensionTable(self.long, **kwargs)`

---

## Geography — `morpc_census.geos`

### `SCOPES`

Dict of named geographic extents. Common keys: `'region15'`, `'franklin'`, `'us'`.

```python
from morpc_census import SCOPES
list(SCOPES.keys())
```

---

### `SumLevel`

```python
SumLevel(name_or_code: str)
```

Maps between summary level names and Census API codes.

```python
SumLevel('county')   # → code '050'
SumLevel('050')      # → name 'county'
```

---

### `GeoIDFQ`

Parse and construct Census fully-qualified geographic IDs.

```python
GeoIDFQ.parse('1400000US39049010100')
# → GeoIDFQ(sumlevel='140', variant='00', parts={'state': '39', 'county': '049', 'tract': '010100'})

GeoIDFQ.build('050', {'state': '39', 'county': '049'})
# → GeoIDFQ(...)
str(GeoIDFQ.build('050', {'state': '39', 'county': '049'}))
# → '0500000US39049'
```

---

### `geoinfo_from_scope_sumlevel`

```python
geoinfo_from_scope_sumlevel(scope, sumlevel=None, output='list')
```

Returns GEOIDFQs for all geographies at `sumlevel` within `scope`.

```python
geoinfo_from_scope_sumlevel('region15')                        # list of GEOIDFQ strings
geoinfo_from_scope_sumlevel('franklin', 'tract', output='table')  # DataFrame
```

---

### `fetch_geos_from_scope_sumlevel`

```python
fetch_geos_from_scope_sumlevel(scope, sumlevel=None, year=None, survey='current')
```

Fetches a GeoDataFrame of boundaries from TIGERweb.

```python
geos   = fetch_geos_from_scope_sumlevel('region15')           # county boundaries
tracts = fetch_geos_from_scope_sumlevel('franklin', 'tract')  # tracts in Franklin County
geos.plot()
```
