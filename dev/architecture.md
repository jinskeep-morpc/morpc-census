# morpc-census — Architecture Overview

## Package Layout

```
morpc_census/
├── __init__.py      # Public API re-exports
├── api.py           # Census API client + data transformation
├── geos.py          # Geography query construction + GEOID parsing
├── constants.py     # Domain lookup tables (pure data, no I/O)
└── tigerweb.py      # TIGERweb geometry fetching
```

---

## Module Dependency Graph

```mermaid
graph TD
    subgraph morpc_census
        INIT["__init__.py<br/>(public re-exports)"]
        API["api.py"]
        GEOS["geos.py"]
        CONST["constants.py"]
        TIGER["tigerweb.py"]
    end

    subgraph External
        MORPC["morpc<br/>(shared MORPC utilities)"]
        REQUESTS["requests"]
        PANDAS["pandas / geopandas"]
        FRICTIONLESS["frictionless"]
        DOTENV["python-dotenv"]
    end

    INIT --> API
    INIT --> GEOS
    INIT --> CONST
    INIT --> TIGER

    API --> GEOS
    API --> CONST
    API --> MORPC
    API --> REQUESTS
    API --> PANDAS
    API --> FRICTIONLESS
    API --> DOTENV

    GEOS --> MORPC
    GEOS --> REQUESTS
    GEOS --> PANDAS

    TIGER --> GEOS
    TIGER --> MORPC
```

---

## Core Classes

```mermaid
classDiagram
    class Endpoint {
        +str survey
        +int year
        +str url
        +list vintages
        +dict groups
        +search_groups(search) dict
    }

    class Group {
        +Endpoint endpoint
        +str code
        +str description
        +str universe
        +dict variables
    }

    class CensusAPI {
        +Endpoint endpoint
        +Scope scope
        +SumLevel sumlevel
        +Group group
        +list variables
        +DataFrame data
        +DataFrame long
        +dict request
        +melt() DataFrame
        +define_schema() Schema
        +create_resource() Resource
        +save(output_path)
    }

    class DimensionTable {
        +DataFrame long
        +DataFrame dims
        +str variable_type
        +remap(variable_map) DimensionTable
        +drop(dim, method) DimensionTable
        +wide() DataFrame
        +percent(decimals) DataFrame
    }

    class RaceDimensionTable {
        +Series race
    }

    class Scope {
        +str name
        +str for_param
        +str in_param
        +dict params
        +str sql
    }

    class SumLevel {
        +str name
        +str sumlevel
        +list parts
        +get_query_req(year) dict
    }

    class GeoIDFQ {
        +str sumlevel
        +str variant
        +str geocomp
        +dict parts
        +str geoid
        +parse(geoidfq)$ GeoIDFQ
        +build(sumlevel, **kwargs)$ GeoIDFQ
    }

    Endpoint "1" --> "1..*" Group : has groups
    Group "1" --> "1" Endpoint : belongs to
    CensusAPI "1" --> "1" Endpoint : uses
    CensusAPI "1" --> "1" Scope : scoped by
    CensusAPI "1" --> "0..1" SumLevel : at resolution
    CensusAPI "1" --> "0..1" Group : fetches group
    CensusAPI ..> GeoIDFQ : parses GEO_ID column
    DimensionTable "1" --> "1" CensusAPI : consumes .long
    RaceDimensionTable --|> DimensionTable : extends
```

---

## Census API Call Flow

All network calls go to one base URL: **`https://api.census.gov/data`**

```mermaid
sequenceDiagram
    participant User
    participant Endpoint
    participant Group
    participant CensusAPI
    participant CensusAPIServer as api.census.gov

    User->>Endpoint: Endpoint('acs/acs5', 2023)
    Endpoint->>CensusAPIServer: GET /data/ (list available datasets)
    CensusAPIServer-->>Endpoint: vintages and surveys

    User->>Group: Group(endpoint, 'B01001')
    Group->>CensusAPIServer: GET /data/2023/acs/acs5/groups/B01001.json
    CensusAPIServer-->>Group: variable codes, labels, universe

    User->>CensusAPI: CensusAPI(endpoint, scope, group)
    CensusAPI->>CensusAPIServer: GET /data/2023/acs/acs5?get=group(B01001)&for=county:*&in=state:39
    CensusAPIServer-->>CensusAPI: raw CSV (GEO_ID, NAME, B01001_001E, ...)
    CensusAPI->>CensusAPI: melt() → long-format DataFrame
```

### Census API Query Parameters

| Parameter | Purpose | Example |
|-----------|---------|---------|
| `get` | Variable group or list | `group(B01001)` or `B01001_001E,B01001_002E` |
| `for` | Geographic unit | `county:049`, `tract:*`, `us:1` |
| `in` | Parent geography filter | `state:39` |
| `ucgid` | Hierarchical pseudo-query | `pseudo(050000US39049$0500000)` |
| `key` | Optional API key | from `CENSUS_API_KEY` env var |

> **Variable suffix codes:** `E` = estimate · `M` = margin of error · `PE` = percent estimate · `PM` = percent MOE · `N` = total

---

## Geography API Call Flow

```mermaid
sequenceDiagram
    participant User
    participant geoinfo_from_scope_sumlevel
    participant GeoinfoCensus as api.census.gov/data/geoinfo
    participant TIGERweb as tigerweb.geo.census.gov

    User->>geoinfo_from_scope_sumlevel: ('region15', 'tract')
    geoinfo_from_scope_sumlevel->>GeoinfoCensus: GET /data/{year}/geoinfo?get=GEO_ID,NAME&for=tract:*&ucgid=pseudo(...)
    GeoinfoCensus-->>geoinfo_from_scope_sumlevel: list of GEOIDFQs

    User->>fetch_geos_from_scope_sumlevel: ('region15', 'tract')
    fetch_geos_from_scope_sumlevel->>geoinfo_from_scope_sumlevel: resolve GEOIDFQs
    fetch_geos_from_scope_sumlevel->>TIGERweb: GET /arcgis/rest/services/.../MapServer/{layer}/query?where=GEOID IN (...)
    TIGERweb-->>fetch_geos_from_scope_sumlevel: GeoJSON features (geometry)
    fetch_geos_from_scope_sumlevel-->>User: GeoDataFrame
```

---

## Data Transformation Pipeline

```mermaid
flowchart LR
    A["Census API\n(wide CSV)"] -->|CensusAPI._fetch| B["Wide DataFrame\n(cols: GEO_ID, NAME,\nB01001_001E, ...)"]
    B -->|CensusAPI.melt| C["Long DataFrame\n(cols: geoidfq, variable,\nvariable_label, value, ...)"]
    C -->|DimensionTable| D["dims DataFrame\n(indexed by variable,\ncols: dim1, dim2, ...)"]
    D -->|.wide| E["Wide MultiIndex\n(dim1, dim2, geoidfq,\nvalue_type)"]
    D -->|.percent| F["Percent Table\n(MOE via Census\nbureau formula)"]
    D -->|.drop + .remap| D
    C -->|save| G["CSV + frictionless\nschema + resource YAML"]
```

---

## Implemented Survey Endpoints

| Survey | Type | Notes |
|--------|------|-------|
| `acs/acs1` | American Community Survey 1-year | Annual estimates |
| `acs/acs1/profile` | ACS 1-year profile tables | DP-prefixed groups |
| `acs/acs1/subject` | ACS 1-year subject tables | S-prefixed groups |
| `acs/acs5` | American Community Survey 5-year | Most commonly used |
| `acs/acs5/profile` | ACS 5-year profile tables | |
| `acs/acs5/subject` | ACS 5-year subject tables | |
| `dec/pl` | Decennial — Public Law redistricting | 2010, 2020 |
| `dec/dhc` | Decennial — Demographic & Housing Chars | 2020 |
| `dec/ddhca` / `dec/ddhcb` | Detailed DHC variants | 2020 |
| `dec/sf1` / `sf2` / `sf3` | Decennial Summary Files | Historical |
| `geoinfo` | Geographic metadata | Used internally |

---

## Key Design Decisions

**Lazy network access** — `import morpc_census` makes no network calls. `SCOPES`, `Endpoint.groups`, and `Group.variables` are all cached on first access.

**Long-format data model** — Census data arrives wide (variables as columns). `CensusAPI.melt()` immediately converts to long format (one row per geography × variable), which makes multi-group concatenation and `DimensionTable` operations straightforward.

**Dimension parsing** — Census variable labels use `!!`-delimited hierarchies (e.g., `Total:!!Male:!!Under 5 years`). `DimensionTable._parse_dims()` splits these into named columns, enabling `drop()` and `remap()` operations with correct MOE propagation (`sqrt(sum(moe²))`).

**Batched variable fetches** — When fetching individual variables (not a full group), the Census API caps at 50 fields per request. `CensusAPI._fetch_variables()` batches in chunks of 48, then concatenates.

**Pseudo-geography queries** — Multi-county regions use the Census API's `ucgid=pseudo(parent$child)` predicate to avoid fetching all state geographies and filtering. Falls back to hierarchical for/in queries when `pseudo()` is unsupported.

**Frictionless metadata** — `CensusAPI.save()` writes three files: `.csv` (data), `schema.yaml` (field types and labels), and `resource.yaml` (title, sources, schema pointer).

---

## Environment Configuration

```
CENSUS_API_KEY    # Optional; raises rate limits on api.census.gov
                  # Loaded from shell environment or .env file
```

---

## Quick Reference: Entry Points

```python
from morpc_census import (
    # Discovery
    Endpoint, Group,

    # Data fetching
    CensusAPI,

    # Data transformation
    DimensionTable, RaceDimensionTable,

    # Geography
    Scope, SCOPES, SumLevel, GeoIDFQ,
    geoinfo_from_scope_sumlevel,
    fetch_geos_from_scope_sumlevel,

    # Domain constants
    AGEGROUP_MAP, RACE_TABLE_MAP, EDUCATION_ATTAIN_MAP,
    INCOME_TO_POVERTY_MAP,
)
```

```python
# Minimal fetch: ACS 5-year poverty data for MORPC 15-county region
ep  = Endpoint('acs/acs5', 2023)
grp = Group(ep, 'B17001')
api = CensusAPI(ep, SCOPES['region15'], group=grp, sumlevel=SumLevel('county'))

table = DimensionTable(api.long)
wide  = table.wide()      # MultiIndex DataFrame
pct   = table.percent()   # Percentage table with MOE
```
