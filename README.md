# morpc-census

## Introduction

`morpc-census` is a Python package maintained by the MORPC data team for working with US Census Bureau data. It provides tools for connecting to the Census API, retrieving survey data, and structuring results as long-format tables with [frictionless](https://github.com/frictionlessdata/frictionless-py) metadata.

This package depends on [morpc-py](https://github.com/morpc/morpc-py) for shared MORPC utilities.

### Modules

- **morpc_census.api** — Connect to the Census API at `https://api.census.gov/data/`, retrieve survey data by group and geography, and structure results as long-format DataFrames with frictionless schema and resource files.
- **morpc_census.geos** — Geography utilities for building Census API queries, translating between Census GEOIDs and MORPC geography definitions, and fetching geographic metadata.
- **morpc_census.census** — Functions for working with ACS variable groups, generating frictionless schemas from Census metadata, and producing dimension tables.
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
from morpc_census.api import CensusAPI

# Fetch ACS 5-year age/sex data for counties in the 15-county region
data = CensusAPI(
    survey_table='acs/acs5',
    year=2023,
    group='B01001',
    scope='region15',
    scale='county',
)

# Long-format DataFrame
print(data.LONG.head())

# Save data + frictionless schema + resource to disk
data.save('./output')
```

## Documentation

See [doc](https://github.com/jinskeep-morpc/morpc-census/tree/main/doc) for notebooks and reference material.
