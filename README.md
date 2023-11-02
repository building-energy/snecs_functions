# snecs_functions
Python package for working with the Sub-National Energy Consumption Statistics dataset

## Overview

The Sub-National Energy Consumption Statistics (SNECS) dataset is published by the UK government here: 
- https://www.gov.uk/government/collections/sub-national-electricity-consumption-data
- https://www.gov.uk/government/collections/sub-national-gas-consumption-data

This Python package contains a series of Python functions which can be used to:
- Download the CSV files which make up the SNECS dataset.
- Import the data into a SQLite database.
- Access the data in the database, for data analysis and visualisation.

A description of the CSV files in the SNECS dataset, along with instructions for downloading and importing in SQLite, has been created using the format of a [CSVW metadata Table Group object](https://www.w3.org/TR/2015/REC-tabular-metadata-20151217/#table-groups) (saved as a JSON file), which is available here: https://raw.githubusercontent.com/building-energy/snecs_functions/main/snecs_tables-metadata.json


## Installation

`pip install git+https://github.com/building-energy/snecs_functions`

## Quick Start



## API

### download_and_import_all_data

Description: Downloads all the SNECS data and imports all data into a SQLite database.

The data to be downloaded is described in the CSVW metadata file here: https://raw.githubusercontent.com/building-energy/snecs_functions/main/snecs_tables-metadata.json

This makes use of two functions in the [csvw_functions_extra](https://github.com/stevenkfirth/csvw_functions_extra) package:
- download_table_group
- import_table_group_to_sqlite

These functions can also be called directly, using the CSVW metadata file above, to download and import individual SNECS tables as needed.

Call signature:

```python
download_and_import_all_data(
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False,
        )
```

Arguments:
- **data_folder** *(str)*: The filepath of a local folder where the downloaded CSV data is saved to and the SQLite database is stored.
- **database_name** *(str)*: The name of the SQLite database, relative to the data_folder.
- **verbose (bool)**: If True, then this function prints intermediate variables and other useful information.

Returns: None

### get_government_office_region_elec

```python
get_government_office_region_elec(
        year=None,
        region_code=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        )
```
### get_government_office_region_gas

```python
get_government_office_region_gas(
        year=None,
        region_code=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        )
```

### get_local_authority_elec

```python
get_local_authority_elec(
        year=None,
        la_code=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        )
```

### get_local_authority_gas

```python
get_local_authority_gas(
        year=None,
        la_code=None,
        region=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        )
```

### get_LSOA_elec_domestic

```python
get_LSOA_elec_domestic(
        year=None,
        la_code=None,
        msoa_code=None,
        lsoa_code=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        )
```

### get_LSOA_gas_domestic

```python
get_LSOA_gas_domestic(
        year=None,
        la_code=None,
        msoa_code=None,
        lsoa_code=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        )
```

### get_MSOA_elec_domestic

```python
get_MSOA_elec_domestic(
        year=None,
        la_code=None,
        msoa_code=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        )
```

### get_MSOA_gas_domestic

```python
get_MSOA_gas_domestic(
        year=None,
        la_code=None,
        msoa_code=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        )
```

### get_MSOA_elec_non_domestic

```python
get_MSOA_elec_non_domestic(
        year=None,
        la_code=None,
        msoa_code=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        )
```

### get_MSOA_gas_non_domestic

```python
get_MSOA_gas_non_domestic(
        year=None,
        la_code=None,
        msoa_code=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        )
```

### get_postcode_elec_all_meters

```python
get_postcode_elec_all_meters(
        year,
        postcode=None,
        outcode=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        )
```

### get_postcode_elec_economy_7

```python
get_postcode_elec_economy_7(
        year,
        postcode=None,
        outcode=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        )
```

### get_postcode_elec_standard

```python
get_postcode_elec_standard(
        year,
        postcode=None,
        outcode=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        )
```

### get_postcode_gas

```python
get_postcode_gas(
        year,
        postcode=None,
        outcode=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        )
```

