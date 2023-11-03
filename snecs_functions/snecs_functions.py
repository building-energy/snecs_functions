# -*- coding: utf-8 -*-
"""
Created on Wed Aug  9 13:16:59 2023

@author: cvskf
"""

import urllib
import urllib.request
import json
import os
import sqlite3
import csvw_functions_extra


#_default_data_folder='_data'  # the default
#_default_database_name='snecs_data.sqlite'

urllib.request.urlcleanup()

metadata_document_location=r'https://raw.githubusercontent.com/building-energy/snecs_functions/main/snecs_tables-metadata.json' 



#%% data folder

def download_and_import_all_data(
        csv_file_names = None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False,
        ):
    ""
    
    # download all tables to data_folder
    fp_metadata=\
        csvw_functions_extra.download_table_group(
            metadata_document_location = metadata_document_location,
            csv_file_names = csv_file_names,
            data_folder = data_folder,
            overwrite_existing_files = True,
            verbose = verbose
            )

    #return
        
    # import all tables to sqlite
    csvw_functions_extra.import_table_group_to_sqlite(
        metadata_document_location=fp_metadata,
        csv_file_names = csv_file_names,
        data_folder=data_folder,
        database_name=database_name,
        overwrite_existing_tables=True,
        verbose=verbose
        )


def get_available_csv_file_names(
        ):
    """Returns the CSV file names of all tables in the (remote) CSVW metadata file.
    
    """
    result = \
        csvw_functions_extra.get_available_csv_file_names(
            metadata_document_location = metadata_document_location
            )
    
    return result
    

def get_table_names_in_database(
        data_folder = '_data',
        database_name = 'snecs_data.sqlite',
        ):
    """
    """
    fp_database=os.path.join(data_folder,database_name)
    
    metadata_table_group_dict = \
        _get_local_metadata_table_group_dict(
                data_folder,
                )
        
    sql_table_names = \
        [metadata_table_dict['https://purl.org/berg/csvw_functions_extra/vocab/sql_table_name']['@value']
         for metadata_table_dict
         in metadata_table_group_dict['tables']]
    
    with sqlite3.connect(fp_database) as conn:
        c = conn.cursor()
        result=[x[0] for x in c.execute('SELECT name FROM sqlite_master WHERE type="table"').fetchall()]
    
    result=[x for x in result if x in sql_table_names]
    
    return result
    
    


def _get_local_metadata_table_group_dict(
        data_folder = '_data',
        ):
    ""
    metadata_filename = 'snecs_tables-metadata.json'
        
    metadata_table_group_dict = \
        csvw_functions_extra.get_metadata_table_group_dict(
        data_folder,
        metadata_filename
        )
        
    return metadata_table_group_dict
        

    
#%% main functions



def get_government_office_region_elec(
        year=None,
        region_code=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        ):
    ""
    table_name='elec_GOR_stacked_2005_21'
    
    fp_database=os.path.join(data_folder,database_name)
    
    where_clause=csvw_functions_extra.get_where_clause_list(
        {'year':year, 'gor':region_code}
        )
        
    query=f"SELECT * FROM {table_name} {where_clause};"
    if verbose:
        print(query)
    
    with sqlite3.connect(fp_database) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        result=[dict(x) for x in c.execute(query).fetchall()]
        
    return result


def get_government_office_region_gas(
        year=None,
        region_code=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        ):
    ""
    table_name='gas_GOR_stacked_2005_21'
    
    fp_database=os.path.join(data_folder,database_name)
    
    where_clause=csvw_functions_extra.get_where_clause_list(
        {'year':year, 'region.code':region_code}
        )
        
    query=f"SELECT * FROM {table_name} {where_clause};"
    if verbose:
        print(query)
    
    with sqlite3.connect(fp_database) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        result=[dict(x) for x in c.execute(query).fetchall()]
        
    return result
    

def get_local_authority_elec(
        year=None,
        la_code=None,
        region=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        ):
    ""
    table_name='elec_LA_stacked_2005_21'
    
    fp_database=os.path.join(data_folder,database_name)
    
    where_clause=csvw_functions_extra.get_where_clause_list(
        {'year':year, 'la_code':la_code, 'region':region}
        )
        
    query=f"SELECT * FROM {table_name} {where_clause};"
    if verbose:
        print(query)
    
    with sqlite3.connect(fp_database) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        result=[dict(x) for x in c.execute(query).fetchall()]
        
    return result


def get_local_authority_gas(
        year=None,
        la_code=None,
        region=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        ):
    ""
    table_name='gas_LA_stacked_2005_21'
    
    fp_database=os.path.join(data_folder,database_name)
    
    where_clause=csvw_functions_extra.get_where_clause_list(
        {'year':year, 'la.code':la_code, 'region':region}
        )
        
    query=f"SELECT * FROM {table_name} {where_clause};"
    if verbose:
        print(query)
    
    with sqlite3.connect(fp_database) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        result=[dict(x) for x in c.execute(query).fetchall()]
        
    return result


def get_LSOA_elec_domestic(
        year=None,
        la_code=None,
        msoa_code=None,
        lsoa_code=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        ):
    ""
    table_name='elec_domestic_LSOA_stacked_2010_21'
    
    fp_database=os.path.join(data_folder,database_name)
    
    where_clause=csvw_functions_extra.get_where_clause_list(
        {'YEAR':year, 'LACode':la_code, 'MSOACode':msoa_code,'LSOACode':lsoa_code}
        )
        
    query=f"SELECT * FROM {table_name} {where_clause};"
    if verbose:
        print(query)
    
    with sqlite3.connect(fp_database) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        result=[dict(x) for x in c.execute(query).fetchall()]
        
    return result
    

def get_LSOA_gas_domestic(
        year=None,
        la_code=None,
        msoa_code=None,
        lsoa_code=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        ):
    ""
    table_name='gas_domestic_LSOA_stacked_2010_21'
    
    fp_database=os.path.join(data_folder,database_name)
    
    where_clause=csvw_functions_extra.get_where_clause_list(
        {'year':year, 
         'la.name':la_code,  # error in CSV file -> 'name' and 'code' are wrong way round
         'msoa.name':msoa_code,
         'lsoa.name':lsoa_code}
        )
        
    query=f"SELECT * FROM {table_name} {where_clause};"
    if verbose:
        print(query)
    
    with sqlite3.connect(fp_database) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        result=[dict(x) for x in c.execute(query).fetchall()]
        
    return result
    

def get_MSOA_elec_domestic(
        year=None,
        la_code=None,
        msoa_code=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        ):
    ""
    table_name='elec_domestic_MSOA_stacked_2010_21'
    
    fp_database=os.path.join(data_folder,database_name)
    
    where_clause=csvw_functions_extra.get_where_clause_list(
        {'YEAR':year, 'LACode':la_code, 'MSOAcode':msoa_code}
        )
        
    query=f"SELECT * FROM {table_name} {where_clause};"
    if verbose:
        print(query)
    
    with sqlite3.connect(fp_database) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        result=[dict(x) for x in c.execute(query).fetchall()]
        
    return result
    

def get_MSOA_gas_domestic(
        year=None,
        la_code=None,
        msoa_code=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        ):
    ""
    table_name='gas_domestic_MSOA_stacked_2010_21'
    
    fp_database=os.path.join(data_folder,database_name)
    
    where_clause=csvw_functions_extra.get_where_clause_list(
        {'year':year, 'la.code':la_code, 'msoa.code':msoa_code}
        )
        
    query=f"SELECT * FROM {table_name} {where_clause};"
    if verbose:
        print(query)
    
    with sqlite3.connect(fp_database) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        result=[dict(x) for x in c.execute(query).fetchall()]
        
    return result
    

def get_MSOA_elec_non_domestic(
        year=None,
        la_code=None,
        msoa_code=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        ):
    ""
    table_name='elec_non_domestic_MSOA_stacked_2010_21'
    
    fp_database=os.path.join(data_folder,database_name)
    
    where_clause=csvw_functions_extra.get_where_clause_list(
        {'YEAR':year, 'LACode':la_code, 'MSOAcode':msoa_code}
        )
        
    query=f"SELECT * FROM {table_name} {where_clause};"
    if verbose:
        print(query)
    
    with sqlite3.connect(fp_database) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        result=[dict(x) for x in c.execute(query).fetchall()]
        
    return result
     

def get_MSOA_gas_non_domestic(
        year=None,
        la_code=None,
        msoa_code=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        ):
    ""
    table_name='gas_non_domestic_MSOA_stacked_2010_21'
    
    fp_database=os.path.join(data_folder,database_name)
    
    where_clause=csvw_functions_extra.get_where_clause_list(
        {'year':year, 'la.code':la_code, 'msoa.code':msoa_code}
        )
        
    query=f"SELECT * FROM {table_name} {where_clause};"
    if verbose:
        print(query)
    
    with sqlite3.connect(fp_database) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        result=[dict(x) for x in c.execute(query).fetchall()]
        
    return result
    

def get_postcode_elec_all_meters(
        year,
        postcode=None,
        outcode=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        ):
    ""
    table_name=f'Postcode_level_all_meters_electricity_{year}'
    
    fp_database=os.path.join(data_folder,database_name)
    
    where_clause=csvw_functions_extra.get_where_clause_list(
        dict(Postcode=postcode, Outcode=outcode)
        )
        
    query=f"SELECT * FROM {table_name} {where_clause};"
    if verbose:
        print(query)
    
    with sqlite3.connect(fp_database) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        result=[dict(x) for x in c.execute(query).fetchall()]
        
    return result


def get_postcode_elec_economy_7(
        year,
        postcode=None,
        outcode=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        ):
    ""
    table_name=f'Postcode_level_economy_7_electricity_{year}'
    
    fp_database=os.path.join(data_folder,database_name)
    
    where_clause=csvw_functions_extra.get_where_clause_list(
        dict(Postcode=postcode, Outcode=outcode)
        )
        
    query=f"SELECT * FROM {table_name} {where_clause};"
    if verbose:
        print(query)
    
    with sqlite3.connect(fp_database) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        result=[dict(x) for x in c.execute(query).fetchall()]
        
    return result
        

def get_postcode_elec_standard(
        year,
        postcode=None,
        outcode=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        ):
    ""
    table_name=f'Postcode_level_standard_electricity_{year}'
    
    fp_database=os.path.join(data_folder,database_name)
    
    where_clause=csvw_functions_extra.get_where_clause_list(
        dict(Postcode=postcode, Outcode=outcode)
        )
        
    query=f"SELECT * FROM {table_name} {where_clause};"
    if verbose:
        print(query)
    
    with sqlite3.connect(fp_database) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        result=[dict(x) for x in c.execute(query).fetchall()]
        
    return result


def get_postcode_gas(
        year,
        postcode=None,
        outcode=None,
        data_folder='_data',
        database_name='snecs_data.sqlite',
        verbose=False
        ):
    ""
    table_name=f'Postcode_level_gas_{year}'
    
    fp_database=os.path.join(data_folder,database_name)
    
    where_clause=csvw_functions_extra.get_where_clause_list(
        dict(Postcode=postcode, Outcode=outcode)
        )
        
    query=f"SELECT * FROM {table_name} {where_clause};"
    if verbose:
        print(query)
    
    with sqlite3.connect(fp_database) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        result=[dict(x) for x in c.execute(query).fetchall()]
        
    return result
    
    
