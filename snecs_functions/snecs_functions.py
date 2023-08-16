# -*- coding: utf-8 -*-
"""
Created on Wed Aug  9 13:16:59 2023

@author: cvskf
"""

import urllib
import urllib.request
import json
import os
import zipfile
import sqlite3
import importlib.resources as pkg_resources
import ogp_functions
import subprocess
from datetime import datetime
import pandas as pd
import csv
from lxml import etree
from bs4 import BeautifulSoup
from csvw_functions import csvw_functions_extra


_default_data_folder='_data'  # the default
_default_database_name='snecs_data.sqlite'

urllib.request.urlcleanup()



#%% data folder

def set_data_folder(
        data_folder=_default_data_folder,
        verbose=True,
        metadata_document_location=r'https://raw.githubusercontent.com/building-energy/snecs_functions/main/snecs_functions/snecs_tables-metadata.json', 
        database_name=_default_database_name,
        _reload_all_database_tables=False  # for testing
        ):
    ""
    
    # download all tables to data_folder
    fp_metadata=\
        csvw_functions_extra.download_table_group(
            metadata_document_location,
            data_folder=data_folder,
            verbose=verbose
            )

    #return
        
    # import all tables to sqlite
    csvw_functions_extra.import_table_group_to_sqlite(
        metadata_document_location=fp_metadata,
        data_folder=data_folder,
        database_name=database_name,
        verbose=verbose,
        _reload_all_database_tables=_reload_all_database_tables
        )


def _read_metadata_table_group_dict(
        data_folder,
        ):
    ""
    fp=os.path.join(
        data_folder,
        'snecs_tables-metadata.json'
        )
    with open(fp) as f:
        metadata_table_group_dic=json.load(f)
        
    return metadata_table_group_dic
        

    
#%% main functions

def get_region_from_local_authority_district(
        lad_code,
        fp_database=os.path.join(_default_data_folder,_default_database_name)
        ):
    """
    """
    
    table_name='Local_Authority_District_to_Region_December_2022'
    
    with sqlite3.connect(fp_database) as conn:
        c = conn.cursor()
        query=f"""
        SELECT
            RGN22CD
        FROM
            "{table_name}"
        WHERE
            LAD22CD = "{lad_code}"
        """
        #print(query)
        result=[x[0] for x in c.execute(query).fetchall()]
        
    return result

