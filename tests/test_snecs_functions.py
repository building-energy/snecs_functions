# -*- coding: utf-8 -*-
"""
Created on Wed Aug  9 13:08:36 2023

@author: cvskf
"""

import unittest

from snecs_functions import snecs_functions

import datetime
import json


class TestDataFolder(unittest.TestCase):
    ""
    
    def test_set_data_folder(self):
        ""
        
        fp=r'C:\Users\cvskf\OneDrive - Loughborough University\_Git\building-energy\snecs_functions\snecs_functions\snecs_tables-metadata.json'
        
        snecs_functions.set_data_folder(
            metadata_document_location=fp,
            verbose=True,
            #overwrite_existing_files=True,
            remove_existing_tables=True,
            )


    def test__read_metadata_table_group_dict(self):
        ""
        


class TestMainFunctions(unittest.TestCase):
    ""
    
    def test__convert_to_iterator(self):
        ""
        
        
    def test__get_where_clause_list(self):
        ""
        
    
    def test_get_government_office_region_elec(self):
        ""
    
    
    def test_get_government_office_region_gas(self):
        ""
        
        result=snecs_functions.get_government_office_region_gas(
            year=2021,
            region_code='E12000004',
            #verbose=True
            )
        #print(result)
        self.assertEqual(
            result,
            [{'year': 2021, 
              'region.code': 'E12000004', 
              'region': 'East Midlands', 
              'domestic.meters': 1881.823, 
              'non.domestic.meters': 19.022, 
              'total.meters': 1900.845, 
              'domestic.sales.gwh': 24686.47417, 
              'non.domestic.sales.gwh': 15226.75651, 
              'total.sales.gwh': 39913.23068, 
              'domestic.mean.kwh': 13118.34081, 
              'non.domestic.mean.kwh': 800481.3642, 
              'mean.kwh': 20997.55882, 
              'domestic.median.kwh': 11719.01385, 
              'non.domestic.median.kwh': 148176.5675, 
              'median.kwh': 11800.46487}]
            )
    
    
    def test_get_local_authority_elec(self):
        ""
        
        
        
    def test_get_local_authority_gas(self):
        ""
        
        
    def test_LSOA_elec_domestic(self):
        ""
        
        
    def test_get_LSOA_gas_domestic(self):
        ""
        
        
    def test_get_MSOA_elec_domestic(self):
        ""
        
        
    def test_get_MSOA_gas_domestic(self):
        ""
        
    
    def test_get_MSOA_elec_non_domestic(self):
        ""
        
    def test_get_MSOA_gas_non_domestic(self):
        ""
        
    def test_get_postcode_elec_all_meters(self):
        ""
        
    def test_get_postcode_elec_economy_7(self):
        ""
        
        
    def test_get_postcode_elec_standard(self):
        ""
        
    
    def test_get_postcode_gas(self):
        ""
        
        result=snecs_functions.get_postcode_gas(
            year=2021,
            postcode='LE11 3PE'
            )
        #print(result)
        self.assertEqual(
            result,
            [{'Outcode': 'LE11', 
              'Postcode': 'LE11 3PE', 
              'Num_meters': 31, 
              'Total_cons_kwh': 502192.22440749354, 
              'Mean_cons_kwh': 16199.749174435276, 
              'Median_cons_kwh': 15071.418638377336}]
            )






    
if __name__=='__main__':
    
    unittest.main()