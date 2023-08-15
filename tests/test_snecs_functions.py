# -*- coding: utf-8 -*-
"""
Created on Wed Aug  9 13:08:36 2023

@author: cvskf
"""

import unittest

from snecs_functions import snecs_functions

import datetime
import json


# get latest_data_file_info JSON
#latest_data_file_info=ogp_functions.get_latest_data_file_info_json()

#data=ogp_functions.load_data()

#with open('data.json','w') as f:
#    json.dump(data,f,indent=4)


#o=ogp_functions.OGPdata()


class TestMainFunctions(unittest.TestCase):
    ""
    
    # def test_get_region_from_local_authority_district(self):
    #     ""
        
    #     result=ogp_functions.get_region_from_local_authority_district(
    #         'E06000001'
    #         )
    #     #print(result)
        
    #     self.assertEqual(
    #         result,
    #         ['E12000001']
    #         )
        
        



class TestDataFolder(unittest.TestCase):
    ""
    
    def test_set_data_folder(self):
        ""
        
        fp=r'C:\Users\cvskf\OneDrive - Loughborough University\_Git\building-energy\snecs_functions\snecs_functions\snecs_tables-metadata.json'
        
        snecs_functions.set_data_folder(
            metadata_document_location=fp,
            #verbose=False,
            #_reload_all_database_tables=True
            )
        

    
if __name__=='__main__':
    
    unittest.main()