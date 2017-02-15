#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

Name Space:

For saving space, I use abbreviation as field key:

- _id: _id
- st: state
- ct: county
- z: zipcode
- str: street
- ad: address
- fi: finished
- n: number

from zillowdb.mongodb import (state_col, county_col, zipcode_col, street_col, 
    address_col, address_col_mapper)
"""

from collections import OrderedDict

import pymongo
import mongoengine

DBNAME = "zillowdb2"

# Client
client = pymongo.MongoClient()
 
# Database
db = client.__getattr__(DBNAME)
 
# Address list collection
state_col = db.__getattr__("state")
county_col = db.__getattr__("county")
zipcode_col = db.__getattr__("zipcode")
street_col = db.__getattr__("street")

# Address detail collection
address_col_mapper = OrderedDict()
for doc in state_col.find():
    state = doc["key"]
    address_col_mapper[state] = db.__getattr__(state)

# MongoEngine
conn_zillowdb = mongoengine.connect(
    db = DBNAME,
    alias = "default",
    username=None, password=None, host="localhost", port=27017,
)

def read_all_state():
    all_state = [doc["key"] for doc in state_col.find()]
    all_state.sort()
    return all_state


#--- Unittest ---
if __name__ == "__main__":
    from zillowdb.logger import create_zillow_crawler_logger
    from zillowdb.model import StatusCode
    
    logger = create_zillow_crawler_logger()
    
    def fix_dc():
        """
        1. find dc state in state_col, change _id, key to "dc", status to 0, 
        n_children to None
        2. find all county {"state": "district-of-columbia-county"}
        """
        county_col.remove({"state": "district-of-columbia-county"})
        zipcode_col.remove({"state": "district-of-columbia-county"})
        
#     fix_dc()
    
    def browse_status():
        import prettytable
        
        t = prettytable.PrettyTable()
        t.field_names = ["State", "Address Count"]
        for state, col in address_col_mapper.items():
            total = col.find().count()
            t.add_row([state, total])
        print(t)
            
#     browse_status()