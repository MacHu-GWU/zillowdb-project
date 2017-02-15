#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

"""

from collections import OrderedDict

import sqlalchemy
from sqlalchemy import select, distinct
from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy import String, Integer, Float, Binary, Boolean
from sqlalchemy.sql import and_, or_, func


from zillowdb import config
from zillowdb.mongodb import address_col_mapper, read_all_state

engine = create_engine("mssql+pymssql://%s:%s@%s/%s?charset=utf8" % (
    config.MSSQLDB_USERNAME, config.MSSQLDB_PASSWORD, 
    config.MSSQLDB_SERVER, config.MSSQLDB_DATABASE
))

metadata = MetaData()

address_table_mapper = OrderedDict()

# Create tables
# for state in read_all_state():
for state in ["md", ]:
    t_name = "zillow_%s" % state
    table = Table(t_name, metadata,
        Column("zid", Integer, primary_key=True),
        Column("address", String(64)),
        Column("city", String(64)),
        Column("state", String(16)),
        Column("zipcode", String(16)),
        Column("status", Integer),
        Column("z_res", Binary),
        Column("bedroom", Float),
        Column("bathroom", Float),
        Column("sqft", Integer),
    )
    address_table_mapper[state] = table

metadata.create_all(engine)

if __name__ == "__main__":
    from zillowdb.packages.sfm import sqlalchemy_mate
    from zillowdb.packages.loggerFactory import StreamOnlyLogger
    
    def copy_data_from_mongodb_to_mssqldb():
        logger = StreamOnlyLogger()
        
        max_size = 1000
        
        state = "md"
        col = address_col_mapper[state]
        table = address_table_mapper[state]
        
        data = list()
        counter = 0
        for doc in col.find():
            counter += 1
            # zid
            if doc["key"].endswith("_zpid"):
                zid = int(doc["key"][:-5])
                
                # address, city
                chunks = doc["name"].split("  ")
                if len(chunks) == 2: 
                    address, other = chunks
                    
                    chunks1 = other.split(",")
                    if len(chunks1) == 2:
                        city = chunks1[0]
                    else:
                        city = None
                else:
                    address, city = None, None
                
                # state
                state = doc["state"]
                
                # zipcode
                zipcode = doc["zipcode"]
                
                # status
                status = 0 # Todo
                
                row = {
                    "zid": zid, 
                    "address": address, 
                    "city": city,
                    "state": state,
                    "zipcode": zipcode,
                    "status": status,
                }
                data.append(row)
                if len(data) == max_size:
                    sqlalchemy_mate.smart_insert(engine, table, data)
                    logger.info("Complete %s documents ..." % counter)
                    data.clear()
        
        sqlalchemy_mate.smart_insert(engine, table, data)
    
#     copy_data_from_mongodb_to_mssqldb()
