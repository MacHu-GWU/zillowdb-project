#!/usr/bin/env python
# -*- coding: utf-8 -*-

import bs4
import requests

from zillowdb.api_manager import BaseApiKey, APIManager
from zillowdb.api_manager import NoAvailableAPIError, BaseApiKeyNotWorkingError


class ZillowAPIKey(BaseApiKey):
    _primary_key = "zws_id"
    
    def __init__(self, zws_id):
        self.zws_id = zws_id

    def setup_client(self):
        self._client = None
        
 
class ZillowClient(object):
    
    def __init__(self, zws_id):
        self.zws_id = zws_id
        self.endpoint = "http://www.zillow.com/webservice/GetUpdatedPropertyDetails.htm?zws-id={zws_id}&zpid={zpid}"
        
    def get_property_detail(self, zpid):
        res = requests.get(self.endpoint.format(zws_id=self.zws_id, zpid=zpid))
        print(res.status_code)
        print(res.text) 
        if res.status_code == 0:
            soup = bs4.BeautifulSoup(res.text, "html.parser")
            print(soup.prettify())


if __name__ == "__main__":
    
    zws_id = "X1-ZWz1dyb91hllhn_6msnx"
    z_client = ZillowClient(zws_id)
    
    zpid = 48749425
    z_client.get_property_detail(zpid)