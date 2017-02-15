#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

"""


import time
from selenium import webdriver


class BaseSpider(object):
    """
    """
    load_timeout = 0.0
    sleep_interval = 0.0
    
    def set_load_timeout(self, value):
        self.load_timeout = value
        self.driver.set_page_load_timeout(self.load_timeout)
        
    def set_sleep_interval(self, value):
        self.sleep_interval = value
    
    def _sleep(self):
        time.sleep(self.sleep_interval)
        
    def get_html(self, url):
        self._sleep()
        self.driver.get(url)
        return self.driver.page_source
    
    def close(self):
        self.driver.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, *exc_info):
        self.close()


class ChromeSpider(BaseSpider):
    """
    """
    def __init__(self, executable_path):
        self.driver = webdriver.Chrome(executable_path=executable_path)
        
