#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    input = raw_input
except:
    pass

import time
import random

from crawl_zillow import zilo_urlencoder as urlencoder
from crawl_zillow import zilo_htmlparser as htmlparser

from zillowdb import config
from zillowdb.mongodb import (state_col, county_col, zipcode_col, street_col,
    address_col_mapper, conn_zillowdb)
from zillowdb.model import (StatusCode,
    State, County, Zipcode, Street, Address)
from zillowdb.logger import (
    create_zillow_crawler_logger, create_trulia_crawler_logger
)
from zillowdb.packages.selenium_spider import ChromeSpider
from zillowdb.packages.crawlib.spider import spider
from zillowdb.packages.crawlib import exc
from zillowdb.packages.sfm import pymongo_mate

ERROR_WAIT_TIME = 3600 

keys = set(["state", "county", "zipcode", "street"])

def create_webdriver():
    driver = ChromeSpider(executable_path=config.CHROMEDRIVER_PATH)
    return driver

def crawl_state():
    """Create all state info.
    """
    logger = create_zillow_crawler_logger()
    driver = create_webdriver()
    input("Press Enter when your browser is ready... ")
    
    logger.info("Crawl all state info ...")
    with driver as driver:
        url = urlencoder.browse_home_listpage_url()
        try:
            html = driver.get_html(url)
            while True:
                if "http://www.google.com/recaptcha/api.js" in html:
                    logger.info("Captcha Warning!", 1)
                    input("Please Solve the Captcha! Then Press Enter ...")
                    html = driver.get_html(url)
                else:
                    break
                
            data = list()
            try:
                for link, name in htmlparser.get_items(html, url):
                    state = State(_id=link, name=name, status=StatusCode.todo)
                    state.key = state.get_key()
                    data.append(state)
            except Exception as e1:  # HtmlParseError or CaptchaError, most like zillow blocks me
                logger.error("%s" % e1, 1)
    
            if len(data):
                State.smart_insert(data)
        except Exception as e2:
            logger.error("failed to crawl %s: %s" % (url, e2), 1)


def crawl_county():
    """Create all county info.
    """
    logger = create_zillow_crawler_logger()
    driver = create_webdriver()
    input("Press Enter when your browser is ready... ")
    
    # select todo list
    filters = {"status": {"$ne": StatusCode.finished}}
    state_list = State.by_filter(filters).all()
    logger.info("Crawl county from %s state ..." % len(state_list))

    with driver as driver:
        for state in state_list:
            url = state.url
            logger.info("Crawl %s ..." % url, 1)
            
            try:
                # get html
                html = driver.get_html(url)
                while True:
                    if "http://www.google.com/recaptcha/api.js" in html:
                        logger.info("Captcha Warning!", 1)
                        input("Please Solve the Captcha! Then Press Enter ...")
                        html = driver.get_html(url)
                    else:
                        break
                
                # parse data
                try:
                    data = list()
                    for link, name in htmlparser.get_items(html, url):
                        county = County(
                            _id=link,
                            state=state.key,
                            name=name,
                            status=StatusCode.todo,
                        )
                        county.key = county.get_key()
                        data.append(county)
                    
                    # page has many items
                    if len(data):
                        County.smart_insert(data)
                        state.n_children = len(data)
                        state.status = StatusCode.finished
                        logger.info("Success", 2)
                    # most likely this listpage has no items, there's no error
                    else:
                        state.status = StatusCode.crawled_but_has_error
                        logger.info("No data", 2)
     
                except Exception as e1:  # HtmlParseError or CaptchaError, most like zillow blocks me
                    state.status = StatusCode.crawled_but_has_error
                    logger.error("%r" % e1, 2)
     
            except Exception as e2:  # HttpError
                logger.error("Http error: %s" % e2, 2)
                state.status = StatusCode.failed_to_crawl
     
            state.save()
        

def crawl_zipcode():
    """Create all county info.
    """
    logger = create_zillow_crawler_logger()
    driver = create_webdriver()
    input("Press Enter when your browser is ready... ")
    
    # select todo list
    filters = {"status": {"$ne": StatusCode.finished}}
    county_list = list(County.by_filter(filters))
    logger.info("Crawl zipcode from %s county ..." % len(county_list))
    
    counter = len(county_list)
    with driver as driver:
        for county in county_list:
            counter -= 1
            url = county.url
            logger.info("Crawl %s, %s left ..." % (url, counter), 1)

            try:
                # get html
                html = driver.get_html(url)
                while True:
                    if "http://www.google.com/recaptcha/api.js" in html:
                        logger.info("Captcha Warning!", 1)
                        input("Please Solve the Captcha! Then Press Enter ...")
                        html = driver.get_html(url)
                    else:
                        break
                  
                # parse data
                try:
                    data = list()
                    for link, name in htmlparser.get_items(html, url):
                        zipcode = Zipcode(
                            _id=link,
                            state=county.state,
                            county=county.key,
                            name=name,
                            status=StatusCode.todo,
                        )
                        zipcode.key = zipcode.get_key()
                        data.append(zipcode)
                      
                    # page has many items
                    if len(data):
                        Zipcode.smart_insert(data)
                        county.n_children = len(data)
                        county.status = StatusCode.finished
                        logger.info("Success", 2)
                    # most likely this listpage has no items, there's no error
                    else:
                        county.status = StatusCode.crawled_but_has_error
                        logger.info("No data", 2)
                  
                # HtmlParseError or CaptchaError, most like zillow blocks me
                except Exception as e1:
                    county.status = StatusCode.crawled_but_has_error
                    logger.error("%r" % e1, 2)
                      
            # HttpError
            except Exception as e2:
                logger.error("Http error: %s" % e2, 2)
                county.status = StatusCode.failed_to_crawl
       
            county.save()


def crawl_street():
    """Create all county info.
    """
    logger = create_zillow_crawler_logger()
    driver = create_webdriver()
    input("Press Enter when your browser is ready... ")
    
    # select todo list
    filters = {
        "state": "md",
        "status": {"$ne": StatusCode.finished},
    }
    zipcode_list = list(Zipcode.by_filter(filters))
    logger.info("Crawl street from %s zipcode ..." % len(zipcode_list))
    
    counter = len(zipcode_list)
    with driver as driver:
        for zipcode in zipcode_list:
            counter -= 1
            url = zipcode.url
            logger.info("Crawl %s, %s left ..." % (url, counter), 1)

            try:
                # get html
                html = driver.get_html(url)
                while True:
                    if "http://www.google.com/recaptcha/api.js" in html:
                        logger.info("Captcha Warning!", 1)
                        input("Please Solve the Captcha! Then Press Enter ...")
                        html = driver.get_html(url)
                    else:
                        break
                  
                # parse data
                try:
                    data = list()
                    for link, name in htmlparser.get_items(html, url):
                        street = Street(
                            _id=link,
                            state=zipcode.state,
                            county=zipcode.county,
                            zipcode=zipcode.key,
                            name=name,
                            status=StatusCode.todo,
                        )
                        street.key = street.get_key()
                        data.append(street)
                      
                    # page has many items
                    if len(data):
                        Street.smart_insert(data)
                        zipcode.n_children = len(data)
                        zipcode.status = StatusCode.finished
                        logger.info("Success", 2)
                    # most likely this listpage has no items, there's no error
                    else:
                        zipcode.status = StatusCode.crawled_but_has_error
                        logger.info("No data", 2)
                  
                # HtmlParseError or CaptchaError, most like zillow blocks me
                except Exception as e1:
                    zipcode.status = StatusCode.crawled_but_has_error
                    logger.error("%r" % e1, 2)
                      
            # HttpError
            except Exception as e2:
                logger.error("Http error: %s" % e2, 2)
                zipcode.status = StatusCode.failed_to_crawl
       
            zipcode.save()


def crawl_address():
    """Create all county info.
    """
    logger = create_zillow_crawler_logger()
    driver = create_webdriver()
    input("Press Enter when your browser is ready... ")
    
    # select todo list
    filters = {
        "state": "md",
        "status": {"$ne": StatusCode.finished},
    }
    street_list = list(Street.by_filter(filters))
    logger.info("Crawl address from %s street ..." % len(street_list))
    
    counter = len(street_list)
    with driver as driver:
        for street in street_list:
            counter -= 1
            url = street.url
            logger.info("Crawl %s, %s left ..." % (url, counter), 1)
            
            col = address_col_mapper[street.state]
            try:
                # get html
                html = driver.get_html(url)
                while True:
                    if "http://www.google.com/recaptcha/api.js" in html:
                        logger.info("Captcha Warning!", 1)
                        input("Please Solve the Captcha! Then Press Enter ...")
                        html = driver.get_html(url)
                    else:
                        break
                  
                # parse data
                try:
                    data = list()
                    for link, name in htmlparser.get_items(html, url):
                        address = Address(
                            _id=link,
                            state=street.state,
                            county=street.county,
                            zipcode=street.zipcode,
                            street=street.key,
                            name=name,
                            status=StatusCode.todo,
                        )
                        address.key = address.get_key()
                        doc = address.to_dict()
                        data.append(doc)
                      
                    # page has many items
                    if len(data):
                        # 因为我们将address按照state分表
                        # 所以使用pymongo_mate.smart_insert中的方法
                        pymongo_mate.smart_insert(col, data)
                        street.n_children = len(data)
                        street.status = StatusCode.finished
                        logger.info("Success", 2)
                    # most likely this listpage has no items, there's no error
                    else:
                        street.status = StatusCode.crawled_but_has_error
                        logger.info("No data", 2)
                  
                # HtmlParseError or CaptchaError, most like zillow blocks me
                except Exception as e1:
                    street.status = StatusCode.crawled_but_has_error
                    logger.error("%r" % e1, 2)
                      
            # HttpError
            except Exception as e2:
                logger.error("Http error: %s" % e2, 2)
                street.status = StatusCode.failed_to_crawl
       
            street.save()
            
def crawl_house_detail_from_zillow():
    """Crawl house detail from zillow. Zillow has more address available than
    Trulia.
    """    
    logger = create_trulia_crawler_logger()
    
    def select_address():
        filters = {
            "status_zillow": StatusCode.todo, 
            "county": "montgomery-county",
        }
        wanted = {
            "_id": True,
#             "name": True,
#             "county": True,
            "state": True,
#             "zipcode": True,
        }
        data = list()
        for state in ["md",]:
            col = address_col_mapper[state]
            for doc in col.find(filters, wanted):
                data.append(doc)
        return data
    
    logger = create_zillow_crawler_logger()
    address_list = select_address()
    counter = len(address_list)
    logger.info("Crawl %s address detail ..." % counter)
    
    driver = create_webdriver()
    input("Press Enter when your browser is ready... ")
    
    for doc in address_list:
        counter -=1
        url = urlencoder.url_join(doc["_id"])
        logger.info("Crawl %s, %s left ..." % (url, counter))
        
        set_doc = dict()
        try:
            # get html
            html = driver.get_html(url)
            while True:
                if "http://www.google.com/recaptcha/api.js" in html:
                    logger.info("Captcha Warning!", 1)
                    input("Please Solve the Captcha! Then Press Enter ...")
                    html = driver.get_html(url)
                else:
                    break
            
            try:
                data = htmlparser.get_house_detail(html)
                if data is None:
                    set_doc["status_zillow"] = StatusCode.crawled_but_has_error
                    logger.info(exc.ParseError(url), 1)
                else:
                    set_doc["zillow_detail"] = data
                    set_doc["status_zillow"] = StatusCode.finished
                    logger.info("Success!", 1)
                    
            except Exception as e1:
                set_doc["status_zillow"] = StatusCode.crawled_but_has_error
                logger.info(exc.ParseError(str(e)), 1)
                
        except Exception as e2:
            set_doc["status_zillow"] = StatusCode.failed_to_crawl
            logger.info("http request error: %s" % url, 1)

        col = address_col_mapper[doc["state"]]
        col.update_one({"_id": doc["_id"]}, {"$set": set_doc})
    
    logger.info("Complete!")


if __name__ == "__main__":
    """
    """
#     crawl_state()
#     crawl_county()
#     crawl_zipcode()
#     crawl_street()
#     crawl_address()
    crawl_house_detail_from_zillow()