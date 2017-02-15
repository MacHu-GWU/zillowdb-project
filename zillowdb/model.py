#!/usr/bin/env python
# -*- coding: utf-8 -*-

from zillowdb.packages.sfm.mongoengine_mate import mongoengine, ExtendedDocument
from crawl_zillow import zilo_urlencoder

class StatusCode(object):
    todo = 0
    failed_to_crawl = 1 # http error
    crawled_but_has_error = 2 # html parse error
    finished = 3  # crawled without error


class AddressMetaDocument(ExtendedDocument):

    def get_key(self):
        return self._id.split("/")[-2]
    
    @property
    def url(self):
        return zilo_urlencoder.browse_home_listpage_url_by_href(self._id)
        
    meta = {
        "abstract": True,
    }


class State(AddressMetaDocument):
    _id = mongoengine.StringField(primary_key=True)
    key = mongoengine.StringField()
    name = mongoengine.StringField()
    n_children = mongoengine.IntField()
    status = mongoengine.IntField()

    meta = {
        "db_alias": "default",
        "collection": "state",
    }


class County(AddressMetaDocument):
    _id = mongoengine.StringField(primary_key=True)
    state = mongoengine.StringField()

    key = mongoengine.StringField()
    name = mongoengine.StringField()
    n_children = mongoengine.IntField()
    status = mongoengine.IntField()

    meta = {
        "db_alias": "default",
        "collection": "county",
    }


class Zipcode(AddressMetaDocument):
    _id = mongoengine.StringField(primary_key=True)
    state = mongoengine.StringField()
    county = mongoengine.StringField()

    key = mongoengine.StringField()
    name = mongoengine.StringField()
    n_children = mongoengine.IntField()
    status = mongoengine.IntField()

    meta = {
        "db_alias": "default",
        "collection": "zipcode",
    }


class Street(AddressMetaDocument):
    _id = mongoengine.StringField(primary_key=True)
    state = mongoengine.StringField()
    county = mongoengine.StringField()
    zipcode = mongoengine.StringField()

    key = mongoengine.StringField()
    name = mongoengine.StringField()
    n_children = mongoengine.IntField()
    status = mongoengine.IntField()
    status_zillow = mongoengine.IntField()
    
    meta = {
        "db_alias": "default",
        "collection": "street",
    }


class Address(AddressMetaDocument):
    _id = mongoengine.StringField(primary_key=True)
    state = mongoengine.StringField()
    county = mongoengine.StringField()
    zipcode = mongoengine.StringField()
    street = mongoengine.StringField()

    key = mongoengine.StringField()
    name = mongoengine.StringField()
    status = mongoengine.IntField()
    
    trulia_detail = mongoengine.DictField()
    zillow_detail = mongoengine.DictField()
    zillow_api
    
    meta = {
        "db_alias": "default",
        "collection": "address",
    }
    
    @property
    def url(self):
        return zilo_urlencoder.url_join(self._id)
    
    
#--- Test ---
# import time
# from macro.bot import bot
# from zillowdb import config
# from zillowdb.logger import (
#     create_zillow_crawler_logger, create_trulia_crawler_logger
# )
# from zillowdb.packages.crawlib.spider import spider
# from zillowdb.packages.selenium_spider import ChromeSpider
# from crawl_zillow import zilo_urlencoder as urlencoder
# from crawl_zillow import zilo_htmlparser as htmlparser


def crawl_state():
    url = urlencoder.listpage_url()
    try:
        html = spider.get_html(url, encoding="utf-8")
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
    # select todo list
    filters = {"status": {"$ne": StatusCode.finished}}

    state_list = State.objects(__raw__=filters).all()
    logger.info("Crawl county from %s state ..." % len(state_list))

    for state in state_list:
        url = state.url
        logger.info("Crawl %s ..." % url, 1)
        try:
            html = spider.get_html(url, encoding="utf-8")
 
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
 
                if len(data):
                    County.smart_insert(data)
                    state.n_children = len(data)
                    state.status = StatusCode.finished
                    logger.info("Success", 2)
                    consecutive_error_counter = 0
                else:
                    state.status = StatusCode.crawled_but_has_error
                    logger.info("No data", 2)
 
            except Exception as e1:  # HtmlParseError or CaptchaError, most like zillow blocks me
                state.status = StatusCode.crawled_but_has_error
                logger.error("%r" % e1, 2)
                consecutive_error_counter += 1
                if consecutive_error_counter == 10:
                    logger.info("IP is blocked! Wait %s seconds ..." % ERROR_WAIT_TIME)
                    time.sleep(ERROR_WAIT_TIME)
                    consecutive_error_counter = 0
 
        except Exception as e2:  # HttpError
            logger.error("Http error: %s" % e2, 2)
            state.status = StatusCode.failed_to_crawl
 
        state.save()


def crawl_zipcode():
    # select todo list
    filters = {
#         "state": "md", 
        "status": {"$ne": StatusCode.finished},
    }

    county_list = County.objects(__raw__=filters).all()
    logger.info("Crawl zipcode from %s county ..." % len(county_list))

    for county in county_list:
        url = county.url

        logger.info("Crawl %s ..." % url, 1)
        try:
            html = spider.get_html(url, encoding="utf-8")

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

                if len(data):
                    Zipcode.smart_insert(data)
                    county.n_children = len(data)
                    county.status = StatusCode.finished
                    logger.info("Success", 2)
                    consecutive_error_counter = 0
                else:
                    county.status = StatusCode.crawled_but_has_error
                    logger.info("No data", 2)

            except Exception as e1:  # HtmlParseError or CaptchaError
                county.status = StatusCode.crawled_but_has_error
                logger.error("%r" % e1, 2)
                consecutive_error_counter += 1
                if consecutive_error_counter == 10:
                    logger.info("IP is blocked! Wait %s seconds ..." % ERROR_WAIT_TIME)
                    time.sleep(ERROR_WAIT_TIME)
                    consecutive_error_counter = 0

        except Exception as e2:  # HttpError
            logger.error("Http error: %s" % e2, 2)
            county.status = StatusCode.failed_to_crawl

        county.save()


def crawl_street():
    # select todo list
    filters = {
        "state": "md", 
#         "county": "montgomery-county",
        "status": {"$ne": StatusCode.finished},
    }

    zipcode_list = Zipcode.objects(__raw__=filters).all()
    logger.info("Crawl street from %s zipcode ..." % len(zipcode_list))

    for zipcode in zipcode_list:
        url = zipcode.url

        logger.info("Crawl %s ..." % url, 1)
        try:
            html = spider.get_html(url, encoding="utf-8")

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

                if len(data):
                    Street.smart_insert(data)
                    zipcode.n_children = len(data)
                    zipcode.status = StatusCode.finished
                    logger.info("Success", 2)
                    consecutive_error_counter = 0
                else:
                    zipcode.status = StatusCode.crawled_but_has_error
                    logger.info("No data", 2)

            except Exception as e1:  # HtmlParseError or CaptchaError, most like zillow blocks me
                zipcode.status = StatusCode.crawled_but_has_error
                logger.error("%r" % e1, 2)
                consecutive_error_counter += 1
                if consecutive_error_counter == 10:
                    logger.info("IP is blocked! Wait %s seconds ..." % ERROR_WAIT_TIME)
                    time.sleep(ERROR_WAIT_TIME)
                    consecutive_error_counter = 0

        except Exception as e2:  # HttpError
            logger.error("Http error: %s" % e2, 2)
            zipcode.status = StatusCode.failed_to_crawl

        zipcode.save()


def crawl_address():
    logger = create_zillow_crawler_logger()
    driver = ChromeSpider(
        executable_path=r"C:\Users\shu\Documents\PythonWorkSpace\py3\py33_projects\zillowdb-project\chromedriver.exe",
    )
    input("Press enter when your browser is ready ...")
    # select todo list
    #     filters = {"status": {"$ne": StatusCode.finished}}
    
    with driver as driver:
        filters = {
            "state": "md", 
    #         "county": "montgomery-county",
            "status": {"$ne": StatusCode.finished},
        }
         
        consecutive_error_counter = 0
         
        street_list = Street.objects(__raw__=filters).limit(1000).all()
        logger.info("Crawl address from %s street ..." % len(street_list))
     
        for street in street_list:
            col = address_col_mapper[street.state]
            url = street.url
     
            logger.info("Crawl %s ..." % url, 1)
            try:
                html = driver.get_html(url)
                if "http://www.google.com/recaptcha/api.js" in html:
                    logger.info("Captcha Warning!", 2)
                    input("Please Solve the Captcha! Then Press Enter ...")
                    
#                     x, y = bot.get_position()
#                     bot.left_click(2431, 390, 3) # click I'm not a robot
#                     bot.move_to(x, y, post_dl=10.0)
#                     
#                     x, y = bot.get_position()
#                     bot.left_click(2556, 467, 1) # click Submit
#                     bot.move_to(x, y, post_dl=6.0) 
                    html = driver.get_html(url)
                
                # Captcha Error
                if "http://www.google.com/recaptcha/api.js" in html:
                    street.status = StatusCode.crawled_but_has_error
                    logger.error("Failed to solve captcha!", 2)
                    consecutive_error_counter += 1
                    if consecutive_error_counter == 10:
                        logger.info("IP is blocked! Wait %s seconds ..." % ERROR_WAIT_TIME)
                        street.save()
                        time.sleep(ERROR_WAIT_TIME)
                        consecutive_error_counter = 0
                    
                else:
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
                         
                        if len(data):
                            # 因为我们将address按照state分表
                            # 所以使用pymongo_mate.smart_insert中的方法
                            smart_insert(col, data)
                            street.n_children = len(data)
                            street.status = StatusCode.finished
                            logger.info("Success", 2)
                            consecutive_error_counter = 0
                        else:
                            street.status = StatusCode.crawled_but_has_error
                            logger.info("No data", 2)
                                 
                    except Exception as e1:  # HtmlParseError or PageNotFound
                        street.status = StatusCode.crawled_but_has_error
                        logger.error("%r" % e1, 2)
      
            except Exception as e2:  # HttpError
                logger.error("Http error: %s" % e2, 2)
                street.status = StatusCode.failed_to_crawl
      
            street.save()


def crawl_house_detail():
    """Crawl house details.
    """
    from crawl_trulia.htmlparser import htmlparser as t_htmlparser
    from crawl_trulia.htmlparser import validate
    from crawl_trulia.urlencoder import urlencoder as t_urlencoder
    
    logger = create_trulia_crawler_logger()
    
    def select():
        filters = {
            "status": StatusCode.todo, 
#             "county": "baltimore-county",
        }
        wanted = {"name": True, "county": True, "state": True, "zipcode": True}
        data = list()
        for state in ["md"]:
            col = address_col_mapper[state]
            for doc in col.find(filters, wanted):
                data.append(doc)
        return data
    
    selected_data = select()
    counter = len(selected_data)
    logger.info("Crawl %s address detail ..." % counter)
    
    for doc in selected_data:
        address = doc["name"].split("  ")[0]
        try:
            city = doc["name"].split("  ")[1].split(", ")[0]
        except:
            city = None
        
        if city is None:
            try:
                city = doc["county"].replace("-county", "")
            except:
                city = None
                
        zipcode = doc["zipcode"]
        url = t_urlencoder.by_address_city_and_zipcode(address, city, zipcode)
        
        counter -=1
        logger.info("%s  %s, %s, %s left ..." % (address, city, zipcode, counter), 1)
         
        set_doc = dict()
        try:
            html = spider.get_html(url, encoding="utf-8")
            data = t_htmlparser.get_house_detail(html)
            # html parse error
            if data is None:
                set_doc["status"] = StatusCode.crawled_but_has_error
                logger.info("Html parse error!", 2)
            else:
                if validate(data):
                    set_doc["status"] = StatusCode.finished
                    set_doc["trulia_detail"] = data
                    logger.info("Success!", 2)
                else:
                    set_doc["status"] = StatusCode.crawled_but_has_error
                    logger.info("Html parse error!", 2)
         
        # http error
        except Exception as e1:
            set_doc["status"] = StatusCode.failed_to_crawl
            logger.info("http request error: %s" % url, 2)
             
#         col = address_col_mapper[doc["state"]]
#         col.update_one({"_id": doc["_id"]}, {"$set": set_doc})
    
    logger.info("Complete!")


if __name__ == "__main__":
    """
    """
#     crawl_state()
#     crawl_county()
#     crawl_zipcode()
#     crawl_street()
#     crawl_address()
#     crawl_house_detail()

#     col = address_col_mapper["md"]
#     col.update({"status": {"$ne": 0}}, {"$set": {"status": 0, "trulia_detail": None}}, multi=True)