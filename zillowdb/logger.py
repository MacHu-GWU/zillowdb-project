#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from zillowdb.config import LOG_DIR_PATH
from zillowdb.packages.pathlib_mate import Path
from zillowdb.packages.loggerFactory import TimeRotatingLogger 

try:
    os.makedirs(LOG_DIR_PATH)
except:
    pass


def create_zillow_crawler_logger():
    logger = TimeRotatingLogger(
        name="zillowdb", 
        path=Path(LOG_DIR_PATH, "zillow_crawler.log").abspath, 
        rotate_on_when="D",
    )
    return logger


def create_trulia_crawler_logger():
    logger = TimeRotatingLogger(
        name="truliadb", 
        path=Path(LOG_DIR_PATH, "truliadb.log").abspath, 
        rotate_on_when="D",            
    )
    return logger