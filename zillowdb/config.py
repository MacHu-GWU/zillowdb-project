#!/usr/bin/env python
# -*- coding: utf-8 -*-

from zillowdb.packages.pathlib_mate import Path

MSSQLDB_SERVER = "QA-MSSQL-EDW1.qa.awscorp.com"
MSSQLDB_DATABASE = "wbh"
MSSQLDB_USERNAME = "wbh"
MSSQLDB_PASSWORD = "wbh"

PROJECT_DIR = Path(r"C:\Users\shu\Documents\PythonWorkSpace\py3\py33_projects\zillowdb-project").abspath
LOG_DIR_PATH = Path(PROJECT_DIR, "log").abspath
CHROMEDRIVER_PATH = Path(PROJECT_DIR, "chromedriver.exe").abspath

ZWSID = [
	"X1-ZWz1dyb91hllhn_6msnx",
	"X1-ZWz1fn13g3pwqz_4ixyf",
	"X1-ZWz1fn36f9zzt7_60bkd",
]