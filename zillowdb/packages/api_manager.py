#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
API Manager is a tools to help you manage multiple API Key. Choose the usable
api key, and automatically archive the expired api key.
"""

#- nameddict -
import json
import copy
from collections import OrderedDict
from functools import total_ordering


@total_ordering
class Base(object):

    """nameddict base class.
    """
    __attrs__ = None
    """该属性非常重要, 定义了哪些属性被真正视为 ``attributes``, 换言之, 就是在
    :meth:`~Base.keys()`, :meth:`~Base.values()`, :meth:`~Base.items()`,
    :meth:`~Base.to_list()`, :meth:`~Base.to_dict()`, :meth:`~Base.to_OrderedDict()`,
    :meth:`~Base.to_json()`, 方法中要被包括的属性。
    """
    
    __excludes__ = []
    """在此被定义的属性将不会出现在 :meth:`~Base.items()` 中
    """
    
    __reserved__ = set(["keys", "values", "items"])

    def __init__(self, **kwargs):
        for attr, value in kwargs.items():
            setattr(self, attr, value)

    def __setattr__(self, attr, value):
        if attr in self.__reserved__:
            raise ValueError("%r is a reserved attribute name!" % attr)
        object.__setattr__(self, attr, value)

    def __repr__(self):
        kwargs = list()
        for attr, value in self.items():
            kwargs.append("%s=%r" % (attr, value))
        return "%s(%s)" % (self.__class__.__name__, ", ".join(kwargs))

    def __getitem__(self, key):
        """Access attribute.
        """
        return object.__getattribute__(self, key)

    @classmethod
    def _make(cls, d):
        """Make an instance.
        """
        return cls(**d)

    def items(self):
        """items按照属性的既定顺序返回attr, value对。当 ``__attrs__`` 未指明时,
        则按照字母顺序返回。若 ``__attrs__`` 已定义时, 按照其中的顺序返回。

        当有 ``@property`` 装饰器所装饰的属性时, 若没有在 ``__attrs__`` 中定义,
        则items中不会包含它。
        """
        items = list()
        
        if self.__attrs__ is None:
            for key, value in self.__dict__.items():
                if key not in self.__excludes__:
                    items.append((key, value))
            items = list(sorted(items, key=lambda x: x[0]))
            return items
        try:
            for attr in self.__attrs__:
                if attr not in self.__excludes__:
                    try:
                        items.append((attr, copy.deepcopy(getattr(self, attr))))
                    except AttributeError:
                        items.append(
                            (attr, copy.deepcopy(self.__dict__.get(attr))))
            return items
        except:
            raise AttributeError()

    def keys(self):
        """Iterate attributes name.
        """
        return [key for key, value in self.items()]

    def values(self):
        """Iterate attributes value.
        """
        return [value for key, value in self.items()]

    def __iter__(self):
        """Iterate attributes.
        """
        if self.__attrs__ is None:
            return iter(self.keys())
        try:
            return iter(self.__attrs__)
        except:
            raise AttributeError()

    def to_list(self):
        """Export data to list. Will create a new copy for mutable attribute.
        """
        return self.keys()

    def to_dict(self):
        """Export data to dict. Will create a new copy for mutable attribute.
        """
        return dict(self.items())

    def to_OrderedDict(self):
        """Export data to OrderedDict. Will create a new copy for mutable 
        attribute.
        """
        return OrderedDict(self.items())

    def to_json(self):
        """Export data to json. If it is json serilizable.
        """
        return json.dumps(self.to_dict())

    def __eq__(self, other):
        """Equal to.
        """
        return self.items() == other.items()

    def __lt__(self, other):
        """Less than.
        """
        for (_, value1), (_, value2) in zip(self.items(), other.items()):
            if value1 >= value2:
                return False
        return True

#--- exception_mate ---
import sys
import traceback


def get_last_exc_info():
    """Get last raised exception, and format the error message.
    """
    exc_type, exc_value, exc_tb = sys.exc_info()
    for filename, line_num, func_name, code in traceback.extract_tb(exc_tb):
        tmp = "{exc_value.__class__.__name__}: {exc_value}, appears in '{filename}' at line {line_num} in {func_name}(), code: {code}"
        info = tmp.format(
            exc_value=exc_value,
            filename=filename,
            line_num=line_num,
            func_name=func_name,
            code=code,
        )
        return info


class ExceptionHavingDefaultMessage(Exception):

    """A Exception class with default error message.
    """
    default_message = None

    def __str__(self):
        length = len(self.args)
        if length == 0:
            if self.default_message is None:
                raise NotImplementedError("default_message is not defined!")
            else:
                return self.default_message
        elif length == 1:
            return str(self.args[0])
        else:
            return str(self.args)


#- API Manager -
import sys
import random
import pprint
from collections import OrderedDict


class NoAvailableAPIError(ExceptionHavingDefaultMessage):
    """Raised when there's no API Key is usable.
    """
    default_message = "Run out of all API keys!"


class BaseApiKeyNotWorkingError(ExceptionHavingDefaultMessage):
    """Raised when API Key is not working, and not a Exceed Quota Error.
    """
    default_message = "This api key is not working!"


class BaseApiKey(Base):

    """An api key may have: access key, secret key, ... and arbitrary many
    information.

    :params primary_key: :class:`BaseApiKey` can have arbitary many attributes, 
      but there's only one is the primary_key. Please specify.
    :params _api_manager: is bind to the :class:`APIManager` instance.
    :params _client: a variable bind to the client actually using this api key.
      self._client is the object doing real api call.
    """
    _primary_key = None
    _api_manager = None
    _client = None
    __excludes__ = ["_primary_key", "_api_manager", "_client"]

    def setup_client(self, *args, **kwargs):
        """A method that create an api client.
        
        has to assign the client to :attr:`BaseApiKey._client`.
        
        :returns: a client using this api key to do real work.
        """
        raise NotImplementedError
        self._client = None
        return self._client

    def is_working(self, *args, **kwargs):
        """A method return True or False to indicate that if this API Key is 
        usable. Usually, it consumes one API quota.

        :returns flag: True, False
        """
        raise NotImplementedError
        flag = False
        return flag

    def get_primary_key(self):
        """Get the value of it's primary_key.
        """
        return getattr(self, self._primary_key)


class APIManager(object):

    """API manager holds collection of :class:`BaseApiKey`. And 

    :param apikey_pool: list of :class:`BaseApiKey`
    :param key_chain: ordered ``{apikey.primary_key: apikey}`` mapping
    :param used_counter:
    """
    def __init__(self, apikey_pool):
        self.key_chain = OrderedDict()
        self.archived_key_chain = OrderedDict()
        self.used_counter = dict()

        for api_key in apikey_pool:
            api_key._api_manager = self

            key = api_key.get_primary_key()

            self.key_chain[key] = api_key
            self.used_counter[key] = 0

    def fetch_one(self):
        try:
            return self.key_chain[random.choice(list(self.key_chain))]
        except IndexError:
            raise NoAvailableAPIError

    def remove_one(self, key):
        self.archived_key_chain[key] = self.key_chain.pop(key)

    def check_usable(self):
        for key, apikey in self.key_chain.items():
            if not apikey.is_working():
                self.remove_one(key)

        if len(self.key_chain) == 0:
            sys.stderr.write("\nThere's no API Key usable!")
        elif len(self.archived_key_chain) == 0:
            sys.stderr.write("\nAll API Key are usable.")
        else:
            sys.stderr.write("\nThese keys are not usable:")
            for key in self.archived_key_chain:
                sys.stderr.write("\n    %s" % key)

    def __repr__(self):
        return pprint.pformat(list(self.key_chain.items()))


#--- Unittest ---
if __name__ == "__main__":
    from geopy.geocoders import GoogleV3
    from geopy.exc import GeocoderQuotaExceeded
    
    def test_APIManager():
        class MyApiKey(BaseApiKey):
            _primary_key = "key"
        
        api_manager = APIManager(apikey_pool=[
            MyApiKey(key="a"), MyApiKey(key="b"), MyApiKey(key="c"),
        ])
        api_manager.remove_one("a")
        assert "a" not in api_manager.key_chain
        assert "a" in api_manager.archived_key_chain
        
    test_APIManager()
    

    class GoogleBaseApiKey(BaseApiKey):
        _primary_key = "key"
    
        def __init__(self, key):
            self.key = key
    
        def setup_client(self, *args, **kwargs):
            self.client = GoogleV3(self.key)
    
        def is_working(self):
            # The White Hhouse
            address = "1600 Pennsylvania Ave NW, Washington, DC 20500"
            expect_formatted_address = "1600 Pennsylvania Ave NW, Washington, DC 20500, USA"
    
            location = self.client.geocode(address, exactly_one=True)
            formatted_address = location.raw["formatted_address"]
            if formatted_address in expect_formatted_address:
                return True
            else:
                #             raise BaseApiKeyNotWorkingError
                sys.stderr.write("\nOutput is %r doesn't match %r!" %
                                 (formatted_address, expect_formatted_address))
                return False
    
    
    def test():
        GOOGLE_API_KEYS = [
            "AIzaSyAuzs8xdbysdYZO1wNV3vVw1AdzbL_Dnpk",  # Sanhe 01
            "AIzaSyBhO6ocH1qfg1zD-bJaptpHy5UWpZxL2iQ",  # Sanhe 02
            "AIzaSyDCNOTxBjrQn12K6XsRykRJCophmL0I91g",  # Sanhe 03
            "AIzaSyDkYgBX_Fi7Jop3IP3ZDMOHJqphCrYxuqs",  # Sanhe 04
            "AIzaSyDlTwtO17n1daw8FMTV_HM0hP4T1FnutyM",  # Sanhe 05
        ]
        
        apikey_pool = list()
        for key in GOOGLE_API_KEYS:
            apikey = GoogleBaseApiKey(key)
            apikey.setup_client()
            apikey_pool.append(apikey)
        
        api_manager = APIManager(apikey_pool=apikey_pool)
        api_manager.check_usable()
        
        # Put many address here
        task = [
            "3120 Kenni Ln Dunkirk, MD 20754",
        ]
        
        # Doing some real work
        for todo in task:
            # Fetch one key, if no key is usable an exception will be raised
            apikey = api_manager.fetch_one()
            try:
                # do geocoding
                location = apikey.client.geocode(task, exactly_one=True)
                if location:
                    pprint.pprint(location.raw)
                else:
                    pass
            except GeocoderQuotaExceeded:
                # this key is no longer usable
                api_manager.remove_one(apikey.key)
            except Exception as e:
                print(repr(e))
    
#     test()