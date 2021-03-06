#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
extend the power of pymongo.
"""

import math
import pymongo


def grouper_list(l, n):
    """Evenly divide list into fixed-length piece, no filled value if chunk
    size smaller than fixed-length.

    Example::

        >>> list(grouper(range(10), n=3)
        [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]

    **中文文档**

    将一个列表按照尺寸n, 依次打包输出, 有多少输出多少, 并不强制填充包的大小到n。

    下列实现是按照性能从高到低进行排列的:

    - 方法1: 建立一个counter, 在向chunk中添加元素时, 同时将counter与n比较, 如果一致
      则yield。然后在最后将剩余的item视情况yield。
    - 方法2: 建立一个list, 每次添加一个元素, 并检查size。
    - 方法3: 调用grouper()函数, 然后对里面的None元素进行清理。
    """
    chunk = list()
    counter = 0
    for item in l:
        counter += 1
        chunk.append(item)
        if counter == n:
            yield chunk
            chunk = list()
            counter = 0
    if len(chunk) > 0:
        yield chunk


def smart_insert(col, data, minimal_size=5):
    """An optimized Insert strategy.

    **中文文档**

    在Insert中, 如果已经预知不会出现IntegrityError, 那么使用Bulk Insert的速度要
    远远快于逐条Insert。而如果无法预知, 那么我们采用如下策略:

    1. 尝试Bulk Insert, Bulk Insert由于在结束前不Commit, 所以速度很快。
    2. 如果失败了, 那么对数据的条数开平方根, 进行分包, 然后对每个包重复该逻辑。
    3. 若还是尝试失败, 则继续分包, 当分包的大小小于一定数量时, 则使用逐条插入。
      直到成功为止。

    该Insert策略在内存上需要额外的 sqrt(nbytes) 的开销, 跟原数据相比体积很小。
    但时间上是各种情况下平均最优的。
    """
    if isinstance(data, list):
        # 首先进行尝试bulk insert
        try:
            col.insert(data)
        # 失败了
        except pymongo.errors.DuplicateKeyError:
            # 分析数据量
            n = len(data)
            # 如果数据条数多于一定数量
            if n >= minimal_size ** 2:
                # 则进行分包
                n_chunk = math.floor(math.sqrt(n))
                for chunk in grouper_list(data, n_chunk):
                    smart_insert(col, chunk, minimal_size)
            # 否则则一条条地逐条插入
            else:
                for doc in data:
                    try:
                        col.insert(doc)
                    except pymongo.errors.DuplicateKeyError:
                        pass
    else:
        try:
            col.insert(data)
        except pymongo.errors.DuplicateKeyError:
            pass


def select_all(col):
    return list(col.find())


def selelct_field(col, *fields):
    """Select single or multiple fields.
    
    :params fields: list of str
    :returns headers: headers
    :return data: list of row
    
    **中文文档**
    
    - 在选择单列时, 返回的是 str, list
    - 在选择多列时, 返回的是 str list, list of list
    
    返回单列或多列的数据。
    """
    if len(fields) == 1:
        header = fields[0]
        data = [doc.get(header) for doc in col.find()]
        return header, data
    else:
        headers = fields
        data = [[doc.get(header) for header in headers] for doc in col.find()]
        return headers, data


def select_distinct_field(col, *columns):
    if len(columns) == 1:
        key = columns[0]
        data = list(col.find({}).distinct(key))
        return data
    else:
        pipeline = [
            {
                "$group": {
                    "_id": {key: "$" + key for key in columns},
                },
            },
        ]
        data = list()
        for doc in col.aggregate(pipeline):
            # doc = {"_id": {"a": 0, "b": 0}} ...
            data.append([doc["_id"][key] for key in columns])
        return data


#--- Unittest ---
if __name__ == "__main__":
    import time
    import random
    from sfm.decorator import run_if_is_main

    client = pymongo.MongoClient()
    db = client.get_database("test")
    col = db.get_collection("test")

    @run_if_is_main(__name__)
    def test_smart_insert():
        def insert_test_data():
            col.remove({})

            data = [{"_id": random.randint(1, 10000)} for i in range(20)]
            for doc in data:
                try:
                    col.insert(doc)
                except:
                    pass
            assert 15 <= col.find().count() <= 20
        data = [{"_id": i} for i in range(1, 1 + 10000)]
        # Smart Insert
        insert_test_data()

        st = time.clock()
        smart_insert(col, data)
        elapse1 = time.clock() - st

        # after smart insert, we got 10000 doc
        assert col.find().count() == 10000

        # Regular Insert
        insert_test_data()

        st = time.clock()
        for doc in data:
            try:
                col.insert(doc)
            except:
                pass
        elapse2 = time.clock() - st

        # after regular insert, we got 10000 doc
        assert col.find().count() == 10000

        assert elapse1 <= elapse2

    test_smart_insert()

    @run_if_is_main(__name__)
    def test_selelct_field():
        def insert_test_data():
            col.remove({})

            data = [{"_id": i, "a": i} for i in range(10)]

            col.insert(data)

        insert_test_data()

        header, data = selelct_field(col, "_id")
        assert data == list(range(10))

        headers, data = selelct_field(col, "_id", "a")
        assert data == [[i, i] for i in range(10)]

    test_selelct_field()

    @run_if_is_main(__name__)
    def test_select_distinct():
        def insert_test_data():
            col.remove({})

            distinct_complexity = 10
            data = list()
            for i in range(distinct_complexity):
                for j in range(distinct_complexity):
                    for k in range(distinct_complexity):
                        data.append({"a": i, "b": j, "c": k})
            col.insert(data)

        insert_test_data()

        assert select_distinct_field(col, "a") == list(range(10))
        assert len((select_distinct_field(col, "a", "b"))) == 100
        assert len((select_distinct_field(col, "a", "b", "c"))) == 1000

    test_select_distinct()
