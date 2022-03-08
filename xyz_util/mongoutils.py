# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals

from django.conf import settings
from django.utils.functional import cached_property
from rest_framework.pagination import PageNumberPagination

from .datautils import access, import_function
import random
from django.core.paginator import Paginator

DEFAULT_DB = {
    'SERVER': 'mongodb://localhost:27017/',
    'DB': access(settings, 'DATABASES.default.NAME')
}
CONF = getattr(settings, 'MONGODB', DEFAULT_DB)


def loadMongoDB(server=None, db=None, timeout=3000):
    import pymongo
    client = pymongo.MongoClient(server or CONF['SERVER'], serverSelectionTimeoutMS=timeout)
    return getattr(client, db or CONF.get('DB', DEFAULT_DB['DB']))


LOADER = import_function(CONF.get('LOADER', 'xyz_util.mongoutils:loadMongoDB'))

class Store(object):
    name = 'test_mongo_store'
    timeout = CONF.get('TIMEOUT', 3000)

    def __init__(self, server=None, db=None, name=None):
        self.db = LOADER(server, db, self.timeout)
        self.collection = getattr(self.db, name or self.name)

    def random_get(self, *args, **kwargs):
        cursor = self.collection.find(*args, **kwargs)
        count = cursor.count()
        if count == 0:
            return
        p = random.randint(0, count - 1)
        cursor.skip(p)
        r = cursor.next()
        cursor.close()
        return r

    def random_find(self, cond={}, count=10):
        return self.collection.aggregate([{'$match': cond}, {'$sample': {'size': count}}])

    def find(self, *args, **kwargs):
        return self.collection.find(*args, **kwargs)

    def upsert(self, cond, value):
        self.collection.update(cond, {'$set': value}, upsert=True)

    def update(self, cond, value, **kwargs):
        self.collection.update(cond, {'$set': value}, **kwargs)

    def inc(self, cond, value):
        self.collection.update(cond, {'$inc': value}, upsert=True)

    def add_to_set(self, cond, value):
        self.collection.update(cond, {'$addToSet': value}, upsert=True)

    def count(self, filter=None, distinct=False):
        if distinct:
            gs = []
            if filter:
                gs.append({'$match': filter})
            gs.append({'$group': {'_id': '$%s' % distinct}}),
            gs.append({'$group': {'_id': 0, 'count': {'$sum': 1}}})
            for a in self.collection.aggregate(gs):
                return a['count']
            return 0
        return self.collection.count(filter)

    def sum(self, field, filter=None):
        gs = []
        if filter:
            gs.append({'$match': filter})
        gs.append({'$group': {'_id': 0, 'result': {'$sum': '$%s' % field}}})
        for a in self.collection.aggregate(gs):
            return a['result']

    def count_by(self, field):
        return self.collection.aggregate([{'$group': {'_id': '$%s' % field, 'count': {'$sum': 1}}}])


class MongoPaginator(Paginator):

    @cached_property
    def count(self):
        # print('count')
        return self.object_list.count()


class MongoPageNumberPagination(PageNumberPagination):
    django_paginator_class = MongoPaginator
    page_size = 100
    page_size_query_param = 'page_size'
    max_page_size = 1000

def drop_id_field(c):
    for a in c:
        a.pop('_id')
        yield a