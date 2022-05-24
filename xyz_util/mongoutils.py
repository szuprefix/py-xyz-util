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
    field_types = {}
    fields = None

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
        self.collection.update_one(cond, {'$set': value}, upsert=True)

    def update(self, cond, value, **kwargs):
        self.collection.update_many(cond, {'$set': value}, **kwargs)

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

    def count_by(self, field, filter=None, output='array'):
        ps = []
        if filter:
            ps.append({'$match': filter})
        ps.append({'$group': {'_id': '$%s' % field, 'count': {'$sum': 1}}})
        rs = self.collection.aggregate(ps)
        if output == 'dict':
            rs = dict([(a['_id'], a['count'])for a in rs])
        return rs

    def clean_data(self, data):
        d = {}
        for a in data.keys():
            if self.fields and a not in self.fields:
                continue
            d[a] = data[a]

        for t, fs in self.field_types.items():
            for f in fs:
                if f in d:
                    d[f] = t(d[f])
        return d


def normalize_filter_condition(data, field_types={}, fields=None):
    d = {}
    for a in data.keys():
        v = data[a]
        if a.endswith('__exists'):
            sl = len('__exists')
            v = {'$exists': int(data[a])}
            a = a[:-sl]
        if fields and a not in fields:
            continue
        d[a] = v

    for t, fs in field_types.items():
        for f in fs:
            if f in d and not isinstance(d[f], dict):
                d[f] = t(d[f])
    return d


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

def get_paginated_response(view, query):
    pager = MongoPageNumberPagination()
    ds = pager.paginate_queryset(query, view.request, view=view)
    return pager.get_paginated_response(list(drop_id_field(ds)))


def model_get_and_patch(view, default={}, field_names=None):
    from rest_framework.response import Response
    a = view.get_object()
    tn = a._meta.label_lower.replace('.', '_')
    st = Store(name=tn)
    fns = field_names or [view.action]
    if view.request.method == 'GET':
        fd = {'_id': 0}
        for fn in fns:
            fd[fn] = 1
        d = st.collection.find_one({'id': a.id}, fd)
        return Response(d)
    else:
        rd = view.request.data
        pd = {}
        for k in rd:
            if k.split('.')[0] in fns:
                pd[k] = rd[k]
        d = st.upsert({'id': a.id}, pd)
        return Response(d)
