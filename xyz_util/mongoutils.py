# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals
from functools import cached_property
import datetime, json
import os, re

from .datautils import access, import_function
from six import text_type
from bson import json_util, ObjectId
from bson.objectid import ObjectId

SERVER = os.getenv('MONGO_SERVER', 'localhost:27017')
if not SERVER.startswith('mongodb://'):
    SERVER = f'mongodb://{SERVER}'
CONN = SERVER.replace('mongodb://', '')
DB = os.getenv('MONGO_DB') or ('/' in CONN and CONN.split('/')[1]) or os.path.basename(os.getcwd())
TIMEOUT = 3000

USING_DJANGO = os.getenv('DJANGO_SETTINGS_MODULE')


if USING_DJANGO:
    from django.conf import settings
    if not DB:
        a = access(settings, 'DATABASES.default.NAME')
        if a:
            DB = a.split('/')[-1].split('.')[0]
    a = access(settings, 'MONGODB.SERVER')
    if a:
        SERVER = a
    a = access(settings, 'MONGODB.DB')
    if a:
        DB = a
    a = access(settings, 'MONGODB.TIMEOUT')
    if a:
        TIMEOUT = a

def loadMongoDB(server=SERVER, db=DB, timeout=TIMEOUT):
    import pymongo
    client = pymongo.MongoClient(server, serverSelectionTimeoutMS=timeout)
    return getattr(client, db)


LOADER = loadMongoDB
if USING_DJANGO:
    a = access(settings, 'MONGODB.LOADER')
    if a:
        LOADER = import_function(a)


def regex_contains(s):
    ns = ''
    for a in s:
        if a in '+.?()[]*':
            ns += '\\'
        ns += a
    return {'$regex': ns}


def django_order_field_to_mongo_sort(s):
    d = 1
    if s.startswith('-'):
        d = -1
        s = s[1:]
    return (s, d)


def mongo_id_value(id):
    if isinstance(id, ObjectId):
        return text_type(id)
    if isinstance(id, dict) and '$oid' in id:
        return id['$oid']

def date_format(date_field, format='%Y-%m-%d'):
    return {
        '$dateToString': { 'format': format, 'date': f"${date_field}" }
      }

def str_left(date_field, left=10):
    return {
        '$substr': [f"${date_field}", 0, left]
    }

class Store(object):
    name = 'test_mongo_store'
    timeout = TIMEOUT
    field_types = {}
    fields = None
    search_fields = []
    ordering = ('-id',)

    def __init__(self, server=SERVER, db=DB, name=None):
        self.db = LOADER(server, db, self.timeout)
        self.collection = getattr(self.db, name or self.name)
        if name:
            self.name = name

    @cached_property
    def _field_type_map(self):
        fts = all_fields_type_func(self._fields)
        if not self.field_types:
            return fts
        for ft, fns in self.field_types.items():
            for fn in fns:
                if isinstance(ft, str):
                    ft = filed_type_func(ft)
                fts[fn] = ft
        return fts

    @cached_property
    def _fields(self):
        fs = {}
        for d in self.random_find(count=10):
            fs.update(json_schema(d))
        if self.fields and isinstance(self.fields, dict):
            fs.update(self.fields)
        return fs

    def random_get(self, *args, **kwargs):
        rs = list(self.random_find(args[0], count=1, **kwargs))
        return rs[0] if rs else None

    def get(self, cond):
        if isinstance(cond, text_type):
            cond = {'_id': ObjectId(cond)}
        else:
            cond = self.normalize_filter(cond)
        return self.collection.find_one(cond)

    def get_or_create(self, cond, defaults={}):
        a = self.get(cond)
        if not a:
            d = {}
            d.update(cond)
            d.update(defaults)
            rs = self.collection.insert_one(d)
            a = self.get({'_id': rs.inserted_id})
        return a

    def random_find(self, cond={}, count=10, fields=None):
        cond = self.normalize_filter(cond)
        fs = [{'$match': cond}, {'$sample': {'size': count}}]
        if fields:
            fs.append({'$project': fields})
        return self.collection.aggregate(fs)

    def find(self, filter=None, projection=None, **kwargs):
        filter = self.normalize_filter(filter)
        if self.ordering and 'sort' not in kwargs:
            kwargs['sort'] = [django_order_field_to_mongo_sort(s) for s in self.ordering]
        rs = self.collection.find(filter, projection,  **kwargs)
        if not hasattr(rs, 'count'):
            setattr(rs, 'count', lambda: self.count(filter))
        return rs

    def search(self, cond, *args, **kwargs):
        # cond = self.normalize_filter(cond)
        return self.find(cond, *args, **kwargs)

    def upsert(self, cond, value, **kwargs):
        d = {'$set': value}
        for k, v in kwargs.items():
            d['$%s' % k] = v
        return self.collection.update_one(cond, d, upsert=True)

    def batch_upsert(self, data_list, key='id', preset=lambda a, i: a, **kwargs):
        i = -1
        for i, d in enumerate(data_list):
            if isinstance(d, tuple):
                d = d[-1]
            d = preset(d, i) or d
            print(d[key])
            self.upsert({key: d[key]}, d, **kwargs)
        return i + 1

    def update(self, cond, value, **kwargs):
        d = {}
        if value:
            d['$set'] = value
        for k, v in kwargs.items():
            d['$%s' % k] = v
        return self.collection.update_many(cond, d)

    def inc(self, cond, value):
        self.collection.update_many(cond, {'$inc': value}, upsert=True)

    def add_to_set(self, cond, value):
        self.collection.update_many(cond, {'$addToSet': value}, upsert=True)

    def count(self, filter=None, distinct=False):
        filter = self.normalize_filter(filter)
        if distinct:
            gs = []
            if filter:
                gs.append({'$match': filter})
            gs.append({'$group': {'_id': '$%s' % distinct}}),
            gs.append({'$group': {'_id': 0, 'count': {'$sum': 1}}})
            for a in self.collection.aggregate(gs):
                return a['count']
            return 0
        if not filter:
            return self.collection.estimated_document_count()
        return self.collection.count_documents(filter)

    def sum(self, field, filter=None):
        filter = self.normalize_filter(filter)
        gs = []
        if filter:
            gs.append({'$match': filter})
        gs.append({'$group': {'_id': 0, 'result': {'$sum': '$%s' % field}}})
        for a in self.collection.aggregate(gs):
            return a['result']

    def count_by(self, field, filter=None, output='array', unwind=False):
        filter = self.normalize_filter(filter)
        ps = []
        if filter:
            ps.append({'$match': filter})
        if unwind:
            ps.append({'$unwind': '$%s' % field})
        exp = '$%s' % field if isinstance(field, str) else field
        ps.append({'$group': {'_id': exp, 'count': {'$sum': 1}}})
        rs = self.collection.aggregate(ps)
        if output == 'dict':
            rs = dict([(a['_id'], a['count']) for a in rs])
        return rs

    def group_by(self, field, aggregate={'count': {'$sum': 1}}, filter=None, unwind=False, prepare=[]):
        filter = self.normalize_filter(filter)
        ps = []+ prepare
        if filter:
            ps.append({'$match': filter})
        if unwind:
            ps.append({'$unwind': '$%s' % field})
        exp = '$%s' % field if isinstance(field, str) else field
        d = {'_id': exp}
        d.update(aggregate)
        ps.append({'$group': d})
        rs = self.collection.aggregate(ps)
        return rs

    def clean_data(self, data):
        d = {}
        for a in data.keys():
            if self._fields and a not in self._fields:
                continue
            d[a] = data[a]

        for t, fs in self.field_types.items():
            for f in fs:
                if f in d:
                    d[f] = t(d[f])
        return d

    def normalize_filter(self, data):
        if not data:
            return data
        return normalize_filter_condition(data, self._field_type_map, self._fields , self.search_fields)

    def create_index(self):
        for i in self.keys:
            self.collection.create_index([(i, 1)])

    def eval_foreign_keys(self, d, foreign_keys=None):
        fks = foreign_keys or getattr(self, 'foreign_keys', None)
        if not fks:
            return d
        for kn, sn in fks.items():
            if kn not in d:
                continue
            id = mongo_id_value(d[kn])
            if not id:
                continue
            d[kn] = Store(name=sn).get(id)
        return d


def ensure_list(a):
    if isinstance(a, str):
        return a.split(',')
    return a
    

def normalize_filter_condition(data, field_types={}, fields=None, search_fields=[]):
    d = {}
    if search_fields:
        sv = data.get('search')
        if sv:
            v = {'$regex': sv}
            for fn in search_fields:
                d = {'$or': [d, {fn: v}]} if d else {fn: v}

    mm = {
        'exists': lambda v: {'$exists': v not in ['0', 'false', False]},
        'isnull': lambda v: {'$ne' if v in ['0', 'false', ''] else '$eq': None},
        'regex': lambda v: {'$regex': v},
        'in': lambda v: {'$in': ensure_list(v)},
        'all': lambda v: {'$all': ensure_list(v)},
        'gt': lambda v: {'$gt': v},
        'lt': lambda v: {'$lt': v},
        'size': lambda v: {'$size': v},
        'gte': lambda v: {'$gte': v},
        'lte': lambda v: {'$lte': v},
    }
    for a in data.keys():
        if a == 'search':
            continue
        v = data[a]
        ps = a.split('__')
        if len(ps)>1:
            mn = ps[-1]
            mf = mm.get(mn)
            if mf:
                sl = len(mn)+2
                a = a[:-sl]
                a = a.replace('__', '.')
                if isinstance(v, str):
                    format_func = field_types.get(a)
                    if format_func:
                        v = format_func(v)
                v = mf(v)

        # for mn, mf in mm.items():
        #     ms = f'__{mn}'
        #     if a.endswith(ms):
        #         sl = len(ms)
        #         a = a[:-sl]
        #         format_func = field_types.get(a)
        #         if format_func:
        #             v = format_func(v)
        #         v = mf(v)
        #         break
        # if fields:
        #     ps = re.split(r'__|\.', a) ##a.split('__')
        #     # if ps[0] not in fields:
        #     #     continue
        #     a = ".".join(ps)
        a = a.replace('__', '.')
        format_func = field_types.get(a)
        expr = format_func(v) if not isinstance(v, dict) and format_func else v
        if a in d and isinstance(d[a], dict):
            d[a].update(expr)
        else:
            d[a] = expr

    # for t, fs in field_types.items():
    #     for f in fs:
    #         if f in d and not isinstance(d[f], dict):
    #             d[f] = t(d[f])
    # print(d)
    return d

def json_schema(d, prefix=''):
    import bson
    tm = {
        int: 'integer',
        bson.int64.Int64: 'integer',
        ObjectId: 'oid',
        float: 'number',
        bool: 'boolean',
        list: 'array',
        text_type: 'string',
        type(None): 'null',
        dict: 'object',
        datetime.datetime: 'datetime'
    }
    r = {}
    for k, v in d.items():
        t = tm[type(v)]
        fn = '%s%s' % (prefix, k)
        r[fn] = t
        if t == 'object':
            r.update(json_schema(v, prefix='%s.' % fn))
    return r


def filed_type_func(f):
    return {
        'integer': int,
        'number': float,
        'datetime': datetime.datetime.fromisoformat,
        'date': datetime.datetime.fromisoformat,
        'object': json.loads,
        'oid': ObjectId
    }.get(f, text_type)

def all_fields_type_func(fs):
    return dict([(fn, filed_type_func(ft)) for fn, ft in fs.items()])

def drop_id_field(c):
    for a in c:
        a.pop('_id', None)
        yield a


class Schema(Store):
    name = 'XYZ_STORE_SCHEMA'

    def guess(self, name, *args, **kwargs):
        st = Store(name=name)
        rs = {}
        for d in st.random_find(*args, **kwargs):
            rs.update(json_schema(d))
        self.upsert({'name': name}, {'guess': rs})
        return rs

    def desc(self, name, *args, **kwargs):
        d = self.collection.find_one({'name': name}, {'_id': 0})
        if not d or not d.get('guess'):
            self.guess(name, *args, **kwargs)
            d = self.collection.find_one({'name': name}, {'_id': 0})
        return d


if USING_DJANGO:
    from django.utils.functional import cached_property
    from django.core.paginator import Paginator
    from django.dispatch import Signal

    from rest_framework.pagination import PageNumberPagination
    from rest_framework import permissions, exceptions
    from rest_framework import viewsets, response, serializers, fields

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




    def get_paginated_response(view, query, wrap=lambda a: a):
        pager = MongoPageNumberPagination()
        ds = pager.paginate_queryset(query, view.request, view=view)
        rs = [wrap(a) for a in ds]
        return pager.get_paginated_response(json_util._json_convert(rs))


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




    class MongoSerializer(serializers.ModelSerializer):

        def get_fields(self):
            assert hasattr(self, 'Meta'), (
                'Class {serializer_class} missing "Meta" attribute'.format(
                    serializer_class=self.__class__.__name__
                )
            )
            assert hasattr(self.Meta, 'store'), (
                'Class {serializer_class} missing "Meta.store" attribute'.format(
                    serializer_class=self.__class__.__name__
                )
            )
            schema = Schema()
            store = self.Meta.store
            rs = {}
            fm = {'string': fields.CharField,
                  'integer': fields.IntegerField,
                  'number': fields.FloatField,
                  'array': fields.ListField,
                  'object': fields.JSONField
                  }
            for fn, ft in schema.desc(store.name).items():
                field = fm[ft]()
                rs[fn] = field
            return rs


    mongo_posted = Signal(providing_args=['table', 'instance', 'created', 'update'])

    class MongoViewSet(viewsets.ViewSet):
        permission_classes = [permissions.IsAdminUser]
        store_name = None
        store_class = None

        def dispatch(self, request, *args, **kwargs):
            self.store = self.get_store()
            return super(MongoViewSet, self).dispatch(request, *args, **kwargs)

        def get_store(self, name=None):
            if name:
                return Store(name=name)
            if self.store_class:
                return self.store_class()
            elif self.store_name:
                return Store(name=self.store_name)
            raise exceptions.NotFound()

        def get_foreign_key(self, store_name, id):
            st = self.get_store(store_name)
            return st.collection.get(id=id)

        def options(self, request, *args, **kwargs):
            # print(self.metadata_class)
            # return super(MongoViewSet, self).options(request, *args, **kwargs)
            sc = Schema().desc(self.get_store().name)
            return response.Response(sc)

        def get_serialize_fields(self):
            return None

        def filter_query(self, cond):
            return cond

        def list(self, request):
            # print(request.query_params)
            qps = request.query_params
            cond = self.store.normalize_filter(qps)
            # print(cond)
            cond = self.filter_query(cond)
            randc = qps.get('_random')
            ordering = qps.get('ordering')
            kwargs = {}
            if ordering:
                kwargs['sort'] = [django_order_field_to_mongo_sort(ordering)]
            if randc:
                rs = self.store.random_find(cond, count=int(randc), fields=self.get_serialize_fields())
                return response.Response(dict(results=json_util._json_convert(rs)))
            rs = self.store.find(cond, self.get_serialize_fields(), **kwargs)
            return get_paginated_response(self, rs, wrap=self.eval_foreign_keys)

        def eval_foreign_keys(self, d):
            fks = getattr(self, 'foreign_keys', None)
            return self.store.eval_foreign_keys(d, foreign_keys=fks)


        def get_object(self, id=None):
            _id = id if id else self.kwargs['pk']
            cond = {'_id': ObjectId(_id)}
            return json_util._json_convert(self.eval_foreign_keys(self.store.collection.find_one(cond, None)))

        def retrieve(self, request, pk):
            return response.Response(self.get_object())

        def get_serialized_data(self):
            d = {}
            d.update(self.request.data)
            return d

        def update(self, request, pk, *args, **kargs):
            instance = self.get_object()
            data = self.get_serialized_data()
            # print(data)
            self.store.update({'_id': ObjectId(pk)}, data)
            new_instance = self.get_object()
            mongo_posted.send_robust(sender=type(self), instance=new_instance, update=data, created=False)
            return response.Response(json_util._json_convert(new_instance))

        def create(self, request, *args, **kargs):
            data = self.get_serialized_data()
            r = self.store.collection.insert_one(data)
            return response.Response(self.get_object(r.inserted_id))

        def patch(self, request, pk, *args, **kargs):
            return self.update(request, pk, *args, **kargs)




    #
    # from rest_framework.metadata import SimpleMetadata
    #
    #
    # class RestMetadata(SimpleMetadata):
    #     def determine_actions(self, request, view):
    #         actions = super(RestMetadata, self).determine_actions(request, view)
    #         view.request = clone_request(request, 'GET')
    #         try:
    #             # Test global permissions
    #             if hasattr(view, 'check_permissions'):
    #                 view.check_permissions(view.request)
    #         except (exceptions.APIException, PermissionDenied, Http404):
    #             pass
    #         else:
    #
    #             search_fields = getattr(view, 'search_fields', [])
    #             cf = lambda f: f[0] in ['^', '@', '='] and f[1:] or f
    #             actions['SEARCH'] = search = {}
    #             search['search_fields'] = [get_related_field_verbose_name(view.queryset.model, cf(f)) for f in
    #                                        search_fields]
    #             ffs = access(view, 'filter_class._meta.fields') or getattr(view, 'filter_fields', [])
    #             if isinstance(ffs, dict):
    #                 search['filter_fields'] = [{'name': k, 'lookups': v} for k, v in ffs.items()]
    #             else:
    #                 search['filter_fields'] = [{'name': a, 'lookups': 'exact'} for a in ffs]
    #             search['ordering_fields'] = getattr(view, 'ordering_fields', [])
    #             serializer = view.get_serializer()
    #             actions['LIST'] = self.get_list_info(serializer)
    #         finally:
    #             view.request = request
    #         return actions
