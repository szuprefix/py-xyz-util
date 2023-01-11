# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals

from django.conf import settings
from django.utils.functional import cached_property
from rest_framework.pagination import PageNumberPagination
from rest_framework import permissions, exceptions
from .datautils import access, import_function
from django.core.paginator import Paginator
from six import text_type
from bson import json_util
from bson.objectid import ObjectId

DEFAULT_DB = {
    'SERVER': 'mongodb://localhost:27017/',
    'DB': access(settings, 'DATABASES.default.NAME').split('/')[-1].split('.')[0]
}
CONF = getattr(settings, 'MONGODB', DEFAULT_DB)


def loadMongoDB(server=None, db=None, timeout=3000):
    import pymongo
    client = pymongo.MongoClient(server or CONF['SERVER'], serverSelectionTimeoutMS=timeout)
    return getattr(client, db or CONF.get('DB', DEFAULT_DB['DB']))


LOADER = import_function(CONF.get('LOADER', 'xyz_util.mongoutils:loadMongoDB'))


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


class Store(object):
    name = 'test_mongo_store'
    timeout = CONF.get('TIMEOUT', 3000)
    field_types = {}
    fields = None
    search_fields = []
    ordering = ('-id',)

    def __init__(self, server=None, db=None, name=None):
        self.db = LOADER(server, db, self.timeout)
        self.collection = getattr(self.db, name or self.name)
        if name:
            self.name = name

    def random_get(self, *args, **kwargs):
        rs = list(self.random_find(args[0], count=1, **kwargs))
        return rs[0] if rs else None

    def random_find(self, cond={}, count=10, fields=None):
        fs = [{'$match': cond}, {'$sample': {'size': count}}]
        if fields:
            fs.append({'$project': fields})
        return self.collection.aggregate(fs)

    def find(self, *args, **kwargs):
        if self.ordering and 'sort' not in kwargs:
            kwargs['sort'] = [django_order_field_to_mongo_sort(s) for s in self.ordering]
        return self.collection.find(*args, **kwargs)

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

    def count_by(self, field, filter=None, output='array', unwind=False):
        ps = []
        if filter:
            ps.append({'$match': filter})
        if unwind:
            ps.append({'$unwind': '$%s' % field})
        ps.append({'$group': {'_id': '$%s' % field, 'count': {'$sum': 1}}})
        rs = self.collection.aggregate(ps)
        if output == 'dict':
            rs = dict([(a['_id'], a['count']) for a in rs])
        return rs

    def group_by(self, field, aggregate={'count': {'$sum': 1}}, filter=None, output='array', unwind=False):
        ps = []
        if filter:
            ps.append({'$match': filter})
        if unwind:
            ps.append({'$unwind': '$%s' % field})
        d = {'_id': '$%s' % field}
        d.update(aggregate)
        ps.append({'$group': d})
        rs = self.collection.aggregate(ps)
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

    def normalize_filter(self, data):
        fs = self.fields
        if not fs:
            sc = Schema().desc(self.name)
            fs = sc.get('guess')
        # print(fs)
        return normalize_filter_condition(data, self.field_types, fs, self.search_fields)

    def create_index(self):
        for i in self.keys:
            self.collection.create_index([(i, 1)])


def normalize_filter_condition(data, field_types={}, fields=None, search_fields=[]):
    d = {}
    if search_fields:
        sv = data.get('search')
        if sv:
            v = {'$regex': sv}
            for fn in search_fields:
                d = {'$or': [d, {fn: v}]} if d else {fn: v}

    mm = {
        'exists': lambda v: {'$exists': v not in ['0', 'false']},
        'isnull': lambda v: {'$ne' if v in ['0', 'false', ''] else '$eq': None},
        'regex': lambda v: {'$regex': v},
        'in': lambda v: {'$in': v.split(',')},
        'all': lambda v: {'$all': v.split(',')}
    }
    for a in data.keys():
        if a == 'search':
            continue
        v = data[a]
        for mn, mf in mm.items():
            ms = '__%s' % mn
            if a.endswith(ms):
                sl = len(ms)
                v = mf(v)
                a = a[:-sl]
                break
        if fields:
            ps = a.split('__')
            if ps[0] not in fields:
                continue
            a = ".".join(ps)
        d[a] = v

    for t, fs in field_types.items():
        for f in fs:
            if f in d and not isinstance(d[f], dict):
                d[f] = t(d[f])
    # print(d)
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
        a.pop('_id', None)
        yield a


def get_paginated_response(view, query):
    pager = MongoPageNumberPagination()
    ds = pager.paginate_queryset(query, view.request, view=view)
    rs = list(ds)
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


from rest_framework import viewsets, response, serializers, fields


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


class MongoViewSet(viewsets.ViewSet):
    permission_classes = [permissions.IsAdminUser]
    store_name = None
    store_class = None

    def dispatch(self, request, *args, **kwargs): 
        self.store = self.get_store()
        return super(MongoViewSet, self).dispatch(request,  *args, **kwargs)

    def get_store(self):
        if self.store_class:
            return self.store_class()
        elif self.store_name:
            return Store(name=self.store_name)
        raise exceptions.NotFound()

    def options(self, request, *args, **kwargs):
        # print(self.metadata_class)
        # return super(MongoViewSet, self).options(request, *args, **kwargs)
        sc = Schema().desc(self.get_store().name)
        return response.Response(sc)

    def list(self, request):
        # print(request.query_params)
        qps = request.query_params
        cond = self.store.normalize_filter(qps)
        # print(cond)
        randc = qps.get('_random')
        ordering = qps.get('ordering')
        kwargs = {}
        if ordering:
            kwargs['sort'] = [django_order_field_to_mongo_sort(ordering)]
        if randc:
            rs = self.store.random_find(cond, count=int(randc))
            return response.Response(dict(results=json_util._json_convert(rs)))
        rs = self.store.find(cond, None, **kwargs)
        return get_paginated_response(self, rs)

    def get_object(self, id=None):
        _id = id if id else self.kwargs['pk']
        cond = {'_id': ObjectId(_id)}
        return json_util._json_convert(self.store.collection.find_one(cond, None))

    def retrieve(self, request, pk):
        return response.Response(self.get_object())

    def get_serialized_data(self):
        return self.request.data

    def update(self, request, pk, *args, **kargs):
        instance = self.get_object()
        data = self.get_serialized_data()
        # print(data)
        self.store.update({'_id': ObjectId(pk)}, data)
        return response.Response(self.get_object())

    def create(self, request, *args, **kargs):
        data = self.get_serialized_data()
        r = self.store.collection.insert_one(data)
        return response.Response(self.get_object(r.inserted_id))

    def patch(self, request, pk, *args, **kargs):
        return self.update(request, pk, *args, **kargs)


def json_schema(d, prefix=''):
    import bson
    tm = {
        int: 'integer',
        bson.int64.Int64: 'integer',
        bson.objectid.ObjectId: 'string',
        float: 'number',
        bool: 'boolean',
        list: 'array',
        text_type: 'string',
        type(None): 'null',
        dict: 'object'
    }
    r = {}
    for k, v in d.items():
        t = tm[type(v)]
        fn = '%s%s' % (prefix, k)
        r[fn] = t
        if t == 'object':
            r.update(json_schema(v, prefix='%s.' % fn))
    return r


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
        if not d:
            self.guess(name, *args, **kwargs)
            d = self.collection.find_one({'name': name}, {'_id': 0})
        return d


from rest_framework.metadata import SimpleMetadata
class RestMetadata(SimpleMetadata):
    def determine_actions(self, request, view):
        actions = super(RestMetadata, self).determine_actions(request, view)
        view.request = clone_request(request, 'GET')
        try:
            # Test global permissions
            if hasattr(view, 'check_permissions'):
                view.check_permissions(view.request)
        except (exceptions.APIException, PermissionDenied, Http404):
            pass
        else:

            search_fields = getattr(view, 'search_fields', [])
            cf = lambda f: f[0] in ['^', '@', '='] and f[1:] or f
            actions['SEARCH'] = search = {}
            search['search_fields'] = [get_related_field_verbose_name(view.queryset.model, cf(f)) for f in
                                       search_fields]
            ffs = access(view, 'filter_class._meta.fields') or getattr(view, 'filter_fields', [])
            if isinstance(ffs, dict):
                search['filter_fields'] = [{'name': k, 'lookups': v} for k, v in ffs.items()]
            else:
                search['filter_fields'] = [{'name': a, 'lookups': 'exact'} for a in ffs]
            search['ordering_fields'] = getattr(view, 'ordering_fields', [])
            serializer = view.get_serializer()
            actions['LIST'] = self.get_list_info(serializer)
        finally:
            view.request = request
        return actions


