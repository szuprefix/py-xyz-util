# -*- coding:utf-8 -*-
from __future__ import unicode_literals
from collections import OrderedDict

from django.db.models import Count
from django.conf import settings
from six import text_type, string_types
from . import modelutils, datautils, dateutils

__author__ = 'denishuang'

DB_FOR_STATS = getattr(settings, 'DB_FOR_STATS', None)


def using_stats_db(qset):
    if DB_FOR_STATS:
        qset = qset.using(DB_FOR_STATS)
    return qset


class StructorStat(object):
    def __init__(self, query_set, fields):
        self.query_set = query_set
        self.fields = fields
        self.model = self.query_set.model

    def stat(self):
        r = OrderedDict()
        for fn in self.fields:
            field = self.model._meta.get_field(fn)
            vname = field.verbose_name
            choices = dict(field.choices)
            data = self.query_set.values(fn).order_by(fn).annotate(count=Count("id"))
            d = OrderedDict()
            for g in data:
                k = g[fn]
                k = choices.get(k, k)
                v = g["count"]
                d[k] = v
            r[vname] = d
        return r


class TimeStat(object):
    def __init__(self, query_set, timeField):
        self.query_set = query_set
        self.timeField = timeField
        self.model = self.query_set.model

    def get_step(self, beginTime, endTime):
        dt = endTime - beginTime
        print(dt.seconds, dt.days)

        if dt.days > 7:
            return 3600 * 24
        elif dt.days >= 1:
            return 3600
        elif dt.seconds <= 36000:
            return 60
        else:
            return 3600

    def stat(self, dateRange="今天", funcMap={}):
        r = OrderedDict()
        tfn = "time"
        beginTime, endTime = dateutils.get_period_by_name(dateRange)
        step = self.get_step(beginTime, endTime)
        print(beginTime, endTime, step)
        fn = self.timeField
        qset = self.query_set.filter(**{"%s__gte" % fn: beginTime, "%s__lt" % fn: endTime})
        time_field = {tfn: "floor(unix_timestamp(%s)/%d)*%d" % (fn, step, step)}
        if not funcMap:
            funcMap = {"id_count": Count("id")}
        for k, v in funcMap.items():
            qset = qset.extra(time_field).values(tfn).order_by(tfn).annotate(count=v).values_list(tfn, "count")
            data = list(qset)  # [[a.strftime("%Y/%m/%d %H:%M:%S"), b] for a, b in qset]
            r[k] = data
        return r

def extract_order_field(sort):
    if not sort:
        return '', 0
    direction = '-' if sort[0] == '-' else ''
    field_num = sort.replace('-', '')
    field_num = int(field_num) if field_num else 0
    return direction, field_num

def group_by(qset, group, measures=None, sort=None, limit=None, group_map=None, group_maps=None):
    if isinstance(group, string_types):
        group = [g.strip() for g in group.split(',') if g.strip()]
    if not measures:
        measures = [Count('id')]

    qset = qset.values(*group).order_by(*group)
    from collections import OrderedDict
    mm = OrderedDict()
    for i, m in enumerate(measures):
        fn = "f%d" % i
        mm[fn] = m
    dl = qset.annotate(**mm)
    if sort is not None:
        dl = dl.order_by("%sf%s" % extract_order_field(sort))
    if limit:
        dl = dl[:limit]
    fs = group + list(mm.keys())
    rs = [[d[f] for f in fs] for d in dl]

    gms = {group[0]: group_map} if group_map else group_maps
    if gms:
        for gn, gm in gms.items():
            if gm:
                if isinstance(gm, (list, tuple)):
                    gm = dict(gm)
                for d in rs:
                    d[0] = gm.get(d[0], d[0])
    return rs


def count_by(qset, group, count_field='id', distinct=False, **kwargs):
    return group_by(qset, group, measures=[Count(count_field, distinct=distinct)], **kwargs)

def rank_by(qset, group, measure=None, top=10):
    if not measure:
        measure = Count('id')
    order_field = "f"
    if top<0:
        top = -top
        order_field = "-f"
    rs = qset.values(group).order_by(group).annotate(f=measure).order_by(order_field)[:top+1]
    d = OrderedDict()
    for a in rs:
        d[a[group]] = a['f']
    return d


def count_by_generic_relation(qset, group, count_field='id', distinct=False, sort=None):
    rfs = modelutils.get_related_fields(qset, group)
    if rfs:
        f = rfs[-2]
        if isinstance(f, dict):
            ct = f['content_type']
            gfk = f['field']
            prefix = "__".join([a.name for a in rfs[:-2]])
            if prefix:
                prefix += "__"
            cts = prefix + gfk.ct_field
            group = prefix + gfk.fk_field
            cond = {cts: ct.id}
            qset = qset.filter(**cond)
            rs = count_by(qset, group, count_field=count_field, distinct=distinct, sort=sort)
            model = ct.model_class()
            ids = [a[0] for a in rs]
            nm = dict(model.objects.filter(id__in=ids).values_list('id', rfs[-1].name))
            for d in rs:
                d[0] = nm.get(d[0], d[0])
            return rs


def count_with_generic_relation(qset, group, count_field='id', trans_map={}):
    return group_by_with_generic_relation(qset, group, measures=[Count(count_field)], trans_map=trans_map)


def group_by_with_generic_relation(qset, group, measures=[], trans_map={}):
    from django.contrib.contenttypes.fields import GenericForeignKey
    from django.contrib.contenttypes.models import ContentType
    if isinstance(group, string_types):
        group = group.split(',')
    trans_size = trans_map and len(trans_map.values()[0]) or 1
    gs = []
    p = 0
    for i, g in enumerate(group):
        rfs = modelutils.get_related_fields(qset, g)
        lf = rfs[-1]
        if isinstance(lf, GenericForeignKey):
            p = i
            ps = g.split('__')
            if len(ps) > 1:
                prefix = '__'.join(ps[:-1])
                gs.append('%s__%s' % (prefix, lf.ct_field))
                gs.append('%s__%s' % (prefix, lf.fk_field))
            else:
                gs.append(lf.ct_field)
                gs.append(lf.fk_field)
        else:
            gs.append(g)
    rs = group_by(qset, gs, measures=measures)
    ss = set([(r[p], r[p + 1]) for r in rs])
    sd = {}
    for ct, fk in ss:
        sd.setdefault(ct, []).append(fk)
    td = {}
    for ct_id, fk_ids in sd.items():
        if ct_id is None:
            continue
        ct = ContentType.objects.get(id=ct_id)
        md = ct.model_class()
        vls = trans_map.get(
            '%s.%s' % (ct.app_label, ct.model),
            [modelutils.find_field(md, lambda f: f.name in ['name', 'title']).name]
        )
        vls = ['id'] + vls
        ctrs = md.objects.filter(id__in=fk_ids).values_list(*vls).distinct()
        for r in ctrs:
            td[ct_id, r[0]] = r[1:]
    for r in rs:
        r[p:p + 2] = td.get((r[p], r[p + 1]), [None] * trans_size)
    if len(sd) > 1:
        rsd = {}
        nrs = []
        for r in rs:
            k = str(r[:-1])
            i = rsd.get(k, -1)
            if i >= 0:
                nrs[i][-1] += r[-1]
            else:
                rsd[k] = len(nrs)
                nrs.append(r)
        rs = nrs
    return rs


class DateStat(object):
    def __init__(self, query_set, time_field):
        self.query_set = query_set
        self.time_field = time_field
        self.model = self.query_set.model

    def get_period_query_set(self, period):
        qset = self.query_set
        if period and period != 'all':
            begin_time, end_time = dateutils.get_period_by_name(period)
            pms = {"%s__gte" % self.time_field: begin_time, "%s__lt" % self.time_field: end_time}
            qset = self.query_set.filter(**pms)
        return qset

    def stat(self, period=None, count_field='id', distinct=False, **kwargs):
        return self.group_by(period, measures=[Count(count_field, distinct=distinct)], **kwargs)

    def count_and_distinct(self, period=None, count_field='id', **kwargs):
        return self.group_by(period, measures=[Count(count_field), Count(count_field, distinct=True)], **kwargs)

    def group_by(self, period=None, group=[], only_first=False, filter=None, **kwargs):
        qset = self.get_period_query_set(period)
        if filter:
            qset = qset.filter(**filter)
        f = modelutils.get_related_field(qset, self.time_field)
        qset = qset.extra(select={'the_date': 'date(%s.%s)' % (f.model._meta.db_table, self.time_field)})
        res = group_by(qset, ['the_date'] + group, **kwargs)
        if only_first:
            res = res[0] if len(res) > 0 else None
        return res


def do_rest_stat_action(view, stats_action):
    qset = using_stats_db(view.filter_queryset(view.get_queryset())) if hasattr(view, 'get_queryset') else None
    pms = view.request.query_params
    ms = pms.getlist('measures', ['all'])
    from rest_framework.response import Response
    kwargs = dict(qset=qset, measures=ms, period=pms.get('period', '近7天'), time_field=pms.get('time_field'))
    return Response(stats_action(**kwargs))


def group_stat(data, fields, result, list_action=None):
    """

    :param data: dict or list
    :param fields: field name list
    :param result: stat result
    :param list_action:
            'concat'    all items joined by ',' as one key
            'seperate'  each items stats by seperately, index number as key
            default      each item as each key
    :return: result

In [51]: r={}

In [52]: group_stat({'a':3,'n':'a','r':False},['n','r'],r)
Out[52]: {'a': {False: 1}}

In [53]: group_stat({'a':3,'n':'a','r':False},['n','r'],r)
Out[53]: {'a': {False: 2}}

In [54]: group_stat({'a':2,'n':'a','r':True},['n','r'],r)
Out[54]: {'a': {False: 2, True: 1}}

In [55]: group_stat({'a':4,'n':'a','r':False},['n','r'],r)
Out[55]: {'a': {False: 3, True: 1}}
    """
    if isinstance(data, (list, tuple)):
        dl = data
    elif isinstance(data, dict):
        dl = [data]
    else:
        raise TypeError('data parameter should be type of list,tuple or dict')
    for d in dl:
        m = result
        for f in fields[:-1]:
            m = m.setdefault(d[f], {})
        lf = fields[-1]
        lv = d[lf]
        if isinstance(lv, (list, tuple)):
            if list_action == 'seperate':
                for i, k in enumerate(lv):
                    mi = m.setdefault(i, {})
                    mi.setdefault(k, 0)
                    mi[k] += 1
            elif list_action == 'concat':
                k = ",".join(lv)
                m.setdefault(k, 0)
                m[k] += 1
            else:
                for k in lv:
                    m.setdefault(k, 0)
                    m[k] += 1
        else:
            m.setdefault(lv, 0)
            m[lv] += 1
    return result


AGG_FUNCS = ['count', 'distinct', 'sum', 'avg']


class QuerySetStat(object):

    def __init__(self, qset, measures, groups=None):
        self.qset = qset
        self.meta = qset.model._meta
        if isinstance(measures, string_types):
            measures = measures.split(',')
        self.measures = [self.measure_split(m) for m in measures]
        if isinstance(groups, string_types):
            groups = groups.split(',')
        self.groups = [self.group_split(g) for g in groups if g] if groups else None

    def stat(self):
        if self.groups:
            return self.stat_by_groups()
        else:
            return self.stat_no_groups()

    def rank(self, top=-10):
        m = self.measures[0]
        af = self.get_agg_function(m['field'].name, m['agg'])
        return rank_by(self.qset, self.groups[0]['name'], measure=af, top=top)

    def stat_by_groups(self):
        ms = []
        for m in self.measures:
            af = self.get_agg_function(m['field'].name, m['agg'])
            ms.append(af)
        qset = self.qset
        gs = []
        gms = {}
        for g in self.groups:
            qset_extra = g.get('qset_extra')
            if qset_extra:
                qset = qset.extra(qset_extra)
            gs.append(g['name'])
            m = g.get('map')
            if m:
                gms[g['name']] = m
        return group_by(qset, gs, ms, group_map=gms)

    def get_agg_function(self, field, agg):
        from django.db.models import Count, Sum, Avg
        if agg == 'distinct':
            return Count(field, distinct=True)
        elif agg == 'count':
            return Count(field)
        elif agg == 'sum':
            return Sum(field)
        elif agg == 'avg':
            return Avg(field)

    def stat_no_groups(self):
        ds = {}
        for m in self.measures:
            af = self.get_agg_function(m['field'].name, m['agg'])
            ds[m['name']] = self.qset.aggregate(s=af)['s']
        return ds

    def measure_split(self, mn):
        field = 'id'
        agg = 'count'
        if mn:
            ps = mn.split('__')
            for a in ps:
                if a in AGG_FUNCS:
                    agg = a
                else:
                    field = a
        return dict(
            field=self.meta.get_field(field),
            agg=agg,
            name=mn
        )

    def group_split(self, gn):
        fp = gn
        ps = gn.split('__')
        func = None
        if ps[-1] == 'date':
            func = 'date'
            fp = '__'.join(ps[:-1])
        f = modelutils.get_related_field(self.qset, fp)
        fvn = modelutils.get_related_field_verbose_name(self.qset, fp)
        d = dict(
            name=gn,
            field=f,
            label=fvn,
        )
        if func == 'date':
            d['qset_extra'] = dict(select={gn: 'date(%s.%s)' % (f.model._meta.db_table, fp)})
            d['func'] = func
        choices = getattr(f, 'choices')
        if choices:
            d['map'] = dict(choices)
        return d

    def get_descriptions(self):
        m = self.meta
        model = dict(name=m.label_lower, label= m.verbose_name)
        ms = []
        for m in self.measures:
            mn = m['name']
            f = m['field']
            ms.append(dict(
                name=mn,
                label=f.verbose_name,
                agg=m['agg']
            ))
        rd = dict(measures=ms, model=model)
        if self.groups:
            gs = []
            for g in self.groups:
                gn = g['name']
                f = g['field']
                gs.append(dict(
                    name=gn,
                    label=f.verbose_name,
                    func=g.get('func')
                ))
            rd['groups'] = gs
        return rd


def smart_filter_queryset(qset, query_str=None):
    from url_filter.filtersets import ModelFilterSet
    from django.http import QueryDict
    r = QueryDict(query_str)
    class MyFilterSet(ModelFilterSet):
        class Meta(object):
            model = qset.model
    # qset = using_stats_db(qset)
    fs = MyFilterSet(data=r, queryset=qset)
    return fs.filter()

def smart_rest_stat_action(view):
    qset = using_stats_db(view.filter_queryset(view.get_queryset())) if hasattr(view, 'get_queryset') else None
    pms = view.request.query_params
    ms = pms.get('measures', 'count')
    st = QuerySetStat(qset, ms, groups=pms.get('groupby'))
    from rest_framework.response import Response
    d = st.get_descriptions()
    d['data'] = st.stat()
    return Response(d)
