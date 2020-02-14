# -*- coding:utf-8 -*-
from __future__ import unicode_literals
from collections import OrderedDict

from django.db.models import Count
from django.conf import settings
from . import modelutils, datautils, dateutils

__author__ = 'denishuang'

DB_FOR_STATS = getattr(settings, 'DB_FOR_STATS', None)


def using_stats_db(qset):
    if DB_FOR_STATS:
        qset = qset.using(DB_FOR_STATS)
    return qset


class Measure(object):
    # Tracks each time a Field instance is created. Used to retain order.
    creation_counter = 0

    def __init__(self, verbose_name, cond={}, exclude=False, agg=Count("id"), default=0):
        self.verbose_name = verbose_name
        self.cond = cond
        self.exclude = exclude
        self.agg = agg
        self.default = default
        # Increase the creation counter, and save our local copy.
        self.creation_counter = Measure.creation_counter
        Measure.creation_counter += 1


class Parameter(object):
    def __init__(self, name):
        self.name = name


class DeclarativeColumnsMetaclass(type):
    def __new__(mcs, name, bases, attrs):
        # Collect fields from current class.
        current_fields = []
        for key, value in list(attrs.items()):
            if isinstance(value, Measure):
                current_fields.append((key, value))
                attrs.pop(key)
        current_fields.sort(key=lambda x: x[1].creation_counter)
        attrs['declared_fields'] = OrderedDict(current_fields)
        return super(DeclarativeColumnsMetaclass, mcs).__new__(mcs, name, bases, attrs)


class StatResult(object):
    def __init__(self, fields, verbose_names, data):
        self.fields = fields
        self.verbose_names = verbose_names
        self.data = data

    def as_csv(self, show_header=True, line_spliter="\n", field_spliter="\t"):
        data = self.data
        if show_header:
            data = [self.verbose_names] + data
        return datautils.list2csv(self.data, line_spliter=line_spliter, field_spliter=field_spliter)

    def __str__(self):
        return self.as_csv()

    def as_html(self, attrs="class='table table-striped table-hover'", row_attrs=""):
        tpl = """<table %s>
<thead>
<tr>
<th>%s</th>
<tr>
</thead>
<tbody>
<tr %s>
<td>%s</td>
</tr>
</tbody></table>"""
        return tpl % (attrs,
                      "</th>\n<th>".join(self.verbose_names),
                      row_attrs,
                      datautils.list2csv(self.data, line_spliter="</td>\n</tr>\n<tr %s>\n<td>" % row_attrs,
                                         field_spliter="</td>\n<td>")
                      )


class StatTableBase(object):
    measure_params_dict = {}

    def __init__(self, query_set):
        self.query_set = query_set

    def stat(self, group):
        r = OrderedDict()
        names = [group]
        verbose_names = [modelutils.get_related_field(self.query_set.model, group).verbose_name]
        for name, field in self.declared_fields.items():
            cond = self.format_measure_condition(name, field)
            if field.exclude == True:
                qset = self.query_set.exclude(**cond)
            else:
                qset = self.query_set.filter(**cond)
            qset = qset.distinct()
            for d in qset.order_by(group).values(group).annotate(measure_value=field.agg):
                g = d[group]
                r.setdefault(g, OrderedDict())
                value = d.get("measure_value")
                r[g][name] = value
            names.append(name)
            verbose_names.append(field.verbose_name)
        data = []
        for k, v in r.items():
            line = [k] + [v.get(name, field.default) for name, field in self.declared_fields.items()]
            data.append(line)
        return StatResult(names, verbose_names, data)

    def format_measure_condition(self, name, measure):
        r = {}
        for k, v in measure.cond.items():
            if isinstance(v, Parameter):
                v = self.measure_params_dict.get(name, {}).get(v.name)
            if callable(v):
                v = v()
            r[k] = v
        return r

    def set_measure_params(self, measure_name, **kwargs):
        self.measure_params_dict[measure_name] = kwargs


StatTable = DeclarativeColumnsMetaclass(str('StatTable'), (StatTableBase,), {})


class StatObject(object):
    def __init__(self, obj):
        self.obj = obj

    def __getitem__(self, item):
        obj = self.obj
        meta = obj._meta
        fs = item.split("__")
        f = fs[0]
        field = meta.get_field(f)
        if not hasattr(field, "related_model"):
            raise Exception("%s is not a related_model", f)

        relate = getattr(obj, f)
        meta = field.related_model._meta
        f = fs[1]
        field = meta.get_field(f)
        value = self._get_choice(field.choices, fs[2])
        return relate.filter(**{f: value}).count()

    def _get_choice(self, choices, value):
        for k, t in choices:
            if unicode(k) == value or t == value:
                return k
        return value


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
        print dt.seconds, dt.days

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
        print beginTime, endTime, step
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


def count_by(qset, group, count_field='id', distinct=False, sort=None, group_map=None):
    if isinstance(group, (str, unicode)):
        group = group.split(',')
    qset = qset.values(*group).order_by(*group)
    dl = qset.annotate(c=Count(count_field, distinct=distinct))
    if sort is not None:
        dl = dl.order_by("%sc" % sort)
    fs = group + ['c']
    rs = [[d[f] for f in fs] for d in dl]
    if group_map:
        for d in rs:
            d[0] = group_map.get(d[0], d[0])
    return rs


def count_by_generic_relation(qset, group, count_field='id', distinct=False, sort=None):
    rfs = modelutils.get_related_fields(qset, group)
    if rfs:
        f = rfs[-2]
        if isinstance(f, dict):
            ct = f['content_type']
            gfk = f['field']
            prefix = "__".join([a.name for a in rfs[:-2]]) + "__"
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
    from django.contrib.contenttypes.fields import GenericForeignKey
    from django.contrib.contenttypes.models import ContentType
    if isinstance(group, (str, unicode)):
        group = group.split(',')
    trans_size = trans_map and len(trans_map.values()[0]) or 1
    gs = []
    p = 0
    for i, g in enumerate(group):
        rfs = modelutils.get_related_fields(qset, g)
        lf = rfs[-1]
        if isinstance(lf, GenericForeignKey):
            prefix = '__'.join(g.split('__')[:-1])
            gs.append('%s__%s' % (prefix, lf.ct_field))
            gs.append('%s__%s' % (prefix, lf.fk_field))
            p = i
        else:
            gs.append(g)
    rs = count_by(qset, gs, count_field=count_field)
    ss = set([(r[p], r[p + 1]) for r in rs])
    sd = {}
    for ct, fk in ss:
        sd.setdefault(ct, []).append(fk)
    td = {}
    for ct_id, fk_ids in sd.iteritems():
        if ct_id is None:
            continue
        ct = ContentType.objects.get(id=ct_id)
        md = ct.model_class()
        vls = trans_map.get('%s.%s' % (ct.app_label, ct.model), ['name'])
        vls = ['id'] + vls
        ctrs = md.objects.filter(id__in=fk_ids).values_list(*vls).distinct()
        for r in ctrs:
            td[ct_id, r[0]] = r[1:]
    for r in rs:
        r[p:p + 2] = td.get((r[p], r[p + 1]), [None]*trans_size)
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

    def stat(self, period=None, count_field='id', distinct=False, sort=None, only_first=False):
        qset = self.get_period_query_set(period).extra(select={'the_date': 'date(%s)' % self.time_field})
        res = count_by(qset, 'the_date', count_field=count_field, distinct=distinct, sort=sort)
        if only_first:
            res = res[0] if len(res) > 0 else None
        return res


def do_rest_stat_action(view, stats_action):
    qset = using_stats_db(view.filter_queryset(view.get_queryset()))
    pms = view.request.query_params
    ms = pms.getlist('measures', ['all'])
    from rest_framework.response import Response
    return Response(stats_action(qset, ms, pms.get('period', '近7天')))


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
