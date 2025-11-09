# -*- coding:utf-8 -*-
from __future__ import unicode_literals, print_function

import copy
from collections import OrderedDict
from django.db import connections

from django.contrib.contenttypes.fields import GenericForeignKey
from django.contrib.contenttypes.models import ContentType
from django.apps.registry import apps
__author__ = 'denishuang'

import django.db.models.fields as djfields
from django.forms.fields import TypedMultipleChoiceField
from django.forms.widgets import CheckboxSelectMultiple
from django.db.models import Count, Model, DateTimeField, Expression, IntegerField, QuerySet, ForeignKey, ManyToManyField, OneToOneField

import json, re
from six import text_type, string_types

from .datautils import auto_code


from django.core.serializers.json import DjangoJSONEncoder
class JSONEncoder(DjangoJSONEncoder):
    def default(self, o):
        from django.db.models.fields.files import FieldFile
        from django.db.models import Model, QuerySet
        if isinstance(o, (FieldFile,)):
            return o.name
        if isinstance(o, Model):
            return o.pk
        if isinstance(o, QuerySet):
            return [self.default(a) for a in o]
        return super(JSONEncoder, self).default(o)


def jsonSpecialFormat(v):
    from decimal import Decimal
    from datetime import date, datetime
    from django.db.models.fields.files import FieldFile
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, datetime):
        return v.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(v, Model):
        return v.pk
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, (FieldFile,)):
        return {'name:': v.name, 'url': v.url}
    return v


def model2dict(model, fields=[], exclude=[]):
    return dict([(attr, jsonSpecialFormat(getattr(model, attr)))
                 for attr in [f.name for f in model._meta.fields]
                 if not (fields and attr not in fields or exclude and attr in exclude)])


def queryset2dictlist(qset, fields=[], exclude=[]):
    return [model2dict(m, fields, exclude) for m in qset]


def queryset2dictdict(qset, fields=[], exclude=[]):
    return dict([(m.pk, model2dict(m, fields, exclude)) for m in qset])


class TimeFieldMixin(object):
    create_time = DateTimeField("创建时间", auto_now_add=True)
    modify_time = DateTimeField("修改时间", auto_now=True)


class CommaSeparatedIntegerField(djfields.CommaSeparatedIntegerField):
    def clean(self, value, model_instance):
        if self.choices and isinstance(value, (list, tuple)):
            for v in value:
                super(CommaSeparatedIntegerField, self).clean(v, model_instance)
            return ",".join(value)
        return super(CommaSeparatedIntegerField, self).clean(value, model_instance)

    def formfield(self, **kwargs):
        defaults = {"choices_form_class": TypedMultipleChoiceField,
                    "widget": CheckboxSelectMultiple,
                    "initial": "",
                    "empty_value": ""
                    }
        defaults.update(kwargs)
        return super(CommaSeparatedIntegerField, self).formfield(**defaults)


class MutipleGetFieldDisplayModelMixin:
    def _get_FIELD_display(self, field):
        from django.utils.encoding import force_str
        value = getattr(self, field.attname)
        print(value, type(value))
        if value:
            d = dict(field.flatchoices)
            return ",".join([force_str(d.get(v, v), strings_only=True) for v in value.split(",")])
        else:
            return force_str(dict(field.flatchoices).get(value, value), strings_only=True)


class CompositeChoicesField(djfields.CharField):
    def __init__(self, verbose_name=None, choice_set={}, format_str=None, **kwargs):
        self.choice_set = choice_set
        self.format_str = format_str
        super(CompositeChoicesField, self).__init__(verbose_name, **kwargs)

    def to_python(self, value):
        if not value:
            return {}
        elif isinstance(value, string_types):
            return json.loads(value)
        elif isinstance(value, (dict)):
            return value

    def get_prep_value(self, value):
        return json.dumps(value)

    def from_db_value(self, value, expression, connection, context):
        return self.to_python(value)

    def formfield(self, **kwargs):
        from .formutils import CompositeChoicesField
        defaults = {"form_class": CompositeChoicesField,
                    "choice_set": self.choice_set,
                    # "initial":{},
                    "format_str": self.format_str,
                    }
        defaults.update(kwargs)
        return super(CompositeChoicesField, self).formfield(**defaults)


class JSONField(djfields.Field):
    def get_internal_type(self):
        return "TextField"

    def to_python(self, value):
        return value

    def get_prep_value(self, value):
        return json.dumps(value, indent=2, cls=JSONEncoder)

    def from_db_value(self, value, expression, connection, context=None):
        if value is None:
            return None
        return json.loads(value)

    def formfield(self, **kwargs):
        from .formutils import JsonField
        defaults = {'form_class': JsonField}
        defaults.update(kwargs)
        return super(JSONField, self).formfield(**defaults)
        # return formutils.JsonField(**kwargs)

    def value_to_string(self, obj):
        val = self.value_from_object(obj)
        return json.dumps(val, indent=2, cls=JSONEncoder)

class SmallJSONField(JSONField):
    def get_internal_type(self):
        return "CharField"

class KeyValueJsonField(JSONField):
    def formfield(self, **kwargs):
        from .formutils import KeyValueJsonField
        defaults = {'form_class': KeyValueJsonField}
        defaults.update(kwargs)
        return super(JSONField, self).formfield(**defaults)


class WordSetField(djfields.Field):
    def get_internal_type(self):
        return "TextField"

    def to_python(self, value):
        return value

    def get_prep_value(self, value):
        return value and "\n".join(value) or ''

    def from_db_value(self, value, expression, connection, context):
        if not value:
            return []
        return value.split("\n")

    def formfield(self, **kwargs):
        from .formutils import WordSetField
        defaults = {'form_class': WordSetField}
        defaults.update(kwargs)
        return super(WordSetField, self).formfield(**defaults)

    def value_to_string(self, obj):
        val = self.value_from_object(obj)
        return self.get_prep_value(val)


def move_relation(src_obj, dest_obj):
    from django.db.models.deletion import get_candidate_relations_to_delete
    for related in get_candidate_relations_to_delete(src_obj._meta):
        field_name = related.field.name
        raname = related.get_accessor_name()
        if hasattr(src_obj, raname):
            related_obj = getattr(src_obj, raname)
            from django.db.models import Manager
            if isinstance(related_obj, Manager):
                related_obj.all().update(**{field_name: dest_obj})
            else:
                setattr(related_obj, field_name, dest_obj)
                related_obj.save()


def group_by(qset, group):
    return list(qset.order_by(group).values_list(group, flat=True).annotate(Count("id")))


def multi_group_by(qset, group):
    if isinstance(group, string_types):
        group = group.split(",")
    return list(qset.order_by(*group).values(*group).annotate(C=Count("id")))


def count_by(qset, group, new_group=None, count_field='id', distinct=False, sort=None):
    qset = qset.values(group).order_by(group)
    if new_group:
        from django.db.models import F
        d = {new_group: F(group)}
        qset = qset.values(**d)
    else:
        new_group = group
    dl = qset.annotate(c=Count(count_field, distinct=distinct))
    if sort is not None:
        dl = dl.order_by("%sc" % sort)
    return [(d[new_group], d["c"]) for d in dl]


def stat_by(qset, fields, group):
    r = {}
    for k, v in fields.items():
        for g, c in count_by(qset.filter(**v), group):
            r.setdefault(g, {})
            r[g][k] = c
    return r


RE_FIELD_SPLITER = re.compile(r"\.|__")


def find_field(m, find_func):
    meta = m._meta if hasattr(m, '_meta') else m
    for f in meta.get_fields():
        if find_func(f):
            return f

def get_datetime_field(meta):
    return find_field(meta, lambda f: isinstance(f, (djfields.DateTimeField, djfields.DateField)))

def get_generic_foreign_key(meta):
    return find_field(meta, lambda f: isinstance(f, GenericForeignKey))


def get_related_fields(obj, field_name, start_position=0):
    meta = obj.model._meta if isinstance(obj, QuerySet) else obj._meta
    fs = RE_FIELD_SPLITER.split(field_name)
    r = []

    def get_field(meta, f):
        try:
            return meta.get_field(f)
        except:
            gfk = get_generic_foreign_key(meta)
            if not gfk:
                from django.core.exceptions import FieldDoesNotExist
                raise FieldDoesNotExist("%s has no field named '%s'" % (meta.object_name, f))
            ps = f.split('_')
            ct = ContentType.objects.get(app_label=ps[0], model='_'.join(ps[1:]))
            return {'field': gfk, 'content_type': ct}

    for f in fs[:-1]:
        fd = get_field(meta, f)
        if isinstance(fd, dict):
            meta = fd['content_type'].model_class()._meta
        else:
            meta = fd.related_model._meta
        r.append(fd)
    r.append(get_field(meta, fs[-1]))
    return r


def get_related_field(obj, field_name):
    return get_related_fields(obj, field_name)[-1]


def get_related_field_verbose_name(obj, field_name, start_position=0):
    r = get_related_fields(obj, field_name, start_position=start_position)
    return "".join([text_type(getattr(a, 'verbose_name', None) or a['content_type']) for a in r[start_position:]])


def get_object_accessor_value(record, accessor):
    penultimate, remainder = accessor.penultimate(record)

    from django.db import models
    if isinstance(penultimate, models.Model):
        try:
            field = accessor.get_field(record)
            display_fn = getattr(penultimate, 'get_%s_display' % remainder, None)
            if getattr(field, 'choices', ()) and display_fn:
                return display_fn()
        except models.FieldDoesNotExist:
            pass
    from django_tables2.utils import A
    v = A(remainder).resolve(penultimate, quiet=True)
    if isinstance(v, Model):
        return text_type(v)
    elif hasattr(v, 'update_or_create'):  # a Model Manager ?
        return ";".join([text_type(o) for o in v.all()])
    return v


def object2dict4display(obj, fields):
    from django_tables2.utils import A
    return OrderedDict(
        [(f, {
            "name": f,
            "verbose_name": get_related_field_verbose_name(obj, f),
            "value": get_object_accessor_value(obj, A(f))
        }
          ) for f in fields]
    )


def get_objects_accessor_data(accessors, content_type_id, object_ids):
    from django_tables2.utils import Accessor
    acs = [Accessor(a) for a in accessors]
    ct = ContentType.objects.get_for_id(content_type_id)
    for id in object_ids:
        obj = ct.get_object_for_this_type(id=id)
        yield [a.resolve(obj) for a in acs]


def get_model_dependants(model):
    return set([f.related_model._meta.label for f in model._meta.fields if f.many_to_one])


class QuerysetDict(object):
    def __init__(self, qset, keyFieldName, valueFieldName):
        self.qset = qset
        self.keyFieldName = keyFieldName
        self.valueFieldName = valueFieldName

    def __getitem__(self, item):
        return self.get(item)

    def get(self, item, default=None):
        obj = self.qset.filter(**{self.keyFieldName: item}).first()
        if obj:
            return getattr(obj, self.valueFieldName, default)
        return default


def translate_model_values(model, values, fields=[]):
    from django.db.models.fields.reverse_related import ManyToManyRel
    if not values:
        return {}
    fs = [f for f in model._meta.get_fields() if f.name in fields]
    rs = {}
    for f in fs:
        vbn = f.related_model._meta.verbose_name if isinstance(f, ManyToManyRel) else f.verbose_name
        v = values.get(vbn)
        if isinstance(f, (ForeignKey, )):
            fo, created = f.related_model.objects.get_or_create(name=v)
            v = fo
        elif isinstance(f, (ManyToManyRel, ManyToManyField)):
            vs = []
            for a in set([ a.strip() for a in v.split(',')]):
                fo, created = f.related_model.objects.get_or_create(name=a)
                vs.append(fo)
            v = vs
        elif f.choices:
            from .datautils import choices_map_reverse
            m = choices_map_reverse(f.choices)
            v = m.get(v)
        if v is None and f.default != djfields.NOT_PROVIDED:
            v = f.default
        rs[f.name] = v

    return rs


class CodeMixin(object):
    def save(self, **kwargs):
        if not self.code:
            self.code = auto_code(self.name)
        return super(CodeMixin, self).save(**kwargs)


class CharCorrelation(Expression):
    def __init__(self, expressions, output_field=None):
        if not output_field:
            output_field = IntegerField()
        super(CharCorrelation, self).__init__(output_field=output_field)
        if len(expressions) != 2:
            raise ValueError('expressions must have 2 elements')
        from django.db.models import F
        fn = expressions[0]
        if not isinstance(fn, F):
            raise ValueError('expressions first element must be instance of Field (F) ')
        self.expressions = expressions

    def as_sql(self, compiler, connection):
        exp = self.expressions
        fn = exp[0].name
        a = '+'.join(['(%s like "%%%%%s%%%%")' % (fn, c) for c in set(exp[1])])
        return '(%s)' % a, []


def get_relations(m1, m2):
    if isinstance(m1, string_types):
        m1 = apps.get_model(m1)
    if isinstance(m2, string_types):
        m2 = apps.get_model(m2)
    return [f for f in m1._meta.get_fields() if f.is_relation and f.related_model == m2]


def get_model_related_field(m1, m2):
    return find_field(m1, lambda f:  f.is_relation and f.related_model == m2)
    # fs = [f for f in m1._meta.get_fields() if f.is_relation and f.related_model == m2]
    # return fs[0] if fs else None


def distinct(qset, field_name):
    return qset.values(field_name).order_by(field_name).annotate().values_list(field_name, flat=True)


def get_field_verbose_name(f):
    return f.many_to_many and f.related_model._meta.verbose_name \
           or hasattr(f, 'field') and f.field.verbose_name \
           or f.verbose_name


def get_model_verbose_name_map():
    r = {}
    for an, a in apps.all_models.items():
        for mn, m in a.items():
            mvn = m._meta.verbose_name
            r.setdefault(mvn, []).append(m)
            avn = m._meta.app_config.verbose_name
            r.setdefault('%s%s' % (avn, mvn), []).append(m)
    return r


def get_model_field_verbose_name_map(m):
    if isinstance(m, string_types):
        m = apps.get_model(m)
    r = {}
    for f in m._meta.get_fields():
        r[get_field_verbose_name(f)] = f
    return r

def get_generic_related_objects(src_object, target_model):
    if isinstance(target_model, string_types):
        target_model = apps.get_model(target_model)
    ct = ContentType.objects.get_for_model(src_object)
    gfk = get_generic_foreign_key(target_model._meta)
    cond = {gfk.ct_field: ct, gfk.fk_field: src_object.pk}
    return target_model.objects.filter(**cond)

def get_all_model_dependants():
    """
    获取所有模型及其依赖关系
    返回：模型依赖字典，key 为模型，value 为被当前模型引用的模型列表
    """
    dependants = {}
    for model in apps.get_models():
        dependants[model] = []

    for model in apps.get_models():
        for field in model._meta.fields:
            if isinstance(field, (ForeignKey, ManyToManyField, OneToOneField)):
                dependants[field.related_model].append(model)

    return dependants

def build_dependency_graph():
    """
    构建模型的依赖图
    返回：依赖图字典，key 为模型，value 为它依赖的模型列表
    """
    dependants = get_all_model_dependants()
    graph = {}

    for model in apps.get_models():
        graph[model] = set(dependants[model])

    return graph

from collections import deque

def topological_sort(graph):
    """
    使用拓扑排序算法计算迁移顺序
    返回：模型顺序列表
    """
    # 入度：记录每个模型的依赖数
    in_degree = {model: 0 for model in graph}
    for model, dependencies in graph.items():
        for dep in dependencies:
            in_degree[dep] += 1

    # 队列：入度为 0 的模型（无依赖，最先迁移）
    queue = deque([model for model, degree in in_degree.items() if degree == 0])

    sorted_order = []

    while queue:
        current_model = queue.popleft()
        sorted_order.append(current_model)

        # 更新所有依赖当前模型的模型的入度
        for dependent_model in graph[current_model]:
            in_degree[dependent_model] -= 1
            if in_degree[dependent_model] == 0:
                queue.append(dependent_model)

    # 检查是否有环（如果有环，排序不可能完成）
    if len(sorted_order) != len(graph):
        raise Exception("模型依赖图中存在循环依赖，无法排序！")

    return sorted_order


class BaseTransfer():

    def __init__(self, source_name, conn_config={}):
        self.source = source_name
        d = copy.deepcopy(connections['default'].settings_dict)
        d.pop('NAME', None)
        d.update(conn_config)
        if 'NAME' not in d:
            d['NAME'] = source_name
        # from django.conf import settings
        # settings.DATABASES[source_name] = d
        connections.databases[source_name] = d
        self.model_pk_map = {}

    def normalize_model(self, model):
        if isinstance(model, str):
            app_label, model_name = model.split('.')
            model = apps.get_model(app_label=app_label, model_name=model_name)
        return model


    def get_unique_keys(self, model):
        model = self.normalize_model(model)
        unique_together = model._meta.unique_together
        if unique_together:
            return unique_together[0]
        unique_fields = [(1 if f.null else 2, f.name) for f in model._meta.fields if f.unique and not f.primary_key]
        if not unique_fields:
            return
        unique_fields = sorted(unique_fields, reverse=True)
        return [unique_fields[0][1]]

    def get_query_set_pair(self, model):
        return model.objects.using(self.source), model.objects.using('default')

#
# class Transfer(BaseTransfer):
#
#     def content_type_map(self, app, model):
#         qs, qd = self.get_query_set_pair(self.normalize_model('contenttypes.ContentType'))
#         #for qs.values_list()
#
#
#     def map_difference(self, source_list, dest_list, by='common'):
#         ms = {}
#         md = {}
#         for a in source_list:
#             ms[tuple(a.values())[1:]] = a['id']
#         for a in dest_list:
#             md[tuple(a.values())[1:]] = a['id']
#         if by == 'common':
#             keys = ms.keys() & md.keys()
#             return {ms[k]: md[k] for k in keys}
#         elif by == 'missing':
#             keys = ms.keys() - md.keys()
#             return {ms[k]: None for k in keys}
#
#     def ensure_map(self, model, key_fields):
#         model = self.normalize_model(model)
#         qs, qd = self.get_query_set_pair(model)
#         idm = self.map_difference(
#             qs.values('id', *key_fields),
#             qd.values('id', *key_fields),
#             by='common'
#         )
#         mn = model._meta.label
#         self.model_pk_map.setdefault(mn, {}).update(idm)
#         return idm
#
#
#     def trans(self, model, key_fields=None, do_action=False, filter=None):
#         model = self.normalize_model(model)
#         if not key_fields:
#             key_fields = self.get_unique_keys(model)
#             if not key_fields:
#                 return
#         self.ensure_map(model, key_fields)
#
#         qs, qd = self.get_query_set_pair(model)
#         if filter:
#             qs = qs.filter(filter)
#         missing = self.map_difference(
#             qs.values('id', *key_fields),
#             qd.values('id', *key_fields),
#             by='missing'
#         )
#
#         if missing:
#             print(f"  ➕ inserting {len(missing)} new rows into default...")
#
#             for sid in missing.keys():
#                 src_obj = qs.get(id=sid)
#                 data = {
#                     f.attname: getattr(src_obj, f.attname)
#                     for f in model._meta.fields
#                     if not f.primary_key
#                 }
#                 data = self._replace_foreign_keys(model, data)
#                 if do_action:
#                     new_obj = model.objects.using('default').create(**data)
#                     self.model_pk_map[model._meta.label][sid] = new_obj.id
#                 else:
#                     print(f'# insert: {data}\n')
#
#         for dm  in get_model_dependants(model):
#             self.trans(dm, do_action=do_action)
#
#
#     def insert_missing_records(self, model):
#         model = self.normalize_model(model)
#         src_qs, dst_qs = self.get_query_set_pair(model)
#         existing_ids = set(self.model_pk_map.get(model._meta.label, {}).keys())
#         for obj in src_qs.exclude(id__in=existing_ids):
#             data = {f.name: getattr(obj, f.name) for f in model._meta.fields if not f.primary_key}
#             # 替换外键引用
#             for f in model._meta.fields:
#                 if f.is_relation and getattr(obj, f.name):
#                     ref_model = f.related_model._meta.label
#                     ref_map = self.model_pk_map.get(ref_model, {})
#                     if getattr(obj, f.name).id in ref_map:
#                         data[f.name + "_id"] = ref_map[getattr(obj, f.name).id]
#             new_obj = model.objects.using('default').create(**data)
#             self.model_pk_map[model._meta.label][obj.id] = new_obj.id
#
#     def _replace_foreign_keys(self, model, obj_data):
#         """将外键值替换成目标库ID"""
#         for f in model._meta.fields:
#             if isinstance(f, ForeignKey):
#                 rel_model = f.remote_field.model
#                 rel_label = rel_model._meta.label
#                 rel_map = self.model_pk_map.get(rel_label)
#                 if rel_map:
#                     old_id = obj_data.get(f.attname)
#                     if old_id in rel_map:
#                         obj_data[f.attname] = rel_map[old_id]
#         return obj_data
#
#     def verify_transfer(self, model):
#         model = self.normalize_model(model)
#         qs, qd = self.get_query_set_pair(model)
#         mismatched = []
#         for sk, tk in self.model_pk_map.get(model._meta.label, {}).items():
#             s = qs.filter(pk=sk).values(*key_fields).first()
#             d = qd.filter(pk=tk).values(*key_fields).first()
#             if s != d:
#                 mismatched.append((sk, tk))
#         if mismatched:
#             print(f"⚠️ {model._meta.label}: {len(mismatched)} mismatches")

class TransferOneByOne(BaseTransfer):
    def replace_obj_foreign_keys(self, obj):
        d = self.model2dict(obj)
        for f in obj._meta.fields:
            if f.is_relation:
                rel_label = f.related_model._meta.label
                rel_map = self.model_pk_map.setdefault(rel_label, {})
                old_id = getattr(obj, f.attname)
                new_id = rel_map.get(old_id)
                print(f'{obj._meta.label} {obj.pk}, {f.attname}')
                if new_id:
                    d[f.attname] = rel_map[old_id]
                else:
                    rel_obj = getattr(obj, f.name)
                    d[f.attname] = self.get_twin_id(rel_obj)
        return d

    def get_twin_id(self, obj, uks=None):
        if obj is None:
            return
        if not uks:
            uks = self.get_unique_keys(obj)
        d = self.replace_obj_foreign_keys(obj)
        qs, qd = self.get_query_set_pair(obj._meta.model)
        cond={}
        for f in obj._meta.fields:
            if f.name in uks:
                cond[f.attname] = d[f.attname]
        print(f'{obj._meta.label} {obj.pk} by {cond}')
        new_obj, created = qd.get_or_create(**cond, defaults=d)
        if created:
            print(f'{obj._meta.label} created: {new_obj}')
        self.model_pk_map.setdefault(obj._meta.label, {})[obj.pk] = new_obj.pk
        return new_obj.pk


    def model2dict(self, obj):
        return {
            f.attname: getattr(obj, f.attname)
            for f in obj._meta.fields
            if not f.auto_created
        }

    def trans(self, model, cond={}, exclude={}, real=True):
        model = self.normalize_model(model)
        qs, qd = self.get_query_set_pair(model)
        qset = qs.filter(**cond)
        if exclude:
            qset = qset.exclude(**exclude)
        if not real:
            return qset.count()
        idm = self.model_pk_map.get(model._meta.label,{})
        for i, a in enumerate(qset):
            if idm.get(a.pk):
                print('ignore', a.pk, 'exists')
                continue
            print(i, a.pk, self.get_twin_id(a), '\n')

    def trans_relations(self, model, relations=[]):
        pks = self.model_pk_map[model].keys()
        mlabel = model if isinstance(model, str) else model._meta.label
        for rel in relations:
            rel_model = self.normalize_model(rel)
            fn = find_field(rel_model, lambda f: f.is_relation and f.related_model._meta.label == mlabel).name
            cond={f'{fn}__in': pks}
            print(rel_model, cond)
            self.trans(rel_model, cond)
