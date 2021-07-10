# -*- coding:utf-8 -*-
from __future__ import unicode_literals
from collections import OrderedDict

from django.forms import ModelForm
from django.forms.fields import Field
from django.core.exceptions import ValidationError

from . import widgetutils

__author__ = 'denishuang'


class CompositeChoicesField(Field):
    def __init__(self, choice_set={}, format_str=None, **kwargs):
        self.choice_set = choice_set
        self.widget = widgetutils.CompositeChoicesWidget(choice_set=choice_set, format_str=format_str)
        kwargs.pop("max_length", None)
        super(CompositeChoicesField, self).__init__(**kwargs)

    def validate(self, value):
        if not isinstance(value, (dict)):
            raise ValidationError("数据格式不正确", code='invalid')


class JsonField(Field):
    widget = widgetutils.JsonEditTextarea

    def to_python(self, value):
        if value is None:
            return None
        if isinstance(value, widgetutils.BrokenData):
            raise ValidationError("Json格式不正确", code='invalid')
        # try:
        #     return json.loads(value)
        # except:
        #     raise ValidationError("Json格式不正确", code='invalid')
        return value


class WordSetField(Field):
    widget = widgetutils.WordSetTextarea

    def to_python(self, value):
        if value is None:
            return None
        if not isinstance(value, list):
            raise TypeError("必须为列表类型")
        return value


class KeyValueJsonField(JsonField):
    widget = widgetutils.KeyValueTextarea


def boundField2json(field):
    def not_none(v):
        if v is None:
            return ''
        return v

    ff = field.field
    d = dict(
        name=field.name,
        value=not_none(field.value()),
        type=ff.__class__.__name__,
        required=ff.required,
        widget=ff.widget.__class__.__name__
    )
    for a in ["min_length", "max_length", "help_text", "label"]:
        if hasattr(ff, a) and getattr(ff, a):
            v = getattr(ff, a)
            if not isinstance(v, (int, basestring)):
                v = str(v)
            d[a] = v
    if hasattr(ff, 'choices'):
        ff.empty_label = None
        cs = ff.choices
        from django.forms.models import ModelChoiceIterator
        if isinstance(cs, ModelChoiceIterator):
            cs = [a for a in cs]
        d['choices'] = cs
    return d


def form2dict(form):
    d = OrderedDict([(f.name, boundField2json(f)) for f in form])
    if isinstance(form, ModelForm):
        fd = form._meta.model._meta
        for n, f in d.items():
            f['dbtype'] = fd.get_field(n).__class__.__name__
    return d
