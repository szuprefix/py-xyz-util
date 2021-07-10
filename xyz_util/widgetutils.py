# -*- coding:utf-8 -*-
from __future__ import unicode_literals
from .datautils import JSONEncoder
from six import string_types
__author__ = 'Administrator'

import json
from django.forms import widgets


class CompositeChoicesWidget(widgets.MultiWidget):
    def __init__(self, attrs=None, choice_set={}, format_str=None):
        self.keynames = [name for name, choices in choice_set.items()]
        _widgets = [widgets.Select(attrs=attrs, choices=choice_set[name]) for name in self.keynames]
        self.format_str = format_str or "".join(["%s:%%(%s)s" % (name, name) for name in self.keynames])
        super(CompositeChoicesWidget, self).__init__(_widgets, attrs)

    def decompress(self, value):
        if value:
            return [value.get(n, None) for n in self.keynames]
        return [None for n in self.keynames]

    def format_output(self, rendered_widgets):
        d = dict([(name, rendered_widgets[i]) for i, name in enumerate(self.keynames)])
        return self.format_str % d

    def value_from_datadict(self, data, files, name):
        choicelist = [
            widget.value_from_datadict(data, files, name + '_%s' % i)
            for i, widget in enumerate(self.widgets)]
        try:
            d = dict([(name, choicelist[i]) for i, name in enumerate(self.keynames)])
        except ValueError:
            return {}
        else:
            return d


class BrokenData(object):
    def __init__(self, value):
        self.value = value


class JsonEditTextarea(widgets.Textarea):
    def format_value(self, value):
        if value is None:
            value = ""
        if isinstance(value, BrokenData):
            value = value.value
        elif isinstance(value, string_types):
            return value
        else:
            value = json.dumps(value, indent=2, cls=JSONEncoder)
        return value

    def render(self, name, value, attrs=None, renderer=None):
        value = self.format_value(value)
        return super(JsonEditTextarea, self).render(name, value, attrs)

    def value_from_datadict(self, data, files, name):
        val = super(JsonEditTextarea, self).value_from_datadict(data, files, name)
        if val == '':
            return None
        try:
            val = json.loads(val)
        except:
            return BrokenData(val)
        return val


class KeyValueTextarea(widgets.Textarea):
    def value_from_datadict(self, data, files, name):
        val = super(KeyValueTextarea, self).value_from_datadict(data, files, name)
        from .datautils import str2dict
        import json
        d = str2dict(val)
        return d

    def render(self, name, value, attrs=None):
        import json
        if value == None:
            d = {}
        else:
            d = value  # json.loads(value)
        value = "\n".join([u"%s:%s" % (k, v) for k, v in d.items()])
        return super(KeyValueTextarea, self).render(name, value, attrs)


class WordSetTextarea(widgets.Textarea):
    def format_value(self, value):
        if value is None:
            value = ""
        if isinstance(value, list):
            value = "\n".join(value)
        return value

    def render(self, name, value, attrs=None):
        value = self.format_value(value)
        return super(WordSetTextarea, self).render(name, value, attrs)

    def value_from_datadict(self, data, files, name):
        val = super(WordSetTextarea, self).value_from_datadict(data, files, name)
        import re
        r = re.compile(r"[\s,;，；]+")
        return r.split(val)


def element_ui_widget(form_field):
    from django.forms import fields, models
    d = {
        fields.Select: 'select',
        fields.TypedChoiceField: 'radio',
        fields.DateField: 'date-picker',
        fields.DateTimeField: 'date-picker!datetime',
        fields.MultipleChoiceField: 'checkbox',
        fields.BooleanField: 'switch',
        models.ModelChoiceField: 'select'
    }
    return d.get(form_field, 'input')
