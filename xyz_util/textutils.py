# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals
import re

from six import text_type


def simple_split(r, s):
    it = r.finditer(s)
    le = 0
    for m in it:
        pb = m.start()
        pe = m.end()
        yield s[le:pb]
        yield s[pb:pe]
        le = pe
    yield s[le:]


class Splitter(object):
    def __init__(self, s):
        self.rexpr = re.compile(s)

    def __call__(self, s):
        d = dict(head='', items=[], splitters=[])
        for i, p in enumerate(simple_split(self.rexpr, s)):
            if i == 0:
                d['head'] = p
            elif i % 2 == 1:
                d['splitters'].append(p)
            else:
                d['items'].append(p)
        return d

class FitSplitter(object):
    def __init__(self, s):
        self.rexpr = re.compile(s)

    def __call__(self, s):
        d = dict(head='', items=[], splitters=[])
        for i, p in enumerate(self.rexpr.split(s)):
            if i == 0:
                d['head'] = p
            elif i % 2 == 1:
                d['splitters'].append(p)
            else:
                d['items'].append(p)
        return d


def hierarchy(s, spliters):
    if not spliters:
        return TreeNode(s.strip())
    spl = spliters[0]
    if isinstance(spl, text_type):
        spl = Splitter(spl)
    a = spl(s)
    tn = TreeNode(a['head'].strip())
    for i, b in enumerate(a['items']):
        sn = hierarchy(b, spliters[1:])
        sn.prefix = a['splitters'][i]
        tn.items.append(sn)
    return tn


class TreeNode(object):

    def __init__(self, name, items=None):
        self.name = name
        self.items = items or []
        self.prefix = ''

    def __iter__(self):
        for a in self.items:
            yield a

    def __getitem__(self, i):
        return self.items[i]

    def __str__(self):
        return self.name

    def to_dict(self):
        from collections import OrderedDict
        d = dict(name=self.name, prefix = self.prefix)
        ds = OrderedDict()
        for a in self.items:
            ds[a.name]=(a.to_dict())
        if ds:
            d['items'] = ds
        return d

    def to_array(self, parent=''):
        from collections import OrderedDict
        d = dict(name=self.name, prefix=self.prefix, parent=parent)
        ds = [d]
        for a in self.items:
            ds+= a.to_array(parent=self.name)
        return ds


import re
from typing import Dict, List, Pattern, Union


def splitter(pattern: Union[str, Pattern[str]]):
    compiled_re = re.compile(pattern) if isinstance(pattern, str) else pattern

    def split_fn(s: str) -> Dict[str, Union[str, List[str]]]:
        ps = compiled_re.split(s)
        rs = ps[1:]
        return {
            "head": ps[0],
            "items": [rs[i] for i in range(len(rs)) if i % 2 == 1],
            "splitters": [rs[i] for i in range(len(rs)) if i % 2 == 0]
        }

    return split_fn


def groups(d: Dict[str, Union[str, List[str]]]) -> Dict[str, Union[str, List[Dict[str, str]]]]:
    return {
        "head": d["head"],
        "items": [{"label": d["splitters"][i], "text": a} for i, a in enumerate(d["items"])]
    }