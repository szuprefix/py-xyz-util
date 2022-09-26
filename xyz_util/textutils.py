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
