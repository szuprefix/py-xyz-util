# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals
import textdistance

from django.db import connection, Error


def desc_table(table):
    cursor = connection.cursor()
    cursor.execute('desc %s' % table)
    return list(cursor.fetchall())


def show_tables(db):
    cursor = connection.cursor()
    cursor.execute('use %s' % db)
    cursor.execute('show tables')
    return [t[0] for t in list(cursor.fetchall())]

def sort_key(s1, s2):
    return [int(textdistance.cosine(s1, s2) * 10), len(s1) == len(s2)]

def get_best_match_table(table, tables):
    if not tables:
        return None
    ss = [(sort_key(table, t), t) for t in tables]
    ss.sort(reverse=True)
    return ss[0][1]


def get_best_match_field(src_field, fields):
    tfs = []
    for f in fields:
        s = textdistance.cosine(src_field, f)
        if s == 1:
            return f
        if s > 0.5:
            tfs.append([s, f])
    if not tfs:
        return None
    tfs.sort(reverse=True)
    for tf in tfs:
        tf[0] += textdistance.cosine(src_field[0], tf[1][0])
    tfs.sort(reverse=True)
    return tfs[0][1]


def trans_table(src_table, dest_table, extra=''):
    src_fields = desc_table(src_table)
    dest_fields = desc_table(dest_table)
    if not src_fields:
        raise Error('src_table %s not exists' % src_table)
    m = {}
    for f in dest_fields:
        bmf = get_best_match_field(f, src_fields)
        m[f] = bmf or ['null']
    dfs = [f[0] for f in m.keys()]
    sfs = [f[0] for f in m.values()]
    return "insert into %s (%s) select %s from %s %s;\n" % (
        dest_table, ','.join(dfs), ','.join(sfs), src_table, extra or "")
