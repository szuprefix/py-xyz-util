#!/usr/bin/env python
# -*- coding:utf8 -*-
# Author:DenisHuang
# Date:2013/11/7
# Usage:
from __future__ import unicode_literals, print_function
from django.db import connections
import logging

from six import string_types

log = logging.getLogger("django")

try:
    db = connections['default']
    cursor = db.cursor()
    cursor._defer_warnings = True
except:
    pass

def get_table_fields(conn, table_name, schema=None):
    return get_table_schema(conn, table_name, schema=schema)['fields']


def switch_schema(cursor, schema=None):
    if not schema:
        schema = 'public' if cursor.db.vendor == 'postgresql' else None
    if schema:
        if cursor.db.vendor == 'postgresql':
            cursor.execute("set search_path to '%s'" % schema)


def get_table_schema(conn, table_name, schema=None):
    if schema is None:
        ps = table_name.split(".")
        table_name = ps[-1]
        schema = ps[0] if len(ps) > 1 else None
    with conn.cursor() as cursor:
        switch_schema(cursor, schema)
        vendor = conn.vendor
        db = conn.get_connection_params().get('db')
        context = {'db': db, 'table': table_name, 'schema': schema or 'public'}
        from collections import OrderedDict
        fields = OrderedDict()
        introspection = conn.introspection
        primary_key_columns = None
        unique_columns_groups = []
        try:
            constraints = introspection.get_constraints(cursor, table_name)
            for c in constraints.values():
                if c['primary_key']:
                    primary_key_columns = c['columns']
                elif c['unique']:
                    unique_columns_groups.append(
                        c['columns']
                    )
        except NotImplementedError:
            constraints = {}
        primary_key_column = ",".join(primary_key_columns) if primary_key_columns else None
        unique_columns = [g[0] for g in unique_columns_groups if len(g) == 1]

        sql = get_column_comment_sql(vendor)
        # print sql % context
        cursor.execute(sql % context)
        column_comment_map = dict(cursor.fetchall())
        for row in introspection.get_table_description(cursor, table_name):
            name = row[0]
            field_params = OrderedDict()
            field_notes = []

            try:
                field_type = introspection.get_field_type(row[1], row)
            except KeyError:
                field_type = 'TextField'
                field_notes.append('This field type is a guess.')

            # This is a hook for data_types_reverse to return a tuple of
            # (field_type, field_params_dict).
            if type(field_type) is tuple:
                field_type, new_params = field_type
                field_params.update(new_params)

            if name == primary_key_column:
                field_params['primary_key'] = True
            elif name in unique_columns:
                field_params['unique'] = True

            # Add max_length for all CharFields.
            if field_type == 'CharField' and row[3]:
                ml = int(row[3])
                field_params['max_length'] = ml if ml > 0 else 64

            if field_type == 'DecimalField':
                if row[4] is None or row[5] is None:
                    field_notes.append(
                        'max_digits and decimal_places have been guessed, as this '
                        'database handles decimal fields as float')
                    md = row[4] if row[4] is not None else 10
                    dp = row[5] if row[5] is not None else 5
                else:
                    md = row[4]
                    dp = row[5]
                field_params['max_digits'] = md == 65535 and 100 or md
                field_params['decimal_places'] = dp == 65535 and 5 or dp
            if row[6]:  # If it's NULL...
                if field_type == 'BooleanField(':
                    field_type = 'NullBooleanField('
                else:
                    field_params['blank'] = True
                    field_params['null'] = True
            comment = column_comment_map.get(name, '')
            label = comment.split(',')[0].strip() or name
            fields[name] = dict(name=name, type=field_type, params=field_params, notes=field_notes,
                                comment=comment, label=label)

        sql = get_table_comment_sql(vendor)
        cursor.execute(sql % context)
        comment = cursor.fetchall()[0][0]

        return dict(
            comment=comment,
            fields=fields,
            constraints=constraints,
            primary_key_columns=primary_key_columns,
            unique_columns_groups=unique_columns_groups
        )


def get_table_comment_sql(vendor):
    return dict(
        postgresql="""select  cast(obj_description(relfilenode,'pg_class') as varchar) as comment
              from pg_class c
              where  relkind = 'r'
              and relname='%(table)s';""",
        mysql="""select table_comment from information_schema.tables
              where table_schema='%(db)s'
              and table_name='%(table)s'"""
    ).get(vendor)


def get_column_comment_sql(vendor):
    return dict(
        postgresql="""select a.attname ,b.description
            from pg_catalog.pg_attribute a
            inner join pg_catalog.pg_description  b
            on a.attrelid=(select oid from pg_class where relname='%(table)s'
                           and relnamespace=(select oid from pg_namespace where nspname = '%(schema)s')
                           )
            and a.attnum>0
            and not a.attisdropped
            and b.objoid=a.attrelid
            and b.objsubid=a.attnum
            order by a.attnum;""",
        mysql="""select column_name,column_comment from information_schema.columns
            where column_comment>'' and  table_schema='%(db)s'
            and table_name='%(table)s'"""
    ).get(vendor)


def get_estimate_count_sql(vendor):
    default = "select count(1) as recs from %(table)s"
    return dict(
        postgresql="""select reltuples as recs from pg_class
where relkind = 'r'
and relnamespace = (select oid from pg_namespace where nspname='%(schema)s')
and relname = '%(table)s'"""
    ).get(vendor, default)


def create_table(conn, table, fields, schema=None, force_lower_name=False, primary_key=None, indexes=[]):
    try:
        old_fields = get_table_fields(conn, table, schema=schema)
        return  # table exists, do nothing
    except Exception as e:
        log.warning("dbutils.create_table exception: %s", e)  # table not exists, continue

    class NoneMeta(object):
        db_tablespace = None

    class NoneModel(object):
        _meta = NoneMeta()

    fs = {}
    model = NoneModel
    from django.db.models import fields as field_types
    column_sqls = []
    with conn.schema_editor() as schema_editor:
        quote_name = schema_editor.quote_name
        for k, v in fields.items():
            es = "field_types.%s('%s',%s)" % (v['type'], k, ','.join(["%s=%s" % a for a in v['params'].items()]))
            fs[k] = field = eval(es)
            field.column = force_lower_name and k.lower() or k
            definition, extra_params = schema_editor.column_sql(model, field)
            column_sqls.append("%s %s" % (
                quote_name(field.column),
                definition.replace('with time zone', 'without time zone'),
            ))
        full_table_name = schema and "%s.%s" % (schema, quote_name(table)) or quote_name(table)
        full_table_name = force_lower_name and full_table_name.lower() or full_table_name
        sql = schema_editor.sql_create_table % {
            "table": full_table_name,
            "definition": ", ".join(column_sqls)
        }
        print(sql)
        result = schema_editor.execute(sql)
        if primary_key and ',' in primary_key:
            sql = schema_editor.sql_create_pk % {
                "table": full_table_name,
                "name": quote_name("%s_pk_%s") % (table, primary_key.replace(',', '_')),
                "columns": primary_key
            }
            schema_editor.execute(sql)

        if indexes:
            for fs in indexes:
                sql = schema_editor.sql_create_index % {
                    'table': full_table_name,
                    'name': '',
                    'columns': ', '.join(fs),
                    'using': '',
                    'extra': ''
                }
                schema_editor.execute(sql)
        return result


def execute_sql(sql, db_name='default'):
    cur = connections[db_name].cursor()
    return cur.execute(sql), cur


def getDB(dbName='default'):
    return connections[dbName]

def get_connection(conn='default'):
    if isinstance(conn, string_types):
        return connections[conn]
    return conn

def getDBOptionals():
    return [(k, v["HOST"]) for k, v in connections.databases.items()]


def django_db_setting_2_sqlalchemy(sd):
    emap = {"mysql": "mysql+mysqldb"}
    engine = sd['ENGINE'].split(".")[-1]
    engine = emap.get(engine, engine)
    charset = sd.get("OPTIONS", {}).get("charset")
    params = charset and "?charset=%s" % charset or ""
    return "%s://%s:%s@%s/%s%s" % (engine, sd['USER'], sd['PASSWORD'], sd['HOST'], sd['NAME'], params)


def db_sqlalchemy_str(con):
    return django_db_setting_2_sqlalchemy(get_connection(con).settings_dict)


def get_slave_time(con):
    con = get_connection(con)
    sd = con.settings_dict
    engine = sd['ENGINE'].split(".")[-1]
    sql = {
        'mysql': "show slave status",
        "postgresql": "select pg_last_xact_replay_timestamp()::timestamp without time zone  as end_time"
    }.get(engine)
    if not sql:
        return
    import pandas as pd
    from datetime import datetime, timedelta
    now = datetime.now()
    df = pd.read_sql(sql, django_db_setting_2_sqlalchemy(sd))
    # print df
    if len(df) == 1:
        if engine == 'mysql':
            sbm = df.iloc[0]['Seconds_Behind_Master']
            return now - timedelta(seconds=sbm)
        elif engine == 'postgresql':
            return df.iloc[0]['end_time']


def transfer_table(db_src, db_dist, sql_select, table_name, chunk=1000, insert_type="", primary_keys=None,
                   update_timestamp_field=None, insert_timestamp_field=None, lower_field_name=False):
    cur_src = db_src.cursor()
    cur_dist = db_dist.cursor()
    cur_src.execute(sql_select)
    vendor = cur_dist.db.vendor
    fields = [db_dist.ops.quote_name(ft[0]) for ft in cur_src.description]
    if lower_field_name:
        fields = [f.lower() for f in fields]
    update_values = {}
    insert_values = {}
    if update_timestamp_field:
        update_values[update_timestamp_field] = 'CURRENT_TIMESTAMP'
        insert_values[update_timestamp_field] = 'CURRENT_TIMESTAMP'
    if insert_timestamp_field:
        insert_values[insert_timestamp_field] = 'CURRENT_TIMESTAMP'

    sql = gen_batch_insert_sql(
        table_name,
        fields,
        primary_keys,
        insert_type=insert_type,
        insert_values=insert_values,
        update_values=update_values,
        vendor=vendor
    )
    # print sql
    c = batch_execute(cur_dist, sql, cur_src, chunk=chunk)
    cur_dist.close()
    cur_src.close()
    return c


def gen_batch_insert_sql(table_name, fields, primary_keys, insert_type='replace', insert_values={}, update_values={},
                         vendor='mysql'):
    insert_fields = [f for f in fields if insert_values.get(f, f) is not None]
    for k, v in insert_values.items():
        if k not in insert_fields:
            insert_fields.append(k)

    update_fields = [f for f in fields if update_values.get(f, f) is not None]
    for k, v in update_values.items():
        if k not in update_fields:
            update_fields.append(k)

    qs_type = "insert"
    praise_on_update = ""
    qs_end = ""
    if vendor == "mysql":
        if insert_type == 'ignore':
            qs_type = 'insert ignore'
        else:
            fns = ",".join(["%s=%s" % (f, update_values.get(f, "VALUES(%s)" % f)) for f in update_fields])
            praise_on_update = "on duplicate key update %s" % fns
    elif vendor == "postgresql":
        if insert_type == "replace":
            fns = ",".join(["%s=%s" % (f, update_values.get(f, "excluded.%s" % f)) for f in update_fields])
            praise_on_update = " on conflict(%s) do update set %s" % (primary_keys, fns)
        else:
            qs_end = " on conflict(%s) do nothing" % primary_keys
    insert_field_names = ",".join(insert_fields)
    insert_field_values = ",".join([insert_values.get(f, '%s') for f in insert_fields])
    return "%s into %s (%s) values (%s) %s %s" % (
        qs_type, table_name, insert_field_names, insert_field_values, praise_on_update, qs_end)


def batch_execute(cursor, sql, data, chunk=1000):
    i = 0
    c = 0
    ds = []
    for row in data:
        ds.append(row)
        i += 1
        if i >= chunk:
            # print i
            cursor.executemany(sql, ds)
            ds = []
            c += i
            i = 0
    if ds:
        # print i
        c += i
        cursor.executemany(sql, ds)
    return c
