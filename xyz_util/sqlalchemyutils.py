import os

def flask_db(app, env_name='CONN'):
    db = app.extensions.get('sqlalchemy')
    if db:
        return db
    CONN = os.getenv(env_name)
    # MySQL数据库连接配置
    app.config['SQLALCHEMY_DATABASE_URI'] = f'mysql+pymysql://{CONN}'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    # 设置多数据库
    DBS = [a.strip() for a in os.getenv('DBS', '').split('\n') if a.strip()]
    if DBS:
        uri = app.config['SQLALCHEMY_DATABASE_URI']
        dfdb = uri.split('/')[-1].split('?')[0]
        binds = {}
        binds[dfdb] = uri
        for a in DBS:
            if '@' in a:
                dfdb, _, conn = a.partition('->')
                binds[dfdb.strip()] = f'mysql+pymysql://{conn.strip()}'
            else:
                binds[a] = uri.replace(f'/{dfdb}?', f'/{a}?')
        app.config['SQLALCHEMY_BINDS'] = binds

    from flask_sqlalchemy import SQLAlchemy

    # 初始化数据库对象
    db = SQLAlchemy(app)
    return db


def model_clean(model, rd):
    allowed_fields = {col.name for col in model.__table__.columns}
    d = {key: value for key, value in rd.items() if key in allowed_fields}
    return d

class ContextExists:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass


class FlaskDB:

    def __init__(self, app):
        self.app = app
        self.db = flask_db(app)

    def ensure_context(self):
        from flask import has_app_context
        if has_app_context():
            return ContextExists()
        return self.app.app_context()

    def query(self, sql):
        from sqlalchemy import text
        with self.ensure_context():
            result = self.db.session.execute(text(sql))
        return [dict(zip(result.keys(),row)) for row in result]

    def execute(self, sql):
        from sqlalchemy import text
        with self.ensure_context():
            self.db.session.execute(text(sql))
            self.db.session.commit()


    def reflect_model(self, table_name):
        db = self.db
        base = None
        ps = table_name.split('.')
        if len(ps) == 2:
            base, table_name = ps
            print(f'reflect_model: {base} {table_name}')

        with self.ensure_context():
            # 获取正确的引擎或元数据对象
            bind_engine = db.get_engine(base) if base else db.engine
            metadata = db.metadatas[base] if base else db.MetaData()

            # 使用正确的引擎来反射表结构
            __table__ = db.Table(table_name, metadata, autoload_with=bind_engine)

            attrs = {'__table__': __table__}
            if base:
                attrs['__bind_key__'] = base

            return type(table_name.capitalize(), (db.Model,), attrs)

    def update_or_create(self, model, defaults=None, create_only=None, **kwargs):
        """
        查找或更新一条记录。如果记录不存在，则创建它。

        参数:
        - model: SQLAlchemy 模型类
        - defaults: 要更新或创建的默认字段值（字典形式）
        - kwargs: 用于查找的条件（通常是唯一标识）
        """
        with self.ensure_context():
            db = self.db
            if isinstance(model, str):
                model = self.reflect_model(model)

            qs = db.session.query(model)

            # 查找是否已有记录
            instance = qs.filter_by(**kwargs).first()

            # 如果有记录，更新字段
            if instance:
                if create_only:
                    for f in create_only:
                        defaults.pop(f, None)
                for key, value in (defaults or {}).items():
                    setattr(instance, key, value)
                db.session.commit()  # 更新后提交
            else:
                # 如果没有记录，创建新记录
                params = {**kwargs, **(defaults or {})}
                instance = model(**params)
                db.session.add(instance)
                db.session.commit()  # 提交新记录

            return as_dict(instance)

def normalize_filter(model, filter):
    from sqlalchemy import and_, or_
    filters = []
    for key, value in filter.items():
        field_name, _, op = key.partition('__')
        if op == 'gt':
            filters.append(getattr(model, field_name) > value)
        elif op == 'gte':
            filters.append(getattr(model, field_name) >= value)
        elif op == 'lt':
            filters.append(getattr(model, field_name) < value)
        elif op == 'lte':
            filters.append(getattr(model, field_name) <= value)
        elif op == 'ne':
            filters.append(getattr(model, field_name) != value)
        elif op == 'in':
            filters.append(getattr(model, field_name).in_(value))
        else:
            # 默认是等于
            filters.append(getattr(model, field_name) == value)

    return and_(True, *filters)

def as_dict(model_instance, follow=None):
    if model_instance is None:
        return None
    from collections import OrderedDict
    if follow is None:
        follow = set()

    result = OrderedDict()
    for key, column in model_instance.__mapper__.columns.items():
        value = getattr(model_instance, key)
        result[key] = value

    # 如果有关系字段并且需要跟随这些关系进行序列化
    if follow:
        for relation in model_instance.__mapper__.relationships:
            if relation.key in follow:
                related_obj = getattr(model_instance, relation.key)
                if related_obj is not None:
                    if relation.uselist:
                        result[relation.key] = [as_dict(item, follow=follow) for item in related_obj]
                    else:
                        result[relation.key] = as_dict(related_obj, follow=follow)

    return result

