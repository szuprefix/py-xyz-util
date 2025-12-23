import os
from sqlalchemy import create_engine, text, and_, MetaData, Table
from sqlalchemy.orm import sessionmaker, scoped_session
from contextlib import contextmanager
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Set, Type, Union

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


class DatabaseManager:
    """通用的数据库管理器，支持单数据库和多数据库配置"""

    def __init__(self, database_uri: str, binds: Optional[Dict[str, str]] = None,
                 echo: bool = False, pool_size: int = 5, max_overflow: int = 10):
        """
        初始化数据库管理器

        Args:
            database_uri: 主数据库连接URI
            binds: 多数据库绑定配置 {database_name: connection_uri}
            echo: 是否打印SQL语句
            pool_size: 连接池大小
            max_overflow: 最大溢出连接数
        """
        self.database_uri = database_uri
        self.binds = binds or {}
        self.echo = echo

        # 创建主引擎
        self.engine = create_engine(
            database_uri,
            echo=echo,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=True  # 连接前检查
        )

        # 创建绑定引擎
        self.bind_engines = {}
        for name, uri in self.binds.items():
            self.bind_engines[name] = create_engine(
                uri,
                echo=echo,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_pre_ping=True
            )

        # 创建session工厂
        self.SessionLocal = scoped_session(sessionmaker(bind=self.engine))

        # 元数据字典
        self.metadatas = {None: MetaData()}
        for name in self.bind_engines.keys():
            self.metadatas[name] = MetaData()

    @contextmanager
    def get_session(self, bind_key: Optional[str] = None):
        """
        获取数据库会话的上下文管理器

        Args:
            bind_key: 绑定的数据库名称，None表示使用主数据库
        """
        engine = self.bind_engines.get(bind_key) if bind_key else self.engine
        Session = sessionmaker(bind=engine)
        session = Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def query(self, sql: str, params: Optional[Dict] = None, bind_key: Optional[str] = None) -> List[Dict]:
        """
        执行查询SQL并返回字典列表

        Args:
            sql: SQL查询语句
            params: SQL参数
            bind_key: 绑定的数据库名称

        Returns:
            查询结果的字典列表
        """
        with self.get_session(bind_key) as session:
            result = session.execute(text(sql), params or {})
            return [dict(zip(result.keys(), row)) for row in result]

    def execute(self, sql: str, params: Optional[Dict] = None, bind_key: Optional[str] = None) -> None:
        """
        执行SQL语句（INSERT, UPDATE, DELETE等）

        Args:
            sql: SQL语句
            params: SQL参数
            bind_key: 绑定的数据库名称
        """
        with self.get_session(bind_key) as session:
            session.execute(text(sql), params or {})

    def get_engine(self, bind_key: Optional[str] = None):
        """获取指定的数据库引擎"""
        return self.bind_engines.get(bind_key) if bind_key else self.engine

    def reflect_table(self, table_name: str, bind_key: Optional[str] = None, schema: Optional[str] = None) -> Table:
        """
        反射数据库表结构

        Args:
            table_name: 表名
            bind_key: 绑定的数据库名称
            schema: 数据库schema名称

        Returns:
            SQLAlchemy Table对象
        """
        engine = self.get_engine(bind_key)
        metadata = self.metadatas.get(bind_key, self.metadatas[None])

        table = Table(table_name, metadata, autoload_with=engine, schema=schema)
        return table

    def close(self):
        """关闭所有数据库连接"""
        self.engine.dispose()
        for engine in self.bind_engines.values():
            engine.dispose()


class ModelOperations:
    """模型操作工具类"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def filter_fields(self, model: Type, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        过滤字典，只保留模型中存在的字段

        Args:
            model: SQLAlchemy模型类
            data: 原始数据字典

        Returns:
            过滤后的字典
        """
        if hasattr(model, '__table__'):
            allowed_fields = {col.name for col in model.__table__.columns}
            return {key: value for key, value in data.items() if key in allowed_fields}
        return data

    def normalize_filter(self, model: Type, filters: Dict[str, Any]) -> Any:
        """
        将字典形式的过滤条件转换为SQLAlchemy查询条件

        支持的操作符:
        - gt: 大于
        - gte: 大于等于
        - lt: 小于
        - lte: 小于等于
        - ne: 不等于
        - in: 在列表中
        - like: 模糊匹配
        - ilike: 不区分大小写的模糊匹配

        Args:
            model: SQLAlchemy模型类
            filters: 过滤条件字典，如 {'age__gte': 18, 'name__like': '%John%'}

        Returns:
            SQLAlchemy查询条件
        """
        conditions = []
        for key, value in filters.items():
            field_name, _, op = key.partition('__')
            field = getattr(model, field_name, None)

            if field is None:
                continue

            if op == 'gt':
                conditions.append(field > value)
            elif op == 'gte':
                conditions.append(field >= value)
            elif op == 'lt':
                conditions.append(field < value)
            elif op == 'lte':
                conditions.append(field <= value)
            elif op == 'ne':
                conditions.append(field != value)
            elif op == 'in':
                conditions.append(field.in_(value))
            elif op == 'like':
                conditions.append(field.like(value))
            elif op == 'ilike':
                conditions.append(field.ilike(value))
            else:
                # 默认是等于
                conditions.append(field == value)

        return and_(True, *conditions) if conditions else and_(True)

    def to_dict(self, model_instance: Any, follow: Optional[Set[str]] = None,
                exclude: Optional[Set[str]] = None) -> Optional[Dict]:
        """
        将模型实例转换为字典

        Args:
            model_instance: 模型实例
            follow: 需要序列化的关联关系字段集合
            exclude: 需要排除的字段集合

        Returns:
            字典或None
        """
        if model_instance is None:
            return None

        follow = follow or set()
        exclude = exclude or set()
        result = OrderedDict()

        # 序列化列字段
        for key, column in model_instance.__mapper__.columns.items():
            if key not in exclude:
                value = getattr(model_instance, key)
                result[key] = value

        # 序列化关系字段
        if follow:
            for relation in model_instance.__mapper__.relationships:
                if relation.key in follow and relation.key not in exclude:
                    related_obj = getattr(model_instance, relation.key)
                    if related_obj is not None:
                        if relation.uselist:
                            result[relation.key] = [
                                self.to_dict(item, follow=follow, exclude=exclude)
                                for item in related_obj
                            ]
                        else:
                            result[relation.key] = self.to_dict(
                                related_obj, follow=follow, exclude=exclude
                            )

        return result

    def update_or_create(self, model: Type, lookup: Dict[str, Any],
                         defaults: Optional[Dict[str, Any]] = None,
                         create_only: Optional[Set[str]] = None,
                         bind_key: Optional[str] = None) -> Dict:
        """
        查找或创建记录，如果存在则更新

        Args:
            model: SQLAlchemy模型类
            lookup: 查找条件
            defaults: 更新或创建时的默认值
            create_only: 仅在创建时设置的字段集合
            bind_key: 绑定的数据库名称

        Returns:
            模型实例的字典表示
        """
        defaults = defaults or {}
        create_only = create_only or set()

        with self.db_manager.get_session(bind_key) as session:
            # 查找记录
            instance = session.query(model).filter_by(**lookup).first()

            if instance:
                # 更新记录（排除create_only字段）
                update_data = {k: v for k, v in defaults.items() if k not in create_only}
                for key, value in update_data.items():
                    setattr(instance, key, value)
            else:
                # 创建新记录
                create_data = {**lookup, **defaults}
                instance = model(**create_data)
                session.add(instance)

            session.flush()
            return self.to_dict(instance)

    def bulk_insert(self, model: Type, data_list: List[Dict[str, Any]],
                    bind_key: Optional[str] = None, batch_size: int = 1000) -> int:
        """
        批量插入数据

        Args:
            model: SQLAlchemy模型类
            data_list: 数据字典列表
            bind_key: 绑定的数据库名称
            batch_size: 每批次插入的数量

        Returns:
            插入的记录数
        """
        total = 0
        with self.db_manager.get_session(bind_key) as session:
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i + batch_size]
                instances = [model(**data) for data in batch]
                session.bulk_save_objects(instances)
                total += len(instances)
        return total
