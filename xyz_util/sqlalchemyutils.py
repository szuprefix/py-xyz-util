import os
from sqlalchemy import create_engine, text, and_, not_, select, func, true, MetaData, Table
from sqlalchemy.orm import sessionmaker, scoped_session
from contextlib import contextmanager
from collections import OrderedDict
from typing import Any, Dict, Iterator, List, Optional, Set, Type, Union, Tuple

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
            bind_engine = db.engines[base] if base else db.engine
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

def load_config():
    from .datautils import str2dict
    conn = os.getenv('SQLALCHEMY_URI', 'mysql+pymysql://localhost')
    bs = str2dict(os.getenv('SQLALCHEMY_BINDS', ''))
    return conn, bs


class DatabaseManager:
    """通用的数据库管理器，支持单数据库和多数据库配置"""

    def _create_engine(self, uri):
        kwargs = {
            "echo": self.echo,
            "pool_pre_ping": True,
        }

        # SQLite 默认使用 SingletonThreadPool，
        # 不支持 pool_size / max_overflow
        if not uri.startswith("sqlite"):
            kwargs.update({
                "pool_size": self.pool_size,
                "max_overflow": self.max_overflow,
            })

        return create_engine(uri, **kwargs)

    def __init__(
            self,
            database_uri: str = None,
            binds: Optional[Dict[str, str]] = None,
            echo: bool = False,
            pool_size: int = 5,
            max_overflow: int = 10
    ):
        if not database_uri and not binds:
            database_uri, binds = load_config()
        self.database_uri = database_uri
        self.binds = binds or {}
        self.echo = echo
        self.pool_size = pool_size
        self.max_overflow = max_overflow

        self.engine = self._create_engine(database_uri)

        self.bind_engines = {}
        for name, uri in self.binds.items():
            self.bind_engines[name] = self._create_engine(uri)

        self.SessionLocal = scoped_session(
            sessionmaker(bind=self.engine)
        )

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
        # 保留原生 Table 返回值，同时提供 Django 风格的 objects manager。
        # 每次反射时重新绑定，确保 bind_key 与当前调用一致。
        table.objects = ModelManager(ModelOperations(self), table, bind_key)
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

    def query(self, model: Type, bind_key: Optional[str] = None) -> 'QuerySet':
        """创建一个 Django 风格、可链式组合的查询集。"""
        return QuerySet(self, model, bind_key=bind_key)

    @staticmethod
    def model_name(model: Type) -> str:
        """返回 ORM 模型类或 Table 的可读名称。"""
        return getattr(model, '__name__', getattr(model, 'name', repr(model)))

    @staticmethod
    def column_names(model: Type) -> Tuple[str, ...]:
        """返回 ORM 模型类或 Table 的列属性名。"""
        if hasattr(model, '__mapper__'):
            return tuple(model.__mapper__.column_attrs.keys())
        if hasattr(model, 'c'):
            return tuple(model.c.keys())
        raise TypeError(f'不支持的查询对象: {model!r}')

    @staticmethod
    def model_field(model: Type, field_name: str) -> Any:
        """取得 ORM 属性或 Table 列，不存在时返回 None。"""
        if hasattr(model, '__mapper__'):
            if field_name in model.__mapper__.column_attrs:
                return getattr(model, field_name)
        elif hasattr(model, 'c') and field_name in model.c:
            return model.c[field_name]
        return None

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
        - contains / icontains: 包含
        - startswith / istartswith: 以指定文本开头
        - endswith / iendswith: 以指定文本结尾
        - isnull: 是否为NULL

        Args:
            model: SQLAlchemy模型类
            filters: 过滤条件字典，如 {'age__gte': 18, 'name__like': '%John%'}

        Returns:
            SQLAlchemy查询条件
        """
        operators = {
            'exact': lambda field, value: field == value,
            'ne': lambda field, value: field != value,
            'gt': lambda field, value: field > value,
            'gte': lambda field, value: field >= value,
            'lt': lambda field, value: field < value,
            'lte': lambda field, value: field <= value,
            'in': lambda field, value: field.in_(value),
            'like': lambda field, value: field.like(value),
            'ilike': lambda field, value: field.ilike(value),
            'contains': lambda field, value: field.contains(value),
            'icontains': lambda field, value: field.ilike(f'%{value}%'),
            'startswith': lambda field, value: field.startswith(value),
            'istartswith': lambda field, value: field.ilike(f'{value}%'),
            'endswith': lambda field, value: field.endswith(value),
            'iendswith': lambda field, value: field.ilike(f'%{value}'),
            'isnull': lambda field, value: (
                field.is_(None) if value else field.is_not(None)
            ),
        }
        conditions = []
        for key, value in filters.items():
            parts = key.split('__')
            if len(parts) > 2:
                raise ValueError(f'暂不支持跨关系过滤: {key!r}')

            field_name = parts[0]
            op = parts[1] if len(parts) == 2 else 'exact'
            field = self.model_field(model, field_name)

            if field is None:
                raise ValueError(
                    f'{self.model_name(model)} 没有字段 {field_name!r}'
                )
            if op not in operators:
                raise ValueError(f'不支持过滤操作符 {op!r}: {key!r}')
            if op == 'isnull' and not isinstance(value, bool):
                raise ValueError(f'{key!r} 的值必须是 bool')
            if op == 'in' and isinstance(value, (str, bytes)):
                raise ValueError(f'{key!r} 的值必须是非字符串可迭代对象')

            conditions.append(operators[op](field, value))

        return and_(*conditions) if conditions else true()

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


class Aggregate:
    """聚合表达式基类。"""

    function_name = None
    field_required = True

    def __init__(self, field: Optional[str] = None,
                 distinct: bool = False, default: Any = None):
        if self.field_required and not field:
            raise ValueError(f'{type(self).__name__} 必须指定字段')
        self.field = None if field == '*' else field
        self.distinct = distinct
        self.default = default

    def expression(self, operations: ModelOperations, model: Type):
        if self.field is None:
            argument = None
        else:
            argument = operations.model_field(model, self.field)
            if argument is None:
                raise ValueError(
                    f'{operations.model_name(model)} 没有字段 {self.field!r}'
                )
            if self.distinct:
                argument = argument.distinct()

        function = getattr(func, self.function_name)
        expression = function() if argument is None else function(argument)
        if self.default is not None:
            expression = func.coalesce(expression, self.default)
        return expression


class Count(Aggregate):
    function_name = 'count'
    field_required = False


class Sum(Aggregate):
    function_name = 'sum'


class Avg(Aggregate):
    function_name = 'avg'


class Min(Aggregate):
    function_name = 'min'


class Max(Aggregate):
    function_name = 'max'


class ModelManager:
    """Django 风格的模型管理器，每次调用都从新的 QuerySet 开始。"""

    def __init__(self, operations: ModelOperations, model: Type,
                 bind_key: Optional[str] = None):
        self.operations = operations
        self.model = model
        self.bind_key = bind_key

    def get_queryset(self) -> 'QuerySet':
        return self.operations.query(self.model, self.bind_key)

    def using(self, bind_key: Optional[str]) -> 'ModelManager':
        """返回使用另一个数据库绑定的新 manager。"""
        return type(self)(self.operations, self.model, bind_key)

    def filter(self, **filters) -> 'QuerySet':
        return self.get_queryset().filter(**filters)

    def exclude(self, **filters) -> 'QuerySet':
        return self.get_queryset().exclude(**filters)

    def order_by(self, *fields) -> 'QuerySet':
        return self.get_queryset().order_by(*fields)

    def all(self) -> List[Dict]:
        return self.get_queryset().all()

    def first(self) -> Optional[Dict]:
        return self.get_queryset().first()

    def one(self) -> Dict:
        return self.get_queryset().one()

    def get(self, **filters) -> Dict:
        return self.get_queryset().get(**filters)

    def count(self) -> int:
        return self.get_queryset().count()

    def exists(self) -> bool:
        return self.get_queryset().exists()

    def values(self, *fields: str) -> List[Dict]:
        return self.get_queryset().values(*fields)

    def values_list(self, *fields: str, flat: bool = False) -> List[Any]:
        return self.get_queryset().values_list(*fields, flat=flat)

    def aggregate(self, **aggregates) -> Dict:
        return self.get_queryset().aggregate(**aggregates)

    def group_by(self, *fields: str) -> 'GroupedQuerySet':
        return self.get_queryset().group_by(*fields)

    def _create_with_session(self, session, data: Dict[str, Any]) -> Dict:
        """使用已有 session 创建一条记录并返回字典。"""
        if hasattr(self.model, '__mapper__'):
            instance = self.model(**data)
            session.add(instance)
            session.flush()
            return self.operations.to_dict(instance)

        if not hasattr(self.model, 'insert'):
            raise TypeError(f'不支持的模型对象: {self.model!r}')

        result = session.execute(self.model.insert().values(**data))
        primary_key = tuple(self.model.primary_key.columns)
        inserted_key = tuple(result.inserted_primary_key)

        if primary_key and all(value is not None for value in inserted_key):
            conditions = [
                column == value
                for column, value in zip(primary_key, inserted_key)
            ]
            row = session.execute(
                select(self.model).where(*conditions)
            ).one()
            return dict(row._mapping)

        # 没有可用于重新查询的主键时，至少返回调用方提交的数据。
        return dict(data)

    def create(self, **data) -> Dict:
        """创建一条记录，返回包含主键和数据库默认值的字典。"""
        with self.operations.db_manager.get_session(self.bind_key) as session:
            return self._create_with_session(session, data)

    def bulk_create(self, data_list: List[Dict[str, Any]]) -> List[Dict]:
        """在一个事务中创建多条记录，并按输入顺序返回字典列表。"""
        if not data_list:
            return []
        with self.operations.db_manager.get_session(self.bind_key) as session:
            return [
                self._create_with_session(session, dict(data))
                for data in data_list
            ]


class QuerySet:
    """轻量、不可变的 Django 风格 SQLAlchemy 查询构造器。

    查询在 ``all``、``first``、``one``、``count`` 或 ``exists`` 等终结
    方法被调用时才执行。结果默认转为字典，避免返回已脱离 session 的实例。
    """

    def __init__(self, operations: ModelOperations, model: Type,
                 bind_key: Optional[str] = None,
                 conditions: Tuple[Any, ...] = (),
                 ordering: Tuple[Any, ...] = (),
                 limit_value: Optional[int] = None,
                 offset_value: Optional[int] = None):
        self.operations = operations
        self.model = model
        self.bind_key = bind_key
        self._conditions = conditions
        self._ordering = ordering
        self._limit_value = limit_value
        self._offset_value = offset_value

    def _clone(self, **changes) -> 'QuerySet':
        values = {
            'operations': self.operations,
            'model': self.model,
            'bind_key': self.bind_key,
            'conditions': self._conditions,
            'ordering': self._ordering,
            'limit_value': self._limit_value,
            'offset_value': self._offset_value,
        }
        values.update(changes)
        return type(self)(**values)

    def __iter__(self) -> Iterator[Dict]:
        """执行查询并迭代字典结果。"""
        return iter(self.all())

    def __getitem__(self, key: Union[int, slice]) -> Union[Dict, 'QuerySet', List[Dict]]:
        """支持 Django 风格的索引和切片查询。"""
        if isinstance(key, int):
            if key < 0:
                raise ValueError('QuerySet 不支持负索引')
            if self._limit_value is not None and key >= self._limit_value:
                raise IndexError('QuerySet index out of range')

            offset = (self._offset_value or 0) + key
            item = self._clone(offset_value=offset, limit_value=1).first()
            if item is None:
                raise IndexError('QuerySet index out of range')
            return item

        if not isinstance(key, slice):
            raise TypeError('QuerySet 索引必须是整数或切片')

        start = 0 if key.start is None else key.start
        stop = key.stop
        if start < 0 or (stop is not None and stop < 0):
            raise ValueError('QuerySet 不支持负索引切片')
        if stop is not None and stop < start:
            stop = start

        # 与 Django 一致，带步长的切片会立即求值并返回列表。
        if key.step is not None and key.step != 1:
            return self.all()[key]

        base_offset = self._offset_value or 0
        new_offset = base_offset + start
        available = None
        if self._limit_value is not None:
            available = max(self._limit_value - start, 0)

        requested = None if stop is None else max(stop - start, 0)
        if available is None:
            new_limit = requested
        elif requested is None:
            new_limit = available
        else:
            new_limit = min(available, requested)

        return self._clone(offset_value=new_offset, limit_value=new_limit)

    def filter(self, **filters) -> 'QuerySet':
        condition = self.operations.normalize_filter(self.model, filters)
        if not filters:
            return self
        return self._clone(conditions=self._conditions + (condition,))

    def exclude(self, **filters) -> 'QuerySet':
        condition = self.operations.normalize_filter(self.model, filters)
        if not filters:
            return self
        return self._clone(conditions=self._conditions + (not_(condition),))

    def order_by(self, *fields) -> 'QuerySet':
        ordering = []
        for value in fields:
            if not isinstance(value, str):
                ordering.append(value)
                continue

            descending = value.startswith('-')
            field_name = value[1:] if descending else value
            field = self.operations.model_field(self.model, field_name)
            if field is None:
                raise ValueError(
                    f'{self.operations.model_name(self.model)} '
                    f'没有字段 {field_name!r}'
                )
            ordering.append(field.desc() if descending else field.asc())
        return self._clone(ordering=tuple(ordering))

    def limit(self, value: Optional[int]) -> 'QuerySet':
        if value is not None and (not isinstance(value, int) or value < 0):
            raise ValueError('limit 必须是非负整数或 None')
        return self._clone(limit_value=value)

    def offset(self, value: Optional[int]) -> 'QuerySet':
        if value is not None and (not isinstance(value, int) or value < 0):
            raise ValueError('offset 必须是非负整数或 None')
        return self._clone(offset_value=value)

    def _statement(self):
        statement = select(self.model)
        if self._conditions:
            statement = statement.where(*self._conditions)
        if self._ordering:
            statement = statement.order_by(*self._ordering)
        if self._limit_value is not None:
            statement = statement.limit(self._limit_value)
        if self._offset_value is not None:
            statement = statement.offset(self._offset_value)
        return statement

    def _serialize(self, instance: Any) -> Dict:
        return self.operations.to_dict(instance)

    def _is_orm_model(self) -> bool:
        return hasattr(self.model, '__mapper__')

    def _result_dict(self, row: Any) -> Dict:
        return dict(row._mapping)

    def all(self) -> List[Dict]:
        with self.operations.db_manager.get_session(self.bind_key) as session:
            result = session.execute(self._statement())
            if self._is_orm_model():
                return [self._serialize(item) for item in result.scalars()]
            return [self._result_dict(row) for row in result]

    def first(self) -> Optional[Dict]:
        limit_value = 1 if self._limit_value is None else min(self._limit_value, 1)
        queryset = self.limit(limit_value)
        with self.operations.db_manager.get_session(self.bind_key) as session:
            result = session.execute(queryset._statement())
            if self._is_orm_model():
                instance = result.scalars().first()
                return self._serialize(instance) if instance is not None else None
            row = result.first()
            return self._result_dict(row) if row is not None else None

    def one(self) -> Dict:
        with self.operations.db_manager.get_session(self.bind_key) as session:
            result = session.execute(self._statement())
            if self._is_orm_model():
                return self._serialize(result.scalars().one())
            return self._result_dict(result.one())

    def get(self, **filters) -> Dict:
        """追加过滤条件并返回唯一记录，否则抛出 SQLAlchemy 标准异常。"""
        return self.filter(**filters).one()

    def count(self) -> int:
        count_statement = select(func.count()).select_from(self.model)
        if self._conditions:
            count_statement = count_statement.where(*self._conditions)
        with self.operations.db_manager.get_session(self.bind_key) as session:
            return session.execute(count_statement).scalar_one()

    def exists(self) -> bool:
        statement = select(self.model).where(*self._conditions).limit(1)
        with self.operations.db_manager.get_session(self.bind_key) as session:
            return session.execute(statement).first() is not None

    def _ensure_unsliced_for_aggregation(self) -> None:
        if self._limit_value is not None or self._offset_value is not None:
            raise ValueError('暂不支持对已执行 limit/offset 的 QuerySet 聚合')

    def _aggregate_columns(self, aggregates: Dict[str, Aggregate]) -> List[Any]:
        if not aggregates:
            raise ValueError('至少需要指定一个聚合表达式')

        columns = []
        for alias, aggregate in aggregates.items():
            if not isinstance(alias, str) or not alias:
                raise ValueError('聚合别名必须是非空字符串')
            if not isinstance(aggregate, Aggregate):
                raise TypeError(
                    f'聚合项 {alias!r} 必须是 Aggregate 表达式'
                )
            columns.append(
                aggregate.expression(self.operations, self.model).label(alias)
            )
        return columns

    def aggregate(self, **aggregates) -> Dict:
        """对当前过滤结果执行聚合，返回单个字典。"""
        self._ensure_unsliced_for_aggregation()
        statement = select(*self._aggregate_columns(aggregates)).select_from(
            self.model
        )
        if self._conditions:
            statement = statement.where(*self._conditions)
        with self.operations.db_manager.get_session(self.bind_key) as session:
            return dict(session.execute(statement).one()._mapping)

    def group_by(self, *fields: str) -> 'GroupedQuerySet':
        """按一个或多个字段分组，等待 aggregate() 执行统计。"""
        self._ensure_unsliced_for_aggregation()
        if not fields:
            raise ValueError('group_by() 至少需要指定一个字段')
        self._validate_value_fields(tuple(fields))
        return GroupedQuerySet(self, tuple(fields))

    def _validate_value_fields(self, fields: Tuple[str, ...]) -> None:
        column_names = self.operations.column_names(self.model)
        for field_name in fields:
            if field_name not in column_names:
                raise ValueError(
                    f'{self.operations.model_name(self.model)} '
                    f'没有字段 {field_name!r}'
                )

    def values(self, *fields: str) -> List[Dict]:
        field_names = fields or self.operations.column_names(self.model)
        self._validate_value_fields(field_names)
        columns = [
            self.operations.model_field(self.model, name)
            for name in field_names
        ]
        statement = select(*columns)
        if self._conditions:
            statement = statement.where(*self._conditions)
        if self._ordering:
            statement = statement.order_by(*self._ordering)
        if self._limit_value is not None:
            statement = statement.limit(self._limit_value)
        if self._offset_value is not None:
            statement = statement.offset(self._offset_value)
        with self.operations.db_manager.get_session(self.bind_key) as session:
            return [dict(row._mapping) for row in session.execute(statement)]

    def values_list(self, *fields: str, flat: bool = False) -> List[Any]:
        if flat and len(fields) != 1:
            raise ValueError('flat=True 时必须且只能指定一个字段')
        rows = self.values(*fields)
        if flat:
            return [row[fields[0]] for row in rows]
        field_names = fields or self.operations.column_names(self.model)
        return [tuple(row[name] for name in field_names) for row in rows]


class GroupedQuerySet:
    """保存分组字段，并在 aggregate() 时执行查询。"""

    def __init__(self, queryset: QuerySet, fields: Tuple[str, ...]):
        self.queryset = queryset
        self.fields = fields

    def aggregate(self, **aggregates) -> List[Dict]:
        collisions = set(self.fields).intersection(aggregates)
        if collisions:
            names = ', '.join(sorted(collisions))
            raise ValueError(f'聚合别名不能覆盖分组字段: {names}')

        queryset = self.queryset
        group_columns = [
            queryset.operations.model_field(queryset.model, field)
            for field in self.fields
        ]
        aggregate_columns = queryset._aggregate_columns(aggregates)
        statement = select(
            *group_columns, *aggregate_columns
        ).select_from(queryset.model)
        if queryset._conditions:
            statement = statement.where(*queryset._conditions)
        statement = statement.group_by(*group_columns)

        with queryset.operations.db_manager.get_session(
                queryset.bind_key) as session:
            return [
                dict(row._mapping)
                for row in session.execute(statement)
            ]
