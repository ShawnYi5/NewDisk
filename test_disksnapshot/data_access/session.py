import contextlib

import sqlalchemy
from sqlalchemy import orm

db_connect_str = 'postgresql+psycopg2://postgres:f@127.0.0.1:21114/disksnapshotservice'
"""
使用scoped_session简化代码
注意：在“业务线程”退出时通过remove释放session；建议使用scoped_session_thread装饰器辅助释放
"""
engine = sqlalchemy.create_engine(db_connect_str, echo=False, max_overflow=1024)
session_maker = orm.scoped_session(orm.sessionmaker(bind=engine))


def get_scoped_session():
    """返回线程关联的session"""
    return session_maker()


def scoped_session_thread(func):
    """装饰器，辅助线程释放session对象"""

    def wrapper(*args, **kwargs):
        s = get_scoped_session()
        try:
            return func(*args, **kwargs)
        finally:
            s.remove()

    return wrapper


@contextlib.contextmanager
def transaction():
    session = get_scoped_session()
    session.begin_nested()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    else:
        session.commit()


@contextlib.contextmanager
def readonly():
    session = get_scoped_session()
    session.begin_nested()
    try:
        yield session
    finally:
        session.rollback()
