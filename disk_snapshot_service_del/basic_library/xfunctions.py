import decimal
import functools
import inspect
import time

import Ice
import arrow
from dateutil import tz
from rest_framework import status
from rest_framework.exceptions import ValidationError
from rest_framework.response import Response

from basic_library import loadIce
from basic_library import xdata
from basic_library import xlogging

_logger = xlogging.getLogger(__name__)


def _get_front_back_instance():
    frame = inspect.currentframe().f_back.f_back  # 需要回溯两层
    _, _, _, value_dict = inspect.getargvalues(frame)
    return value_dict.get('self')  # 不做容错，调用者保证


class DecorateClass(object):
    """为类方法添加装饰器的基类"""

    def iter(self):
        raise NotImplementedError()

    def operate(self, name, fn):
        raise NotImplementedError()

    def decorate(self):
        for name, fn in self.iter():
            if callable(fn):
                self.operate(name, fn)


class ApiViewExceptionHandlerDecorator(DecorateClass):
    """为view类型提供默认的异常处理

    :remark:
        自动为 api view 的get、put、post、delete方法加入异常处理
        所有 api view 必须使用
    """

    def __init__(self, msgs=None, obj=None, logger=None):
        """
        :param obj: 对象实例，建议直接使用需要添加装饰器的对象的self
        :param logger: 日志对象，建议使用xlogging.getLogger(__name__)，当不传入时，将通过方法自动获取
        """
        self.obj = _get_front_back_instance() if obj is None else obj
        self.logger = logger
        self.msgs = msgs if msgs else dict()

    def iter(self):
        return [(name, getattr(self.obj, name)) for name in dir(self.obj) if name in ('get', 'put', 'post', 'delete')]

    def get_logger(self, module_name):
        if self.logger is not None:
            return self.logger
        else:
            return xlogging.getLogger(module_name, False)

    def get_msg(self, fn_name):
        return self.msgs.get(fn_name, '接口异常')

    def operate(self, name, fn):
        @functools.wraps(fn)
        def handler(*args, **kv):
            try:
                result = fn(*args, **kv)
                if not isinstance(result, Response):
                    s = f'{fn.__qualname__} api view return not Response : {result}'
                    self.get_logger(fn.__module__).error(s)
                    result = Response(data={'detail': s}, status=xdata.ERROR_HTTP_STATUS_DEFAULT)
                return result
            except ValidationError as ve:
                s = f'{fn.__qualname__} api view ValidationError :{ve}'
                self.get_logger(fn.__module__).error(s, exc_info=True)
                return Response(data={'detail': s}, status=xdata.ERROR_HTTP_STATUS_VALIDATION_ERROR)
            except xdata.DSSException as de:
                s = f'{fn.__qualname__} api view DSSException:{de.msg} debug:{de.debug}'
                self.get_logger(fn.__module__).error(s, exc_info=(not de.is_log))
                msg = self.get_msg(fn.__name__)
                return Response(data={'detail': f'{msg}：{de.msg}', 'debug': s}, status=de.http_status)
            except Exception as e:
                s = f'{fn.__qualname__} api view Exception :{e}'
                self.get_logger(fn.__module__).error(s, exc_info=True)
                return Response(data={'detail': f'接口异常', 'debug': s}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        setattr(self.obj, name, handler)


class ExceptionHandlerDecorator(DecorateClass):
    """处理其他模块抛出异常的默认处理

    :remark:
        自动为公共方法加入异常处理
        调用其他组件的封装类必须使用
    """

    def __init__(self, msg_define=None, obj=None, logger=None):
        """
        :param obj: 对象实例，建议直接使用需要添加装饰器的对象的self
        :param logger: 日志对象，建议使用xlogging.getLogger(__name__)，当不传入时，将通过方法自动获取
        """
        self.obj = _get_front_back_instance() if obj is None else obj
        self.logger = logger
        self.msg_define = msg_define if msg_define else dict()

    def iter(self):
        return [(name, getattr(self.obj, name)) for name in dir(self.obj) if not name.startswith('_')]

    def get_logger(self, module_name):
        if self.logger is not None:
            return self.logger
        else:
            return xlogging.getLogger(module_name, False)

    def _get_msg_define(self, fn_name):
        define = self.msg_define.get(fn_name, None)
        if define:
            return define['msg'], define['code'], define.get('http_status', xdata.ERROR_HTTP_STATUS_DEFAULT)
        else:
            return '', xdata.ERROR_FAILED, xdata.ERROR_HTTP_STATUS_DEFAULT

    def operate(self, name, fn):
        @functools.wraps(fn)
        def handler(*args, **kv):
            try:
                return fn(*args, **kv)
            except xdata.DSSException as de:
                if not de.is_log:
                    xlogging.getLogger(fn.__module__, False).error(
                        f'{fn.__qualname__} raise DSSException:{de.msg} debug:{de.debug}', exc_info=True)
                raise
            except loadIce.ICE_UTILS.SystemError as se:
                fn_name = fn.__qualname__
                fn_line = 0
                _, _, http_status = self._get_msg_define(fn.__name__)

                debug = f'{fn_name} raise Utils.SystemError:{se.description} debug:{se.debug} raw_code:{se.rawCode}'
                xlogging.getLogger(fn.__module__, False).error(debug)
                raise xdata.DSSException(fn_name, se.description, debug, fn_line, http_status, True)
            except Ice.Exception as ie:
                fn_name = fn.__qualname__
                fn_line = 0
                debug = f'{fn_name} Ice.Exception:{ie}'
                xlogging.getLogger(fn.__module__, False).error(debug)
                msg, code, http_status = self._get_msg_define(fn.__name__)
                raise xdata.DSSException(fn_name, f'{msg}发生网络请求异常', debug, fn_line, http_status, True)
            except Exception as e:
                fn_name = fn.__qualname__
                fn_line = 1
                debug = f'{fn_name} Exception:{e}'
                self.get_logger(fn.__module__).error(debug, exc_info=True)
                msg, code, http_status = self._get_msg_define(fn.__name__)
                raise xdata.DSSException(fn_name, f'{msg}发生异常，代码{code}', debug, fn_line, http_status, True)

        setattr(self.obj, name, handler)


class TraceDecorator(DecorateClass):
    """方法调用跟踪

    :remark:
        自动为“类的公共方法”加上跟踪装饰器，“公共方法”是指不是由“_”打头的类方法
        调用其他组件的封装类必须使用
    """

    def __init__(self, ignore=None, obj=None, logger=None):
        """
        :param ignore: list 忽略列表，可忽略额外的方法
        :param obj: 对象实例，建议直接使用需要添加装饰器的对象的self，当在__init__中被调用时可不传入，内部将通过调用栈自动获取
        """
        self.ignore = list() if ignore is None else ignore
        self.obj = _get_front_back_instance() if obj is None else obj
        self.index = 0
        self.logger = logger

    def iter(self):
        return [(name, getattr(self.obj, name)) for name in dir(self.obj) if
                ((not name.startswith('_')) and (name not in self.ignore) and (not name.startswith('ice_')))]

    def operate(self, name, fn):
        @functools.wraps(fn)
        def trace(*args, **kv):
            index = self.index  # 仅仅用于打印调试无需同步
            self.index += 1

            args_exclude_bytearray = tuple(x for x in args if
                                           not isinstance(x, (bytearray, bytes)) and x is not Ice.Unset)

            kv_exclude_bytearray = {
                key: value for key, value in kv.items() if
                not isinstance(value, (bytearray, bytes)) and value is not Ice.Unset
            }

            logger = self.logger if self.logger else xlogging.getLogger(fn.__module__, False)

            logger.debug(
                r'{index}:{fn_name} input args:{args} kv:{kv}'.format(
                    index=index, fn_name=fn.__qualname__, args=args_exclude_bytearray, kv=kv_exclude_bytearray))

            returned = fn(*args, **kv)

            logger.debug(
                r'{index}:{fn_name} return:{returned}'.format(index=index, fn_name=fn.__qualname__, returned=returned))

            return returned

        setattr(self.obj, name, trace)


def convert_exception_to_value(value, logger_fn):
    """异常转换为状态

    方法装饰器：当方法内发生异常时，返回 value

    :remark:
        当需要屏蔽掉某些调用的所有异常时使用
    """

    def _real_decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kv):
            try:
                return fn(*args, **kv)
            except xdata.DSSException as de:
                if logger_fn:
                    logger_fn(f'{fn.__qualname__} raise DSSException need convert to '
                              f'{value} :{de.msg} debug:{de.debug}')
                return value
            except Exception as e:
                if logger_fn:
                    logger_fn(f'{fn.__qualname__} raise Exception need convert to {value} :{e}', exc_info=True)
                return value

        return wrapper

    return _real_decorator


def locker_guard(locker):
    """锁守卫
    方法装饰器：在进入方法前后上锁与解锁

    :remark:
        如果整个方法都需要在临界区中执行，使用该装饰器可减少代码缩进层次
    """

    def _real_decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kv):
            with locker:
                return fn(*args, **kv)

        return wrapper

    return _real_decorator


class LockerDecorator(DecorateClass):
    """锁守卫

    :remark:
        自动为“对象的公共方法”加上锁装饰器，“对象类的公共方法”是指不是由“_”打头的类方法
    """

    def __init__(self, lock, obj=None):
        """
        :param lock: 锁对象，可使用with语句的对象
        :param obj: 对象实例，建议直接使用需要添加装饰器的对象的self，当在__init__中被调用时可不传入，内部将通过调用栈自动获取
        """
        self.obj = _get_front_back_instance() if obj is None else obj
        self.lock = lock

    def iter(self):
        return [(name, getattr(self.obj, name)) for name in dir(self.obj) if not name.startswith('_')]

    def operate(self, name, fn):
        @functools.wraps(fn)
        def locker(*args, **kv):
            with self.lock:
                return fn(*args, **kv)

        setattr(self.obj, name, locker)


class DataHolder(object):
    """数据保持辅助类

    模拟C语言的在一行语句中进行赋值与比较，C语言可写为如下形式：
        if (some_value = some_func()) == spec_value:
            do_func(some_value)

    将类似的python代码进行简化，未简化前：
        some_value = some_func()
        while some_value == spec_value:
            ...
            some_value = some_func()

        简化后

        value_holder = DataHolder()
        while value_holder.set(some_func()) == spec_value:
            ...

    :remark:
        在使用该辅助类前，请考虑使用 yield
    """

    def __init__(self, value=None):
        self.value = value

    def set(self, value):
        self.value = value
        return value

    def get(self):
        return self.value


def db_ex_wrap(func, logger_fn):
    """防止重启数据库后，连接丢失的情况发生"""

    @functools.wraps(func)
    def wrap(*args, **kwargs):
        try:
            rev = func(*args, **kwargs)
        finally:
            update_db_connections(logger_fn, func.__qualname__)
        return rev

    return wrap


def update_db_connections(logger_fn, fn_name):
    try:
        from django.db import connections
        for conn in connections.all():
            conn.close_if_unusable_or_obsolete()
    except Exception as e:
        logger_fn(f'update_db_connections {fn_name} : {e}')


def convert_timestamp_float_to_decimal(timestamp: float) -> decimal.Decimal:
    return decimal.Decimal(f'{timestamp:.06f}')


def current_timestamp() -> decimal.Decimal:
    return convert_timestamp_float_to_decimal(time.time())


def humanize_timestamp(timestamp: decimal.Decimal, empty_str='') -> str:
    """格式化时间戳为人可读的描述"""
    if not timestamp:
        return empty_str

    return arrow.Arrow.fromtimestamp(timestamp, tz.tzlocal()).format('YYYY-MM-DD HH:mm:ss.SSSSSS')


def humanize_size_bytes(size_bytes, precision=2) -> str:
    """格式化容量为人可读的描述"""
    size_bytes = int(size_bytes)
    if size_bytes < 0:
        return ''
    elif size_bytes < 1024:
        return '{}B'.format(size_bytes)
    elif 1024 <= size_bytes < 1024 ** 2:
        return '{:.{precision}f}KB'.format(size_bytes / 1024, precision=precision)
    elif 1024 ** 2 <= size_bytes < 1024 ** 3:
        return '{:.{precision}f}MB'.format(size_bytes / 1024 ** 2, precision=precision)
    elif 1024 ** 3 <= size_bytes < 1024 ** 4:
        return '{:.{precision}f}GB'.format(size_bytes / 1024 ** 3, precision=precision)
    elif 1024 ** 4 <= size_bytes < 1024 ** 5:
        return '{:.{precision}f}TB'.format(size_bytes / 1024 ** 4, precision=precision)
    else:  # 1024 ** 5 <= size_bytes
        return '{:.{precision}f}PB'.format(size_bytes / 1024 ** 4, precision=precision)
