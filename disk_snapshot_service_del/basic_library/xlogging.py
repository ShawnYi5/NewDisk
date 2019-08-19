# coding:utf-8
import inspect
import logging
import logging.config
import os

from basic_library import loadIce, xdata

__all__ = [r'raise_and_logging_error', r'getLogger']

config_path = os.path.join(loadIce.current_dir, 'logging.conf')
logging.config.fileConfig(config_path)

root_path = os.path.join(loadIce.current_dir, '..')


def _get_front_back_function_info():
    class_name = ''

    frame = inspect.currentframe().f_back.f_back  # 需要回溯两层
    arg_values = inspect.getargvalues(frame)
    args, _, _, value_dict = arg_values
    # we check the first parameter for the frame function is
    # named 'self'
    if len(args) and args[0] == 'self':
        # in that case, 'self' will be referenced in value_dict
        instance = value_dict.get('self', None)
        if instance:
            class_name = getattr(instance, '__class__', None).__name__
            class_name += '.'

    module_name = inspect.getmodule(frame).__name__

    return class_name + frame.f_code.co_name, frame.f_lineno, module_name, arg_values


def _print_dict_exclude_many_binary(info, obj):
    if obj:
        for k, v in obj.items():
            if isinstance(v, (bytearray, bytes)):
                info.append(f'"{k}":{v[:32]}…… ')
            else:
                info.append(f'"{k}":{v} ')
    else:
        info.append('None ')


def raise_and_logging_error(
        msg, debug, http_status=xdata.ERROR_HTTP_STATUS_DEFAULT, logger=None, print_args=True, function_name=None,
        file_line=None, is_log=True, exc_info=False, exception_class=xdata.DSSException, logger_level='error'
):
    """抛出并记录错误

    :param msg: 参考DashboardException
    :param debug: 参考DashboardException
    :param http_status: 参考DashboardException
    :param logger: 打印调试的logger对象，建议使用logging.get_logger(__name__)，当不传入时，将通过调用栈自动获取
    :param print_args: 打印调用上下文
    :param function_name: 参考DashboardException，当不传入时，将通过调用栈自动获取
    :param file_line: 参考DashboardException，当不传入时，将通过调用栈自动获取
    :param is_log: 标记为已经打印调用栈
    :param exc_info: 是否启用 error 的 exc_info
    :param exception_class: 异常类 / 构造函数
    :param logger_level: 调试日志等级，支持 'error' 'warning' 'info' 'debug'
    """
    function_info = None
    if (function_name is None) or (file_line is None) or print_args or (logger is None):
        function_info = _get_front_back_function_info()
        if function_name is None:
            function_name = function_info[0]
        if file_line is None:
            file_line = function_info[1]
        if logger is None:
            logger = logging.getLogger(function_info[2])

    _log_msg = r'{function_name}({file_line}):{msg} debug:{debug}' \
        .format(function_name=function_name, file_line=file_line, msg=msg, debug=debug)

    if print_args:
        args_info = [f' args:args={function_info[3].args} varargs={function_info[3].varargs} keywords=', ]
        _print_dict_exclude_many_binary(args_info, function_info[3].keywords)
        args_info.append('locals=')
        _print_dict_exclude_many_binary(args_info, function_info[3].locals)
        _log_msg = f'{_log_msg}{"".join(args_info)}'
    else:
        pass  # do nothing

    if logger_level == 'debug':
        logger.debug(_log_msg, exc_info=exc_info)
    elif logger_level == 'warning':
        logger.warning(_log_msg, exc_info=exc_info)
    elif logger_level == 'info':
        logger.info(_log_msg, exc_info=exc_info)
    else:
        logger.error(_log_msg, exc_info=exc_info)

    raise exception_class(function_name, msg, debug, file_line, http_status, is_log)


def getLogger(name, log=True):
    if log:
        logging.getLogger(__name__).warning(f'get logger : {name}')
    return logging.getLogger(name)
