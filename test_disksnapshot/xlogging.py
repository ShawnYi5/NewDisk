import argparse
import inspect
import logging
import os
import sys

LOGGER_DIR_FLAG = "-logger_dir"


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


def getLogger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    return logger


class LogicError(Exception):
    def __init__(self, returned_code):
        self.returned_code = returned_code


# remark：
#   1.继承后需要提供 work_real 方法
#   2.异常与进程返回值
#       如果 work_real 方法中不抛出任何异常，则进程返回 0
#       如果 work_real 中的逻辑需要报告错误，且影响进程返回值，请调用 raise_logic_error 方法
#       如果 work_real 中抛出 LogicError 以外的异常，则进程返回构造时的 error_returned_code
#   3.如果需要打印日志，请使用 self.logger
class WorkWithLogger(object):
    # name 模块名，英文无空格，符合文件名规则
    # error_returned_code 当发生内部错误时，进程返回的错误码
    def __init__(self, name, error_returned_code):
        self.error_returned_code = error_returned_code
        self.logger = getLogger(name)

        args_parser = argparse.ArgumentParser(description="restore iso main logic")
        args_parser.add_argument(LOGGER_DIR_FLAG, help="logger directory path", default="None")
        cmd_args = args_parser.parse_args()

        self.logger.info(r'logger directory path : {}'.format(cmd_args.logger_dir))
        self.logger_dir = cmd_args.logger_dir

        if cmd_args.logger_dir != "None" and os.path.isdir(cmd_args.logger_dir):
            log_path = os.path.join(cmd_args.logger_dir, name + r'.log')
            fh = logging.FileHandler(log_path)
            fmt = "%(asctime)-15s %(levelname)s %(filename)s %(lineno)d %(process)d %(message)s"
            datefmt = "%a %d %b %Y %H:%M:%S"
            formatter = logging.Formatter(fmt, datefmt)
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)

        self.logger.info('start ...')

    def work(self):
        try:
            self.work_real()
            returned = 0
        except LogicError as le:
            returned = le.returned_code
        except Exception as e:
            self.logger.error(r'call work real failed : {}'.format(e), exc_info=True)
            returned = self.error_returned_code

        self.logger.info(r'work return : {}'.format(returned))
        sys.exit(returned)

    # msg 日志信息
    # returned_code 进程返回值
    # remark ：
    #   该方法自动记录调用函数的函数名等信息
    def raise_logic_error(self, msg, returned_code):
        function_info = _get_front_back_function_info()
        function_name = function_info[0]
        file_line = function_info[1]

        log_msg = r'{function_name}({file_line}):{msg}' \
            .format(function_name=function_name, file_line=file_line, msg=msg)
        self.logger.error(log_msg + ' args:{}'.format(function_info[3]))
        self.logger.warning(r'returned_code : {}'.format(returned_code))
        raise LogicError(returned_code)


class DataHolder(object):
    def __init__(self, value=None):
        self.value = value

    def set(self, value):
        self.value = value
        return value

    def get(self):
        return self.value
