import os
import sys
import threading
import time
import traceback

from basic_library import xlogging

_logger = xlogging.getLogger(__name__)

dump_key_status_fn = None


class XDebugHelper(threading.Thread):

    TIMER_INTERVAL_SECS = 10
    DUMP_ALL_THREAD_STACK_FILE = r'/run/dump_dss_stack'
    DUMP_KEY_STATUS_THREAD_STACK_FILE = r'/run/dump_dss_key_status_stack'

    def __init__(self):
        threading.Thread.__init__(self, name='xdebug')

    def run(self):
        while True:
            try:
                self.do_run()
                break
            except Exception as e:
                _logger.error(r'XDebugHelper run Exception : {}'.format(e), exc_info=True)

    def do_run(self):
        while True:
            time.sleep(self.TIMER_INTERVAL_SECS)

            self.dump_all_thread_stack_when_file_exist()
            self.dump_key_status_thread_stack_when_file_exist()

    def dump_all_thread_stack_when_file_exist(self):
        try:
            if not os.path.isfile(self.DUMP_ALL_THREAD_STACK_FILE):
                return
            self.dump_all_thread_stack()
        except Exception as e:
            _logger.error(r'XDebugHelper dump_all_thread_stack_when_file_exist Exception : {}'.format(e), exc_info=True)

    def dump_key_status_thread_stack_when_file_exist(self):
        try:
            if not os.path.isfile(self.DUMP_KEY_STATUS_THREAD_STACK_FILE) or dump_key_status_fn is None:
                return
            _logger.info('{}nas备份任务关键运行状态：{}{}'.format(
                os.linesep, os.linesep, dump_key_status_fn()))
        except Exception as e:
            _logger.error(r'XDebugHelper dump_all_thread_stack_when_file_exist Exception : {}'.format(e), exc_info=True)

    @staticmethod
    def dump_all_thread_stack():
        id2name = dict((th.ident, th.name) for th in threading.enumerate())
        for thread_id, stack in sys._current_frames().items():
            _logger.info('Thread {} - {}\n>{}'
                         .format(thread_id, id2name[thread_id], '>'.join(traceback.format_stack(stack))))
