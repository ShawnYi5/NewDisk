import logging
import os
import random
import time

import client
import test_disksnapshot_complex_task as td
import create_storage_task as cst
import xlogging


_logger = xlogging.getLogger('open_task')
logging.basicConfig(filename='open_task.log', format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)


class OpenStorage(object):
    def __init__(self):
        self.storage_ident = random.sample(td.normal_guid_pool, 1)[0]
        self.handle = self.storage_ident

    def open_storage(self):
        caller_trace = 'open storage of {}'.format(self.handle)
        caller_pid = os.getpid()
        caller_pid_created = int(time.time())
        # timestamp = cst.GetStorage(self.storage_ident).start_time()
        timestamp = float(cst.GetStorage(self.storage_ident).start_time())
        open_raw_handle = False
        open_storage_params_dict = {'handle': self.handle,
                                    'caller_trace': caller_trace,
                                    'caller_pid': caller_pid,
                                    'caller_pid_created': caller_pid_created,
                                    'storage_ident': self.storage_ident,
                                    'timestamp': timestamp,
                                    'open_raw_handle': open_raw_handle}
        _logger.info('===================open storage {{}}start.=============='.format(self.handle))
        client.box_service.open_storage(open_storage_params_dict)
        _logger.info('===================open storage {{}}end.=============='.format(self.handle))
        time.sleep(random.randint(1, 3))

    def check_and_accept_the_error(self):
        pass

    def close_storage(self):
        close_storage_params_dict = {'handle': self.handle}
        try:
            _logger.info('===================close storage {{}}start.=============='.format(self.handle))
            client.box_service.close_storage(close_storage_params_dict)
        except Exception as e:
            _logger.info('close storage failed.')
            raise e
        _logger.info('=========================close storage {{}} end.======================'.format(self.handle))
        _logger.info('normal_guid_pool:{}'.format(td.normal_guid_pool))
        _logger.info('security_guid_pool:{}'.format(td.security_guid_pool))
        time.sleep(random.randint(1, 3))

    def execute(self):
        self.open_storage()
        self.check_and_accept_the_error()
        self.close_storage()


if __name__ == '__main__':
    _logger.info('start opening...')
    OpenStorage().execute()
