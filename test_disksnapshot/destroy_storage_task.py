import client
import time
import logging
import random
import uuid
from unittest.mock import patch
# import pytest

from data_access import models as m
from data_access import session as s
import create_storage_task as cst
import xlogging

_logger = xlogging.getLogger('destroy_task')
logging.basicConfig(filename='destroy_task.log', format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)


class DestroyStorage(object):
    def __init__(self):
        self.journal_token = cst.generate_token()
        self.idents = [self.journal_token, ]

    def create_destroy_journal(self):
        destroy_journal_params = {
            'journal_token': self.journal_token,
            'idents': self.idents,
        }
        _logger.info('===================generate  journal {{}} for destroy  start============='.format(self.idents[0]))
        client.box_service.create_journal_for_destroy(destroy_journal_params)  # 调用新增日志的接口
        _logger.info('===================generate  journal {{}} for destroy  end =============='.format(self.idents[0]))
        time.sleep(random.randint(1,3))

    def destroy_storage(self):
        pass

    def check_and_accept_the_error(self):
        pass

    def execute(self):
        self.create_destroy_journal()
        self.destroy_storage()
        self.check_and_accept_the_error()


if __name__ == '__main__':
    # DestroyStorage().create_destroy_journal()
    client.box_service.create_destroy_journal_demo()