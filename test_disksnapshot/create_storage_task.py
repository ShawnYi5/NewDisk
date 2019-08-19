import decimal
import json
import logging
import os
import random
import string
import time

import client
from data_access import models as m
from data_access import session as s
import test_disksnapshot_complex_task as td
import xlogging

_logger = xlogging.getLogger('create_task')
logging.basicConfig(filename='create_task.log', format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)


token_list = list()
ident_list = list()
handle_list = list()


def _convert_timestamp_to_datetime_display(timestamp):
    decimal_timestamp = round(timestamp - int(timestamp), 6)
    date_time = str(time.strftime("%Y-%m-%d %H:%M:%S.", time.localtime(timestamp))) + str(decimal_timestamp)[2:5]
    return date_time


def generate_token():
    _time = time.time()
    return _convert_timestamp_to_datetime_display(_time)


def generate_path():
    a = ''.join(random.sample(string.ascii_letters, 2)[0])
    b = ''.join(random.sample(string.ascii_letters, 2)[0])
    return os.path.join('/', a, b)


class GetStorage(object):
    def __init__(self, ident):
        self.ident = ident
        self.storage_obj = self.get_storage_obj_by_ident()

    def is_in_storage(self):
        if self.storage_obj:
            return True
        else:
            return False

    def get_storage_obj_by_ident(self):
        return s.get_scoped_session().query(m.SnapshotStorage).filter(m.SnapshotStorage.ident == self.ident).first()

    def is_qcow(self):
        if self.storage_obj.type == 'q':
            return True
        else:
            return False

    def is_cdp(self):
        if self.storage_obj.type == 'c':
            return True
        else:
            return False

    def end_time(self):
        return self.storage_obj.finish_timestamp

    def start_time(self):
        return self.storage_obj.start_timestamp


class GetCreatedIdents(object):
    def __init__(self, ident):
        self.ident = ident

    def add_normal_ident(self):
        session = s.get_scoped_session()
        ident_obj = m.CreatedIdents(created_ident=self.ident)
        session.add(ident_obj)
        session.commit()

    def transform_unsafe_to_safe(self):
        session = s.get_scoped_session()
        ident_obj = session.query(m.CreatedIdents).filter(m.CreatedIdents.created_ident == self.ident).first()
        ident_obj.safe_status = m.CreatedIdents.SAFE
        session.commit()

    def transform_unopen_to_open(self):
        session = s.get_scoped_session()
        ident_obj = session.query(m.CreatedIdents).filter(m.CreatedIdents.created_ident == self.ident).first()
        ident_obj.open_status = m.CreatedIdents.OPEN
        session.commit()

    def transform_open_to_unopen(self):
        session = s.get_scoped_session()
        ident_obj = session.query(m.CreatedIdents).filter(m.CreatedIdents.created_ident == self.ident).first()
        ident_obj.open_status = m.CreatedIdents.NOT_OPEN
        session.commit()

    def transform_undestroy_to_destroy(self):
        session = s.get_scoped_session()
        ident_obj = session.query(m.CreatedIdents).filter(m.CreatedIdents.created_ident == self.ident).first()
        ident_obj.open_status = m.CreatedIdents.DESTROY
        session.commit()

    def get_ident_status_by_ident(self):
        return s.get_scoped_session().query(m.CreatedIdents).filter(m.CreatedIdents.created_ident == self.ident).first()

    def is_safe(self):
        if self.get_ident_status_by_ident().safe_status == 's':
            return True
        return False

    def is_opened(self):
        if self.get_ident_status_by_ident().open_status == 'op':
            return True
        return False

    def is_destroy(self):
        if self.get_ident_status_by_ident().destroy_status == 'd':
            return True
        return False


class CreateQcowJournal(object):
    """
    生成创建Qcow日志所需参数
    """

    def __init__(self):
        self.journal_token = generate_token()
        self.new_ident = self.journal_token
        self.parent_ident = random.sample(td.security_guid_pool, 1)[0]
        self.new_type = 'qcow'
        self.new_hash_type = '1'
        self.parent_timestamp = None
        self.new_storage_folder = None
        self.new_disk_bytes = None

    def _generate_other_params(self):
        parent_ident = random.sample(td.security_guid_pool, 1)[0]  # 获取随机的parent
        _logger.info('create new qcow journal by parent ident {}'.format(parent_ident))
        parent_obj = GetStorage(parent_ident)
        assert parent_obj, 'parent is not in storage'
        if parent_obj.is_qcow():
            self.parent_timestamp = parent_obj.start_time()
        else:
            self.parent_timestamp = decimal.Decimal(
                parent_obj.start_time() + random.randint(0, int(parent_obj.end_time() - parent_obj.start_time())))
        # self.parent_timestamp = str(parent_timestamp)
        self.new_storage_folder = generate_path()
        self.new_disk_bytes = random.randint(1, 1000)

    def execute(self):
        self._generate_other_params()
        qcow_journal_params = {
            'journal_token': self.journal_token,
            'new_ident': self.new_ident,
            'parent_ident': self.parent_ident,
            # 'parent_timestamp': self.parent_timestamp,
            'new_type': self.new_type,
            'new_storage_folder': self.new_storage_folder,
            'new_disk_bytes': self.new_disk_bytes,
            'new_hash_type': self.new_hash_type}
        _logger.info('======================generate qcow journal {{}} start：==============='.format(self.new_ident))
        client.box_service.create_journal_for_create(qcow_journal_params)  # 调用新增日志的接口
        _logger.info('========================generate qcow journal {{}} end ================='.format(self.new_ident))
        return self.new_ident  # new_ident  与 journal_token 值一致


class CreateCdpJournal(object):
    """
    生成创建Qcow日志所需参数
    """

    def __init__(self, parent_ident):
        self.journal_token = generate_token()
        self.new_ident = self.journal_token
        self.parent_ident = parent_ident
        self.new_type = 'cdp'
        self.new_hash_type = '1'
        self.parent_timestamp = None
        self.new_storage_folder = None
        self.new_disk_bytes = None

    def _generate_other_params(self):
        _logger.info('create new cdp journal by parent ident :{}'.format(self.parent_ident))
        parent_obj = GetStorage(self.parent_ident)
        if parent_obj.is_in_storage():
            if parent_obj.is_qcow():
                self.parent_timestamp = parent_obj.start_time()
            else:
                self.parent_timestamp = (
                        parent_obj.start_time() + random.randint(0,
                                                                 int(parent_obj.end_time() - parent_obj.start_time())))
        else:
            self.parent_timestamp = None
        self.new_storage_folder = generate_path()
        self.new_disk_bytes = random.randint(1, 1000)

    def execute(self):
        self._generate_other_params()
        cdp_journal_params = {
            'journal_token': self.journal_token,
            'new_ident': self.new_ident,
            'parent_ident': self.parent_ident,
            # 'parent_timestamp': self.parent_timestamp,
            'new_type': self.new_type,
            'new_storage_folder': self.new_storage_folder,
            'new_disk_bytes': self.new_disk_bytes,
            'new_hash_type': self.new_hash_type
        }
        _logger.info('==============create cdp journal {{}} begin!====================='.format(self.new_ident))
        client.box_service.create_journal_for_create(cdp_journal_params)  # 调用新增日志的接口
        _logger.info('==============create cdp journal {{}} end!====================={}'.format(self.new_ident))
        return self.new_ident  # new_ident  与 journal_token 值一致


class CreateStorage(object):

    def generate_new_create_journal(self):
        cdp_journals_num = random.randint(0, 3)  # create组合中cdp的个数
        # 创建qcow storage
        qcow_ident = CreateQcowJournal().execute()  # 完成 create journal 操作
        td.normal_guid_pool.append(qcow_ident)  # 创建journal日志后 将ident放入normal_guid_pool 中 提供给open事务 和destroy事务使用
        GetCreatedIdents(qcow_ident).add_normal_ident()
        ident_list.append(qcow_ident)  # ident_list 存放本次事务生成的ident,提供给之后的cdp作为父
        qcow_token = qcow_ident
        token_list.append(qcow_token)  # token_list 存放本次事务生成的token,提供给之后的create过程消费journal使用
        _logger.info('add qcow info end.')
        _logger.info('ident_list:{}.token_list:{}'.format(ident_list, token_list))

        # 创建cdp storage
        _logger.info('{} cdp journals will be create'.format(cdp_journals_num))
        for i in range(cdp_journals_num):
            cdp_ident = CreateCdpJournal(parent_ident=ident_list[-1]).execute()
            ident_list.append(cdp_ident)  # ident_list 存放本次事务生成的ident,提供给后面的cdp作为父
            cdp_token = cdp_ident
            token_list.append(cdp_token)

        time.sleep(random.randint(1, 3))

    def create_storage(self):
        _logger.info('ident_list:{}.token_list:{}'.format(ident_list, token_list))
        for token in token_list:
            handle = token  # handle 与 token  ident 值一致
            handle_list.append(handle)  # handle_list 存放本次事务生成的handle,提供给后面close storage传参作为父
            storage_trace_msg = 'create storage of {}'.format(token)
            caller_pid = os.getpid()
            caller_pid_created = int(time.time())
            create_storage_params_dict = {'handle': handle, 'journal_token': token, 'caller_trace': storage_trace_msg,
                                          'caller_pid': caller_pid, 'caller_pid_created': caller_pid_created}
            try:
                _logger.info('====================create storage {{}}start!========================='.format(handle))
                client.box_service.create_storage(create_storage_params_dict)
            except Exception as e:
                _logger.info('create storage faild!!!!！')
                raise e
            _logger.info('======================create storage {{}} end!=========================='.format(handle))

            time.sleep(random.randint(1, 3))

        _logger.info('create storage OK.')
        _logger.info('normal_guid_pool:{}'.format(td.normal_guid_pool))
        _logger.info('security_guid_pool:{}'.format(td.security_guid_pool))

    def check_and_accept_the_error(self):
        pass

    def close_storage(self):
        i = 0
        for handle in handle_list:
            close_storage_params_dict = dict()
            close_storage_params_dict['handle'] = handle
            try:
                _logger.info('===================close storage {{}}start.=============='.format(handle))
                client.box_service.close_storage(close_storage_params_dict)
                td.security_guid_pool.append(ident_list[i])
                GetCreatedIdents(ident_list[i]).transform_unsafe_to_safe()
                td.handle_pool.append(handle)
                _logger.info('=========================close storage {{}} end.======================'.format(handle))
            except Exception as e:
                _logger.info('close storage failed.')
                raise e
        _logger.info('normal_guid_pool:{}'.format(td.normal_guid_pool))
        _logger.info('security_guid_pool:{}'.format(td.security_guid_pool))
        i = i + 1
        _ = i

        time.sleep(random.randint(1, 3))

    def execute(self):
        self.generate_new_create_journal()
        self.create_storage()
        self.check_and_accept_the_error()
        self.close_storage()


if __name__ == '__main__':
    CreateStorage().execute()
