import decimal
import json
import sys
import uuid
import time

from cpkt.core import xlogging as lg
from cpkt.rpc import ice

import Ice

_logger = lg.get_logger(__name__)


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)


class _BoxService(object):
    def __init__(self):

        _logger.info(r'test disksnapshot service starting ...')

        self.__boxPrx = None
        self.__ktsPrx = None
        self.__logicPrx = None
        self.__imgPrx = None
        self.__installPrx = None
        self.__cTcpPrx = None
        self.__PowerPrx = None
        self.__HTBCreatePrx = None

        config = self.__generate_ice_config()
        self.__init_ice(config=config)

    @staticmethod
    def __generate_ice_config():
        init_data = Ice.InitializationData()
        init_data.properties = Ice.createProperties()
        init_data.properties.setProperty(r'Ice.ThreadPool.Client.Size', r'8')
        init_data.properties.setProperty(r'Ice.ThreadPool.Client.SizeMax', r'64')
        init_data.properties.setProperty(r'Ice.ThreadPool.Client.ThreadIdleTime', r'0')
        init_data.properties.setProperty(r'Ice.ThreadPool.Client.StackSize', r'8388608')
        init_data.properties.setProperty(r'StorageApi.Proxy', r'dss : tcp -h 127.0.0.1 -p 21119')
        init_data.properties.setProperty(r'Ice.Default.Host', r'localhost')
        init_data.properties.setProperty(r'Ice.Warn.Connections', r'1')
        init_data.properties.setProperty(r'Ice.RetryIntervals', r'0')
        init_data.properties.setProperty(r'Ice.MessageSizeMax', r'65536')  # 64MB
        init_data.properties.setProperty(r'Ice.ACM.Heartbeat', r'3')  # BoxService KernelTcp 会检测心跳
        return init_data

    def __init_ice(self, config):
        self.communicator = Ice.initialize(sys.argv, config)

    def get_prx(self):
        return ice.SnapshotApi.SnapshotPrx.checkedCast(self.communicator.propertyToProxy(r'StorageApi.Proxy'))

    def close_storage(self, close_params_dict):
        j = json.dumps(close_params_dict)
        print(f'begin .close_snapshot.. {j}')
        r = self.get_prx().Op('close_snapshot', j)
        print(f'end. {r}')

    def create_storage(self, create_params_dict):
        j = json.dumps(create_params_dict)
        print(f'begin .create_snapshot.. {j}')
        r = self.get_prx().Op('create_snapshot', j)
        print(f'end. {r}')

    def open_storage(self, open_params_dict):
        j = json.dumps(open_params_dict)
        print(f'begin .open_snapshot.. {j}')
        r = self.get_prx().Op('open_snapshot', j)
        print(f'end. {r}')

    def create_journal_for_create(self, create_journal_params_dict):
        j = json.dumps(create_journal_params_dict, cls=DecimalEncoder)
        print(f'begin .generate_journal_for_create.. {j}')
        r = self.get_prx().Op('generate_journal_for_create', j)
        print(f'end. {r}')

    def create_journal_for_destroy(self, destroy_journal_params_dict):
        j = json.dumps(destroy_journal_params_dict, cls=DecimalEncoder)
        print(f'begin .generate_journal_for_destroy.. {j}')
        r = self.get_prx().Op('generate_journal_for_destroy', j)
        print(f'end. {r}')

    def create_destroy_journal_demo(self):
        # 生成销毁快照日志
        j = json.dumps({
            'journal_token': uuid.uuid4().hex,
            'idents': [uuid.uuid4().hex, ],
        })
        print(f'begin ... 生成销毁日志 {j}')
        try:
            r = self.get_prx().Op('generate_journal_for_destroy', j)
            print(f'end. {r}')
        except Exception as e:
            print(f'failed. {e}')

    def create_journal_for_create_demo(self):
        create_qcow_journal_token = uuid.uuid4().hex
        new_qcow_ident = uuid.uuid4().hex
        parent_timestamp = decimal.Decimal(time.time())
        j = json.dumps({
            'journal_token': create_qcow_journal_token,
            'new_ident': new_qcow_ident,
            'new_type': 'qcow',
            'parent_timestamp': str(parent_timestamp),
            # 'parent_timestamp': parent_timestamp,
            'new_storage_folder': r'/home/xxxxx',
            'new_disk_bytes': 1024 ** 3,
        })
        print(f'begin ... 创建qcow日志 {j}')
        try:
            r = self.get_prx().Op('generate_journal_for_create', j)
            print(f'end. {r}')
        except Exception as e:
            print(f'failed. {e}')


box_service = _BoxService()
