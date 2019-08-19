import json
import random
import sys
import uuid

from cpkt.core import rt
from cpkt.core import xlogging as lg
from cpkt.data import define as dd
from cpkt.icehelper import application
from cpkt.rpc import ice

_logger = lg.get_logger(__name__)

caller_pid, caller_pid_created = rt.PidReplier.get_current_pid_and_create_timestamp()


class Client(application.Application):

    def run(self, args):
        self.demo()
        self.communicator().waitForShutdown()
        return 0

    def get_prx(self):
        return ice.SnapshotApi.SnapshotPrx.checkedCast(self.communicator().propertyToProxy(r'StorageApi.Proxy'))

    def demo(self):
        create_qcow_journal_token = uuid.uuid4().hex
        create_cdp_journal_token = uuid.uuid4().hex
        new_qcow_ident = uuid.uuid4().hex
        new_cdp_ident = uuid.uuid4().hex
        create_qcow_handle = f'demo_handle_{random.randint(0, 1000000)}'
        create_cdp_handle = f'demo_handle_{random.randint(0, 1000000)}'
        open_handle = f'demo_handle_{random.randint(0, 1000000)}'

        # 生成创建qcow快照日志
        j = json.dumps({
            'journal_token': create_qcow_journal_token,
            'new_ident': new_qcow_ident,
            'new_type': dd.DiskSnapshotService.STORAGE_TYPE_QCOW,
            'new_storage_folder': r'/home/xxxxx',
            'new_disk_bytes': 1024 ** 3,
        })
        print(f'begin ... 创建qcow日志 {j}')
        try:
            r = self.get_prx().Op('generate_journal_for_create', j)
            print(f'end. {r}')
        except Exception as e:
            print(f'failed. {e}')

        # 生成创建cdp快照日志
        j = json.dumps({
            'journal_token': create_cdp_journal_token,
            'new_ident': new_cdp_ident,
            'new_type': dd.DiskSnapshotService.STORAGE_TYPE_CDP,
            'new_storage_folder': r'/home/xxxxx',
            'new_disk_bytes': 1024 ** 3,
            'parent_ident': 'ba771f70c47a43d6a7473b67f3b9e9e8'  # 数据库随机获取的一个qcow
        })
        print(f'begin ... 创建cdp日志 {j}')
        try:
            r = self.get_prx().Op('generate_journal_for_create', j)
            print(f'end. {r}')
        except Exception as e:
            print(f'failed. {e}')

        # 创建qcow快照句柄
        j = json.dumps({
            'handle': create_qcow_handle,
            'journal_token': create_qcow_journal_token,
            'caller_trace': 'demo trace',
            'caller_pid': caller_pid,
            'caller_pid_created': caller_pid_created,
        })
        print(f'begin ... 创建qcow快照 {j}')
        try:
            r = self.get_prx().Op('create_snapshot', j)
            print(f'end. {r}')
        except Exception as e:
            print(f'failed. {e}')

        #   创建cdp快照句柄
        j = json.dumps({
            'handle': create_cdp_handle,
            'journal_token': create_cdp_journal_token,
            'caller_trace': 'demo trace',
            'caller_pid': caller_pid,
            'caller_pid_created': caller_pid_created,
        })
        print(f'begin ... 创建cdp快照 {j}')
        try:
            r = self.get_prx().Op('create_snapshot', j)
            print(f'end. {r}')
        except Exception as e:
            print(f'failed. {e}')

        # 打开快照句柄
        j = json.dumps({
            'handle': open_handle,
            'caller_trace': 'demo trace',
            'caller_pid': caller_pid,
            'caller_pid_created': caller_pid_created,
            'storage_ident': '8a8b20be6da040d4b1b3610197ee235f',  # 从数据库随机获取一个ident
            'open_raw_handle': False
        })
        print(f'begin ... 打开快照 {j}')
        r = self.get_prx().Op('open_snapshot', j)
        print(f'end. {r}')

        # 关闭快照句柄
        j = json.dumps({
            'handle': create_qcow_handle
        })
        print(f'begin ... 关闭句柄 {j}')
        try:
            r = self.get_prx().Op('close_snapshot', j)
            print(f'end. {r}')
        except Exception as e:
            print(f'failed. {e}')

        # 生成销毁快照日志
        j = json.dumps({
            'journal_token': uuid.uuid4().hex,
            'idents': [new_cdp_ident, ],
        })
        print(f'begin ... 生成销毁日志 {j}')
        try:
            r = self.get_prx().Op('generate_journal_for_destroy', j)
            print(f'end. {r}')
        except Exception as e:
            print(f'failed. {e}')

        # 获取原生句柄
        j = json.dumps({
            'handle': open_handle
        })
        print(f'begin ... 获取源句柄 {j}')
        try:
            r = self.get_prx().Op('get_raw_handle', j)
            print(f'end. {r}')
        except Exception as e:
            print(f'failed. {e}')


app = Client()

app_default_properties = [
    (r'Ice.Default.Host', r'localhost'),
    (r'Ice.Warn.Connections', r'1'),
    (r'Ice.ACM.Heartbeat', r'3'),  # HeartbeatAlways
    (r'Ice.MessageSizeMax', r'131072'),  # 单位KB, 128MB
    (r'StorageApi.Proxy', r'dss : tcp -h 127.0.0.1 -p 21119'),
]
app.main(sys.argv, '/none.cfg', app_default_properties, _logger)
