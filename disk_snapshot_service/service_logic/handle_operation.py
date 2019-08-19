from cpkt.core import xlogging as lg

import interface_data_define as idd
from business_logic import handle_pool as pool
from business_logic import locker_manager as lm
from business_logic import storage
from business_logic import storage_action as action
from business_logic import storage_chain as chain
from business_logic import storage_reference_manager as srm
from business_logic import storage_tree as tree
from data_access import models as m
from data_access import session as s

_logger = lg.get_logger(__name__)


class CloseStorage(object):
    """关闭磁盘快照"""

    def __init__(self, handle: pool.Handle):
        self.handle = handle

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return f'close storage : <{self.handle}>'

    @property
    def trace_msg(self):
        return self.__str__()

    def execute(self):
        """
        写句柄
            1. 如果底层模块发生错误，那么标记为 Abnormal 状态，后台回收线程异步进行状态修正
            2. 如果当前不为 Writing 状态，那么状态转移有误，同样视为异常状态
            3. 成功完成后，将标记为 Hashing 状态（CDP文件类型同样时 Hashing 状态）
        读句柄
            无特殊处理
        """
        if self.handle.writing:
            try:
                self.handle.destroy()
                self._set_storage_status(m.SnapshotStorage.STATUS_HASHING)
            except Exception as e:
                self._set_storage_status(m.SnapshotStorage.STATUS_ABNORMAL)
                raise e
        else:
            self.handle.destroy()

    def _set_storage_status(self, status):
        with lm.get_storage_locker(self.trace_msg), s.transaction():
            storage.query_by_ident(self.handle.storage_chain.last_storage_item.ident).update_status(status)


class OpenStorage(object):
    def __init__(self, handle: str, caller_trace: str, caller_pid: int, caller_pid_created: int,
                 storage_ident: str, timestamp, open_raw_handle):
        """
        :param handle: 操作句柄
        :param caller_trace: 调试跟踪信息
        :param caller_pid: 调用进程的pid
        :param caller_pid_created: 调用进程的创建时间戳
        :param storage_ident: 需要打开的快照
        :param timestamp: 打开快照的时间戳（CDP文件类型有效）
        :param open_raw_handle: 是否同步打开读写的原始句柄

        :param self.trace_msg: 调试跟踪信息，锁管理器使用
        """
        self._handle = handle

        self._caller_trace = caller_trace
        self._caller_pid = caller_pid
        self._caller_pid_created = caller_pid_created

        self.storage_ident = storage_ident
        self.timestamp = timestamp

        self.open_raw_handle = open_raw_handle

        self.raw_flag = action.DiskSnapshotAction.generate_flag(self._caller_trace)

        self._trace_msg = None

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return f'open storage : <{self.storage_ident},{self.timestamp}> in [{self._handle}]'

    @property
    def caller_name(self):
        """供引用管理器使用的调试跟踪信息"""
        return self.__str__()

    @property
    def trace_msg(self) -> str:
        if not self._trace_msg:
            self._trace_msg = (f'open {self.storage_ident}, {self.timestamp}, ' f'handle:{self._handle},'
                               f'trace:{self._caller_trace},'
                               f'pid:{self._caller_pid},' f'pid_ts:{self._caller_pid_created}')
        return self._trace_msg

    def execute(self):
        """
        1. 获取快照存储链
        2. 调用底层接口打开快照存储链
        """
        handle: pool.Handle = pool.generate_handle(self._handle, False, self.raw_flag)
        try:
            with lm.get_journal_locker(self.trace_msg), lm.get_storage_locker(self.trace_msg), s.readonly():
                depend_nodes = self._query_depend_nodes()
                handle.storage_chain = self._generate_chain(depend_nodes).acquire()

            if self.open_raw_handle:
                with handle.locker:
                    handle.raw_handle, handle.ice_endpoint = action.DiskSnapshotAction.open_disk_snapshot(
                        handle.storage_chain, handle.raw_flag)

            return handle
        except Exception:
            handle.destroy()
            raise

    def _generate_chain(self, depend_nodes):
        r_chain = chain.StorageChainForRead(srm.get_srm(), self.caller_name, self.timestamp)
        for node in depend_nodes:
            r_chain.insert_tail(node.storage)
        return r_chain

    def _query_depend_nodes(self):
        tree_ident = storage.query_by_ident(self.storage_ident).tree_ident
        storage_tree = tree.generate(tree_ident)
        return storage_tree.fetch_nodes_to_root(self.storage_ident)


def close_snapshot(params: idd.CloseSnapshotParams):
    handle = pool.get_handle(params.handle, True)
    CloseStorage(handle).execute()


def open_snapshot(params: idd.OpenSnapshotParams) -> pool.Handle:
    return OpenStorage(params.handle, params.caller_trace, params.caller_pid, params.caller_pid_created,
                       params.storage_ident, params.timestamp, params.open_raw_handle).execute()


def get_raw_handle(params: idd.GetRawHandleParams) -> pool.Handle:
    handle = pool.get_handle(params.handle, True)
    with handle.locker:
        if (not handle.writing) and (not handle.raw_handle):
            handle.raw_handle, handle.ice_endpoint = action.DiskSnapshotAction.open_disk_snapshot(
                handle.storage_chain, handle.raw_flag)
    return handle


def set_hash_mode(params: idd.SetHashModeParams):
    handle = pool.get_handle(params.handle, True)
    # TODO set close mode
    pass
