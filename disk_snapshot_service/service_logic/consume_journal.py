import abc
import contextlib
import typing
import uuid

from cpkt.core import exc
from cpkt.core import xlogging as lg

import interface_data_define as idd
from basic_library import xfunctions as xf
from business_logic import handle_pool as pool
from business_logic import journal
from business_logic import locker_manager as lm
from business_logic import storage
from business_logic import storage_action as action
from business_logic import storage_chain as chain
from business_logic import storage_reference_manager as srm
from business_logic import storage_tree as tree
from data_access import models as m
from data_access import session as s

_logger = lg.get_logger(__name__)


class CreateStorage(abc.ABC):
    """创建快照存储基类"""

    def __init__(self, handle: str, caller_trace: str, caller_pid: int, caller_pid_created: int,
                 params: journal.CreateInJournal):
        """
        :param handle: 操作句柄
        :param caller_trace: 调试跟踪信息mm
        :param caller_pid: 调用进程的pid
        :param caller_pid_created: 调用进程的创建时间戳
        :param params: 创建参数

        :param self.trace_msg: 调试跟踪信息，锁管理器使用
        """
        self._handle = handle
        self._params = params

        self._caller_trace = caller_trace
        self._caller_pid = caller_pid
        self._caller_pid_created = caller_pid_created

        self.raw_flag = action.DiskSnapshotAction.generate_flag(self._caller_trace)

        self._trace_msg = None

        assert self._params.consumed, ('磁盘快照日志未被标记为已消费', f'journal NOT consumed {self._params.token}', 0)

    def __repr__(self):
        return self.__str__()

    @property
    def disk_bytes(self) -> int:
        return self._params.new_disk_bytes

    @property
    def new_ident(self) -> str:
        return self._params.new_ident

    @property
    def parent_timestamp(self):
        return self._params.parent_timestamp

    @property
    def is_cdp(self) -> bool:
        return self._params.is_cdp

    @property
    def is_qcow(self) -> bool:
        return self._params.is_qcow

    @property
    def is_root_node(self) -> bool:
        return self._params.parent_ident is None

    @property
    def parent_ident(self) -> str:
        return self._params.parent_ident

    @property
    def storage_folder(self) -> str:
        return self._params.new_storage_folder

    @property
    def caller_name(self) -> str:
        """供引用管理器使用的调试跟踪信息"""
        return self.__str__()

    @property
    def trace_msg(self) -> str:
        if not self._trace_msg:
            self._trace_msg = (f'create {self._params.new_type} storage:{self.new_ident},' f'handle:{self._handle},'
                               f'trace:{self._caller_trace},'
                               f'pid:{self._caller_pid},' f'pid_ts:{self._caller_pid_created}')
        return self._trace_msg

    @abc.abstractmethod
    def execute(self):
        """执行作业"""
        raise NotImplementedError()

    @abc.abstractmethod
    def _generate_image_path(self, parent_storage_obj) -> str:
        raise NotImplementedError()

    @staticmethod
    def _generate_tree_ident():
        return uuid.uuid4().hex

    def _create_new_storage(self, parent_storage: storage.Storage, tree_ident=None) -> storage.Storage:
        """创建新快照点，返回新创建的快照对象"""
        if not tree_ident:
            if parent_storage:
                tree_ident = parent_storage.tree_ident
            else:
                tree_ident = self._generate_tree_ident()

        if parent_storage:
            parent_storage_ident = parent_storage.ident
            parent_storage_obj = parent_storage.storage_obj
        else:
            parent_storage_ident = None
            parent_storage_obj = None

        return storage.create_new_storage(
            storage_ident=self.new_ident,
            parent_ident=parent_storage_ident,
            parent_timestamp=self.parent_timestamp,
            is_cdp=self.is_cdp,
            disk_bytes=self.disk_bytes,
            image_path=self._generate_image_path(parent_storage_obj),
            tree_ident=tree_ident,
        )

    @staticmethod
    def _set_storage_abnormal_when_except(new_snapshot: storage.Storage):
        if not new_snapshot:
            return

        try:
            with s.transaction():
                new_snapshot.update_status(m.SnapshotStorage.STATUS_ABNORMAL)
        except Exception as e:
            _logger.error(lg.format_exception(e))


@contextlib.contextmanager
def deal_handle_when_excption(handle: pool.Handle):
    """处理创建过程中发生异常的情况"""
    try:
        yield handle
    except Exception as e:
        handle.destroy()
        raise e


class CreateCdpStorage(CreateStorage):
    """创建cdp快照存储"""

    def __init__(self, handle: str, caller_trace: str, caller_pid: int, caller_pid_created: int,
                 params: journal.CreateInJournal):
        super(CreateCdpStorage, self).__init__(handle, caller_trace, caller_pid, caller_pid_created, params)
        assert self.is_cdp, ('磁盘快照日志类型无效', f'journal NOT cdp {self._params.token}', 0)
        assert self.parent_ident, ('CDP快照存储无父节点', f'journal cdp NOT parent {self._params.token}', 0)
        assert not self.parent_timestamp, (
            'CDP快照存储日志无效',
            f'journal cdp parent_timestamp {self._params.token} {self.parent_ident} {self.parent_timestamp}', 0)

    def __str__(self):
        return f'creating new cdp storage : <{self.new_ident}>  in [{self._handle}]'

    def _generate_image_path(self, parent_storage_obj):
        _ = parent_storage_obj
        return action.ImagePathGenerator.generate_cdp(self.storage_folder, self._params.new_ident)

    def execute(self) -> pool.Handle:
        """
        1. 更新数据库记录，新建立的存储对象指向父节点
            * 父快照为qcow类型文件时，支持父快照还在日志表中 CreateInJournal；
              也就是，创建文件的时序与日志的时序不一致
              需要在日志表中记录当前节点，支持创建父快照时，调整当前节点的父
            * 其余情况，其父快照必须已经在存储对象表中
        2. 调用底层模块创建存储文件
            * 如果创建突然中断，那么数据库记录会保持为 Creating 状态，下次启动时进行状态修正
            * 如果创建失败，那么标记为 Abnormal 状态，后台回收线程异步进行状态修正
        3. 更新数据库记录，将建立的存储对象标记为 Writing 状态

        :remark:
        1. 仅仅支持单次的创建时序与依赖关系顺序不一致
            也就是不支持依赖链上有超过一个 CreateInJournal 类型的节点，且这些节点在未来某个时刻会真实创建；
            上层逻辑应杜绝出现此类状况
            参考 CreateQcowStorage
        """
        handle: pool.Handle = pool.generate_handle(self._handle, True, self.raw_flag)
        new_snapshot: storage.Storage = None
        try:
            with deal_handle_when_excption(handle):
                with lm.get_journal_locker(self.trace_msg), lm.get_storage_locker(self.trace_msg), s.transaction():
                    parent_storage = self._query_parent_storage()
                    new_snapshot = self._create_new_storage(parent_storage)
                    handle.storage_chain = (chain.StorageChainForWrite(srm.get_srm(), self.caller_name)
                                            .insert_tail(new_snapshot.storage_obj).acquire())

                with handle.locker:
                    handle.raw_handle, handle.ice_endpoint = (
                        action.DiskSnapshotAction.create_cdp_snapshot(
                            handle.storage_chain.last_storage_item, handle.raw_flag)
                    )

                with lm.get_storage_locker(self.trace_msg), s.transaction():
                    new_snapshot.update_status(m.SnapshotStorage.STATUS_WRITING)

                return handle
        except Exception as e:
            self._set_storage_abnormal_when_except(new_snapshot)
            raise e

    def _query_parent_storage(self) -> storage.Storage:
        parent_storage = storage.query_by_ident(self.parent_ident)
        if not parent_storage:
            unconsumed = journal.query_unconsumed_create(self._params.journal_obj)
            for jn in unconsumed:
                if jn.new_ident == self.parent_ident and jn.is_qcow:
                    """发现父快照还在日志表中，且父快照为qcow文件类型"""
                    jn.append_child_storage_ident(self.new_ident)
                    if jn.is_root:
                        _logger.info(f'parent in journal and is root. <{jn.token}>')
                    else:
                        _logger.info(f'parent in journal and not root. _find_parent_in_storage. <{jn.token}>')
                        parent_storage = self._find_parent_in_storage(unconsumed)
                    break
            else:
                raise exc.generate_exception_and_logger(
                    'CDP快照存储父节点无效',
                    f'journal {self._params.token} parent_ident invalid {self.parent_ident}, not in storage',
                    0)
        _logger.info(f'{self} _query_parent_storage : ({parent_storage})')
        return parent_storage

    def _find_parent_in_storage(self, unconsumed: typing.List[journal.CreateInJournal]) -> storage.Storage:
        first_in_journals = self._params
        _ = xf.DataHolder()
        while _.set(first_in_journals.query_parent_in_journals(unconsumed)):
            first_in_journals = _.get()

        st = storage.query_by_ident(first_in_journals.parent_ident)
        if st:
            return st
        else:
            raise exc.generate_exception_and_logger(
                'CDP快照存储父节点无效',
                f'journal {self._params.token} parent_ident invalid {self.parent_ident}, not chain',
                0)


class CreateQcowStorage(CreateStorage):
    """创建qcow快照存储"""

    def __init__(self, handle: str, caller_trace: str, caller_pid: int, caller_pid_created: int,
                 params: journal.CreateInJournal):
        super(CreateQcowStorage, self).__init__(handle, caller_trace, caller_pid, caller_pid_created, params)
        assert self.is_qcow, ('磁盘快照日志类型无效', f'journal NOT qcow {self._params.token}', 0)

    def __str__(self):
        return f'creating new qcow storage : <{self.new_ident}> in [{self._handle}]'

    def _generate_image_path(self, parent_storage_obj):
        return action.ImagePathGenerator.generate_qcow(parent_storage_obj, self.storage_folder, self.disk_bytes)

    def execute(self):
        """执行作业

        1. 更新数据库记录，新建立的存储对象指向父节点
            * 新建状态为 Creating 状态
            * 日志对象有子节点记录，就意味着子节点先于本节点创建
              如本节点不是根节点，那么该步骤不修改子节点的指向，在第3步骤再修改
              如本节点为根节点，那么在该步骤修改子节点依赖；因为如果本节点创建失败，逻辑上后续子节点应该无效
            * 父快照必须在 storage 表中
        2. 调用底层模块创建存储文件
            * 如果创建突然中断，那么数据库记录会保持为 Creating 状态，下次启动时进行状态修正
            * 如果创建失败，那么标记为 Abnormal 状态，后台回收线程异步进行状态修正
        3. 更新数据库记录，将建立的存储对象标记为 Writing 状态
            * 如果本节点不是根节点，且具有子节点，需要移动子节点的指向

        :remark:
        1. 调用底层模块创建存储文件并未纳入锁空间
            可知：上层逻辑如果确切需要使用某个存储文件中的数据（同时写入与读取），那么应该自行保证调用时序
        """

        handle: pool.Handle = pool.generate_handle(self._handle, True, self.raw_flag)
        new_snapshot: storage.Storage = None
        try:
            with deal_handle_when_excption(handle):
                with lm.get_journal_locker(self.trace_msg), lm.get_storage_locker(self.trace_msg), s.transaction():
                    parent_storage, rw_chain, tree_ident = self._query_parent_storage_and_chain()
                    new_snapshot = self._create_new_storage(parent_storage, tree_ident)
                    if self.is_root_node:
                        self._deal_children_in_journal(new_snapshot)
                    handle.storage_chain = rw_chain.insert_tail(new_snapshot.storage_obj).acquire()
                    tree.check(new_snapshot.tree_ident)

                with handle.locker:
                    handle.raw_handle, handle.ice_endpoint = (
                        action.DiskSnapshotAction.create_qcow_snapshot(
                            handle.storage_chain, handle.raw_flag)
                    )

                with lm.get_storage_locker(self.trace_msg), s.transaction():
                    if not self.is_root_node:
                        self._deal_children_in_journal(new_snapshot)
                    new_snapshot.update_status(m.SnapshotStorage.STATUS_WRITING)
                    tree.check(new_snapshot.tree_ident)

                return handle
        except Exception as e:
            self._set_storage_abnormal_when_except(new_snapshot)
            raise e

    def _query_parent_storage_and_chain(self) -> (storage.Storage, chain.StorageChainForRW, str):
        if self.is_root_node:
            parent_storage = None
            tree_ident = self._query_tree_ident_from_children()
            depend_nodes = list()
        else:
            parent_storage = self._query_parent_storage()
            tree_ident = parent_storage.tree_ident
            storage_tree = tree.generate(tree_ident)
            depend_nodes = storage_tree.fetch_nodes_to_root(self.parent_ident)

        rw_chain = chain.StorageChainForRW(srm.get_srm(), self.caller_name, self.parent_timestamp)
        for node in depend_nodes:
            if node.storage.status == m.SnapshotStorage.STATUS_CREATING:
                raise exc.generate_exception_and_logger(
                    r'快照存储链无效，存在创建中的快照存储', f'invalid storage chain, {node.storage}', 0)
            if node.storage.status == m.SnapshotStorage.STATUS_ABNORMAL:
                raise exc.generate_exception_and_logger(
                    r'快照存储链无效，存在异常的快照存储', f'invalid storage chain, {node.storage}', 0)
            rw_chain.insert_tail(node.storage)

        return parent_storage, rw_chain, tree_ident

    def _query_parent_storage(self) -> storage.Storage:
        parent_storage = storage.query_by_ident(self.parent_ident)
        if not parent_storage:
            raise exc.generate_exception_and_logger(
                'QCOW快照存储父节点无效',
                f'journal {self._params.token} parent_ident invalid {self.parent_ident}, not in storage',
                0)
        _logger.info(f'{self} _query_parent_storage : ({parent_storage})')
        return parent_storage

    def _query_tree_ident_from_children(self):
        for st in self._params.children_storages:
            return st.tree_ident
        return None

    def _deal_children_in_journal(self, new_snapshot):
        for st in self._params.children_storages:
            assert not st.parent_timestamp, (
                '子快照存储对象无效',
                f'child in journal parent_timestamp invalid : {st} {st.parent_timestamp}', 0)
            st.update_parent(new_snapshot)


class DestroyJournal(object):
    class DelayDealException(Exception):
        pass

    def __init__(self, params: journal.DestroyInJournal):
        """
        :param params: 创建参数

        :param self.trace_msg: 调试跟踪信息，锁管理器使用
        """
        self._params = params
        self.op_number = xf.generate_unique_number(xf.UNIQUE_NUMBER_DESTROY_JOURNAL)
        self.trace_msg = f'destroy storage {self.op_number} : <{",".join(self.idents)}>'

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return self.trace_msg

    @property
    def idents(self):
        return self._params.idents

    def execute(self):
        """
        首先判断是否在storage表中
            如果当前为 Storage 状态，那么标记为 Recycling 状态
            如果当前为 Abnormal， Recycling 或 Deleted 状态，那么就 warning
            其余状态等待下次扫描
        然后判断是否在journal表中
            如果为 CreateInJournal，那么标记为 已消费
            其余状态等待下次扫描
        都没有
            warning
        """
        with lm.get_journal_locker(self.trace_msg), lm.get_storage_locker(self.trace_msg), s.transaction():
            consume = True
            for ident in self.idents:
                try:
                    if self._deal_in_storage(ident):
                        continue
                    elif self._deal_in_journal(ident):
                        continue
                    else:
                        _logger.warning(f'<{ident}> in [{self}] NOT exist')
                except DestroyJournal.DelayDealException:
                    consume = False  # DelayDealException 异常表示本次不处理，下次再处理

            if consume:
                self._params.consume()

    def _deal_in_storage(self, ident) -> bool:
        st = storage.query_by_ident(ident)
        if not st:
            return False

        if st.status == m.SnapshotStorage.STATUS_STORAGE:
            st.update_status(m.SnapshotStorage.STATUS_RECYCLING)
            _logger.info(f'set [{st}] recycling. because destroy by {self._params.token}')
            return True
        elif st.status in (m.SnapshotStorage.STATUS_ABNORMAL,
                           m.SnapshotStorage.STATUS_DELETED,
                           m.SnapshotStorage.STATUS_RECYCLING,):
            _logger.warning(f'{ident} status is {st}, NOT update to RECYCLING')
            return True
        else:
            raise DestroyJournal.DelayDealException()

    def _deal_in_journal(self, ident) -> bool:
        unconsumed = journal.query_unconsumed_create()
        for jn in unconsumed:
            if jn.new_ident == ident:  # (shawn):error word?!
                jn.consume()
                _logger.info(f'journal {jn.token} will NOT create {jn.new_ident}.'
                             f'because destroy by {self._params.token}')
                return True
        else:
            return False


def create_snapshot(params: idd.CreateSnapshotParams) -> pool.Handle:
    """创建快照存储"""

    jn: journal.CreateInJournal = journal.consume(params.journal_token, params.caller_trace, journal.CreateInJournal)
    if jn.is_cdp:
        return CreateCdpStorage(
            params.handle, params.caller_trace, params.caller_pid, params.caller_pid_created, jn
        ).execute()
    else:
        return CreateQcowStorage(
            params.handle, params.caller_trace, params.caller_pid, params.caller_pid_created, jn
        ).execute()
