import decimal
import uuid

from basic_library import xdata
from basic_library import xfunctions
from basic_library import xlogging
from storage_manager import models as m
from storage_manager import storage_chain as chain
from storage_manager import storage_tree as tree

_logger = xlogging.getLogger(__name__)


class StorageChainQueryByHostSnapshot(object):
    """通过HostSnapshot查询StorageChain

    :remark:
        支持业务中“打开主机快照点”的需求
    """

    def __init__(self, storage_locker_manager, storage_reference_manager,
                 host_snapshot_ident: str, timestamp: decimal.Decimal = None):
        """
        :param storage_locker_manager: StorageLockerManager
            快照存储锁管理器
        :param storage_reference_manager: StorageReferenceManager
            快照存储引用管理器
        :param host_snapshot_ident:
            主机快照标识字符串
        :param timestamp:
            CDP型主机快照有效，指定时刻，当为None时，意为该HostSnapshot最新时刻
        """
        self.storage_locker_manager = storage_locker_manager
        self.storage_reference_manager = storage_reference_manager
        self.host_snapshot_ident = host_snapshot_ident
        self.timestamp = timestamp
        self.chain_list = None
        self._uuid_hex = uuid.uuid4().hex  # 对象唯一标识
        self.name = f'{self} {self._uuid_hex}'

    def __str__(self):
        return f'query chain by host snapshot : <{self.host_snapshot_ident}|{self.timestamp}>'

    def __repr__(self):
        return self.__str__()

    def get_storage_chain_list(self) -> list:
        """获取快照存储链（复数）

        :remark:
            获取过程中，会使用快照存储标识进入锁空间
            返回值会内部缓存，也就是获取的结果在第一次调用时就固定
            返回的chain已经进入entry状态，参考 StorageChainForRead
        :return:
            [
                {
                    'disk_index': 0,
                    'disk_snapshot_obj': disk_snapshot_obj,
                    'storage_chain': storage_chain,
                },
                ...
            ]
        :raises:
             xdata.HostSnapshotInvalid
                主机快照已被标识为不可用
             xdata.DiskSnapshotStorageInvalid
                磁盘快照不可用
        """
        if self.chain_list:
            return self.chain_list

        _chain = list()

        host_snapshot_obj, timestamp = self._get_host_snapshot_obj()
        for disk_snapshot_obj in host_snapshot_obj.disk_snapshots.all():
            storage_root_obj = self._get_storage_objs(disk_snapshot_obj).first().storage_root

            with self.storage_locker_manager.get_locker(storage_root_obj.root_ident, self.name):
                self._get_host_snapshot_obj()  # 进入锁空间后再次检查
                disk_snapshot_obj.refresh_from_db()  # 进入锁空间后更新数据库对象

                _chain.append({
                    'disk_index': disk_snapshot_obj.disk_index,
                    'disk_snapshot_obj': disk_snapshot_obj,
                    'storage_chain': self._generate_storage_chain(storage_root_obj, disk_snapshot_obj, timestamp),
                })

        self.chain_list = _chain
        return self.chain_list

    def _get_storage_objs(self, disk_snapshot_obj):
        assert disk_snapshot_obj.locator_id
        storage_objs = m.DiskSnapshotStorage.objects.filter(locator_id=disk_snapshot_obj.locator_id).exclude(
            storage_status__in=m.DiskSnapshotStorage.STATUS_NOT_READABLE).order_by('storage_begin_timestamp')

        if storage_objs.count():
            return storage_objs
        else:
            xlogging.raise_and_logging_error(
                '指定的备份已被标识为不可用', f'{disk_snapshot_obj} can not find readable storage in {self}',
                print_args=False, exception_class=xdata.DiskSnapshotStorageInvalid)

    def _get_host_snapshot_obj(self):
        host_snapshot_obj = m.HostSnapshot.get_obj_by_ident(self.host_snapshot_ident)

        if host_snapshot_obj.is_cdp_host_snapshot:
            for disk_snapshot_obj in host_snapshot_obj.disk_snapshots.all():
                assert (host_snapshot_obj.host_snapshot_begin_timestamp >=
                        self._get_storage_objs(disk_snapshot_obj).first().storage_begin_timestamp)

            if not self.timestamp:
                self.timestamp = host_snapshot_obj.host_snapshot_end_timestamp
            return self._check_cdp_host_snapshot_obj(host_snapshot_obj), self.timestamp
        else:
            assert (not self.timestamp)
            return (
                self._check_normal_host_snapshot_obj(host_snapshot_obj),
                host_snapshot_obj.host_snapshot_begin_timestamp,
            )

    @staticmethod
    def _check_normal_host_snapshot_obj(host_snapshot_obj):
        if not host_snapshot_obj.host_snapshot_valid:
            xlogging.raise_and_logging_error(
                '指定的备份已被标识为不可用', f'{host_snapshot_obj} invalid', print_args=False,
                exception_class=xdata.HostSnapshotInvalid)
        else:
            return host_snapshot_obj

    def _check_cdp_host_snapshot_obj(self, host_snapshot_obj):
        debug_msg = None
        if not host_snapshot_obj.host_snapshot_valid:
            debug_msg = f'{host_snapshot_obj} invalid'
        elif self.timestamp < host_snapshot_obj.host_snapshot_begin_timestamp:
            debug_msg = (f'{host_snapshot_obj} begin '
                         f'{self.timestamp} < {host_snapshot_obj.host_snapshot_begin_timestamp}')
        elif self.timestamp > host_snapshot_obj.host_snapshot_end_timestamp:
            debug_msg = (f'{host_snapshot_obj} end '
                         f'{self.timestamp} > {host_snapshot_obj.host_snapshot_end_timestamp}')
        else:
            pass  # do nothing

        if debug_msg:
            xlogging.raise_and_logging_error(
                f'指定时刻“{xfunctions.humanize_timestamp(self.timestamp)}”的备份已被标识为不可用',
                debug_msg, print_args=False, exception_class=xdata.HostSnapshotInvalid)
        else:
            return host_snapshot_obj

    def _generate_storage_chain(self, storage_root_obj, disk_snapshot_obj, timestamp):
        storage_obj = self._find_storage_obj(disk_snapshot_obj, timestamp)

        storage_tree = tree.DiskSnapshotStorageTree.create_instance_by_storage_root(storage_root_obj)
        assert not storage_tree.is_empty()

        storage_chain = StorageChainQueryByDiskSnapshotStorage(
            chain.StorageChainForRead, storage_tree, self.storage_reference_manager,
            storage_obj, timestamp, self.name).get_storage_chain()

        return storage_chain.acquire()

    def _find_storage_obj(self, disk_snapshot_obj, timestamp):
        prev_storage_obj = None
        for storage_obj in self._get_storage_objs(disk_snapshot_obj).all():
            if timestamp < storage_obj.storage_begin_timestamp:
                return prev_storage_obj if prev_storage_obj else storage_obj
            elif timestamp <= storage_obj.storage_end_timestamp:
                return storage_obj
            else:
                prev_storage_obj = storage_obj
        else:
            assert prev_storage_obj is not None
            return prev_storage_obj


class StorageChainQueryByDiskSnapshotStorage(object):
    """通过DiskSnapshotStorage查询StorageChain

    :remark:
        支持业务中“打开快照存储”的需求
        内部不会进入锁空间，需在锁空间内执行该逻辑
    """

    def __init__(
            self, storage_chain_class, storage_tree, storage_reference_manager,
            storage_obj, timestamp: decimal.Decimal = None, caller_name: str = ''):
        """
        :param storage_tree: DiskSnapshotStorageTree
            快照存储树
        :param storage_reference_manager: StorageReferenceManager
            快照存储引用管理器
        :param storage_obj:
            快照存储数据库对象
        :param timestamp:
            CDP型快照存储有效，指定时刻，当为None时，意为该storage的全部区域
        :param caller_name:
            调用者描述
        """
        self.storage_chain_class = storage_chain_class
        self.storage_tree = storage_tree
        self.storage_reference_manager = storage_reference_manager
        self.storage_obj = storage_obj
        self.timestamp = timestamp if timestamp else self._get_timestamp_from_qcow_storage(storage_obj)
        self.chain = None
        self._uuid_hex = uuid.uuid4().hex  # 对象唯一标识
        self.name = f'{caller_name} # {self} {self._uuid_hex}' if caller_name else f'{self} {self._uuid_hex}'
        self.node = self.storage_tree.get_node_by_storage_obj(storage_obj)
        assert self.node is not None

    def __str__(self):
        if self.storage_obj.is_cdp_file:
            return f'query chain by storage : <{self.storage_obj.disk_snapshot_storage_ident}|{self.timestamp}>'
        else:
            return f'query chain by storage : <{self.storage_obj.disk_snapshot_storage_ident}>'

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def _get_timestamp_from_qcow_storage(storage_obj):
        if storage_obj.is_cdp_file:
            return None
        else:
            assert storage_obj.storage_begin_timestamp == storage_obj.storage_end_timestamp
            return storage_obj.storage_begin_timestamp

    def get_storage_chain(self) -> chain.StorageChain:
        """获取快照存储链

        :remark:
            返回值会内部缓存，也就是获取的结果在第一次调用时就固定
        :raises:
             xdata.DiskSnapshotStorageInvalid 磁盘快照不可用
        """
        if not self.chain:
            self._check_valid()
            self.chain = self._generate_storage_chain()
        return self.chain

    def _check_valid(self):
        if self.storage_obj.storage_status == m.DiskSnapshotStorage.RECYCLED:
            xlogging.raise_and_logging_error(
                '指定的备份已被标识为不可用', f'{self.storage_obj} storage_status is {self.storage_obj.storage_status}',
                print_args=False, exception_class=xdata.DiskSnapshotStorageInvalid)
        elif (not self.storage_obj.is_cdp_file) and self.timestamp != self.storage_obj.storage_begin_timestamp:
            xlogging.raise_and_logging_error(
                '内部异常，指定的备份时刻不可用',
                f'{self.storage_obj} timestamp {self.timestamp} != {self.storage_obj.storage_begin_timestamp}',
                print_args=False, exception_class=xdata.DiskSnapshotStorageInvalid)
        else:
            pass  # do nothing

    def _generate_storage_chain(self):
        storage_chain = self.storage_chain_class(self.timestamp, self.storage_reference_manager, f'{self.name}')

        for _node in tree.dfs_to_root(self.node):
            storage_chain.insert_head(_node.storage_obj)

        return storage_chain
