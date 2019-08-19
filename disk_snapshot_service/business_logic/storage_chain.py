import abc
import typing

from cpkt.core import xlogging as lg

from basic_library import xfunctions as xf
from business_logic import storage
from business_logic import storage_reference_manager as srm
from data_access import models as m

_logger = lg.get_logger(__name__)


class StorageChain(abc.ABC):
    """快照存储链基类

    :remark:
        acquire方法与release方法配对使用
        acquire方法调用后，在调用release方法之前，不可重入
        命名规范参考 http://172.16.1.11/AIO/DiskSnapshotService/wikis/names#%E7%A3%81%E7%9B%98%E5%BF%AB%E7%85%A7%E5%AD%98%E5%82%A8-disksnapshotstorage
        storage_obj均为只读，不应该发生写入操作
    """

    def __init__(self, reference_manager, caller_name: str, name_prefix: str, timestamp):
        self.timestamp = timestamp
        self._reference_manager: srm.StorageReferenceManager = reference_manager
        self.name = f'{name_prefix} | {xf.generate_unique_number(xf.UNIQUE_NUMBER_STORAGE_CHAIN)} | {caller_name}'
        self._storage_items: typing.List[storage.StorageItem] = list()  # 快照链上的所有存储对象
        self._valid = False
        self._key_storage_items: typing.List[storage.StorageItem] = None  # 快照链上的关键对象，打开快照时，仅需要这些对象

    def __del__(self):
        if self._valid:
            _logger.warning(f'!!! {self.name} NOT call release !!! must fix !!!')
            self.release()

    def release(self):
        """释放快照链上的关键对象

        :remark: 派生类需要释放掉“引用标记”，使对应存储对象进入“解锁”状态
        """
        self._valid = False
        self._key_storage_items = None

    def acquire(self):
        """获取快照链上的关键对象

        :remark: 派生类需要根据业务逻辑进行“引用标记”，使对应存储对象进入“锁定”状态
        """
        assert not self._valid
        assert not self.is_empty()
        try:
            self._key_storage_items = self._query_key_storage_items()
            self._valid = True
        except Exception:
            self.release()
            raise

    def _query_key_storage_items(self) -> typing.List[storage.StorageItem]:
        """获取“关键”对象"""

        key_items = list()
        storage_items_count = len(self._storage_items)
        storage_info_list_max_i = storage_items_count - 1

        for i, storage_item in enumerate(self._storage_items):
            assert storage_item.status not in (m.SnapshotStorage.STATUS_DELETED, m.SnapshotStorage.STATUS_ABNORMAL,)

            if i == storage_info_list_max_i:
                key_items.append(storage_item)  # 最后一个节点
                continue
            if i == 0 and storage_item.file_level_deduplication:
                assert storage_item.parent_ident is None
                key_items.append(storage_item)  # 根节点且有文件级去重
                continue
            if storage_item.image_path != self._storage_items[i + 1].image_path:
                key_items.append(storage_item)  # 与下一个节点不在同一个文件中
                continue
            if self._storage_items[i + 1].status == m.SnapshotStorage.STATUS_WRITING:
                key_items.append(storage_item)  # 下一个节点正在写入数据中
                continue

        return key_items

    def insert_head(self, storage_obj: m.SnapshotStorage):
        assert not self._valid
        self._storage_items.insert(0, storage.StorageItem(storage_obj))
        return self

    def insert_tail(self, storage_obj: m.SnapshotStorage):
        assert not self._valid
        self._storage_items.append(storage.StorageItem(storage_obj))
        return self

    def is_empty(self):
        return len(self._storage_items) == 0

    @property
    def storage_items(self) -> typing.List[storage.StorageItem]:
        """获取“所有”快照存储节点"""
        assert self._valid
        assert self._storage_items
        return self._storage_items

    @property
    @abc.abstractmethod
    def key_storage_items(self) -> typing.List[storage.StorageItem]:
        """获取“关键”快照存储节点"""
        raise NotImplementedError()

    @property
    def last_storage_item(self) -> storage.StorageItem:
        assert self._storage_items
        return self._storage_items[-1]


class StorageChainForRead(StorageChain):
    """供读取时使用的快照存储链"""

    def __init__(self, reference_manager, caller_name: str, timestamp=None):
        super(StorageChainForRead, self).__init__(reference_manager, caller_name, 'r', timestamp)

    def release(self):
        super(StorageChainForRead, self).release()
        self._reference_manager.remove_reading_record(self.name)

    def acquire(self):
        for storage_item in self._storage_items:
            assert storage_item.status not in (
                m.SnapshotStorage.STATUS_CREATING, m.SnapshotStorage.STATUS_ABNORMAL, m.SnapshotStorage.STATUS_DELETED)
        try:
            super(StorageChainForRead, self).acquire()
            self._reference_manager.add_reading_record(self.name, self._key_storage_items)
            return self
        except Exception:
            self.release()
            raise

    @property
    def key_storage_items(self) -> typing.List[storage.StorageItem]:
        """读取快照存储链需要打开的storage列表"""
        assert self._valid
        assert self._key_storage_items
        return self._key_storage_items


class StorageChainForWrite(StorageChain):
    """供写入时使用的快照存储链（仅供创建CDP文件与回收逻辑使用）

    :remark:
        链中的最后一个元素为将要写入数据的快照存储
        当写入文件是qcow时，该写入快照点不支持边读边写模式
    """

    def __init__(self, reference_manager, caller_name: str, timestamp=None):
        super(StorageChainForWrite, self).__init__(reference_manager, caller_name, 'w', timestamp)
        self._key_storage_items_for_write = None  # 关键快照存储链

    def _query_key_storage_items_for_write(self):
        """获取写入时的关键storage列表"""
        last_item = self._storage_items[-1]
        assert last_item.status == m.SnapshotStorage.STATUS_CREATING
        if last_item.is_cdp:
            return [last_item, ]
        else:
            writing_image_path = last_item.image_path
            return [item for item in self._key_storage_items if item.image_path == writing_image_path]

    def release(self):
        super(StorageChainForWrite, self).release()
        self._reference_manager.remove_writing_record(self.name)
        self._key_storage_items_for_write = None

    def acquire(self):
        try:
            super(StorageChainForWrite, self).acquire()
            self._key_storage_items_for_write = self._query_key_storage_items_for_write()
            self._reference_manager.add_writing_record(self.name, self.last_storage_item)
            return self
        except Exception:
            self.release()
            raise

    @property
    def key_storage_items(self) -> list:
        """写入快照存储时需要打开的storage列表"""
        assert self._valid
        assert self._key_storage_items_for_write
        return self._key_storage_items_for_write


class StorageChainForRW(StorageChain):
    """供可读写使用的快照存储链

    :remark:
        链中的最后一个元素为将要写入数据的快照存储
    """

    def __init__(self, reference_manager, caller_name: str, timestamp=None):
        super(StorageChainForRW, self).__init__(reference_manager, caller_name, 'rw', timestamp)

    def release(self):
        super(StorageChainForRW, self).release()
        self._reference_manager.remove_writing_record(self.name)
        self._reference_manager.remove_reading_record(self.name)

    def acquire(self):
        try:
            super(StorageChainForRW, self).acquire()
            self._reference_manager.add_reading_record(self.name, self._key_storage_items)
            self._reference_manager.add_writing_record(self.name, self.last_storage_item)
            return self
        except Exception:
            self.release()
            raise

    @property
    def key_storage_items(self) -> list:
        """可读写快照存储，需要打开的storage列表"""
        assert self._valid
        assert self._key_storage_items
        return self._key_storage_items


class ChainGuarder(xf.DataHolder):
    def __init__(self):
        super(ChainGuarder, self).__init__(None)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.value:
            self.value.release()
