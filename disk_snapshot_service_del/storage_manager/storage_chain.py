import abc
import decimal
import uuid

from rest_framework import serializers

from basic_library import xlogging
from storage_manager import models as m

_logger = xlogging.getLogger(__name__)


class DiskSnapshotStorageSerializer(serializers.ModelSerializer):
    class Meta:
        model = m.DiskSnapshotStorage
        fields = '__all__'


class StorageChain(abc.ABC):
    """快照存储链基类

    :remark:
        acquire方法与release方法配对使用
        acquire方法调用后，在调用release方法之前，不可重入
        快照存储数据为storage的数据库对象转换为的字典对象，而非数据库对象
        命名规范参考 http://172.16.1.11/AIO/DiskSnapshotService/wikis/names#%E7%A3%81%E7%9B%98%E5%BF%AB%E7%85%A7%E5%AD%98%E5%82%A8-disksnapshotstorage
    """

    def __init__(self, timestamp: decimal.Decimal, storage_reference_manager, caller_name: str, name_prefix: str):
        self.timestamp = timestamp
        self.storage_reference_manager = storage_reference_manager
        self.name = f'{name_prefix} | {uuid.uuid4().hex} | {caller_name}'
        self._storage_info_list = list()
        self._valid = False
        self._key_storage_info_list = None  # 关键快照存储链

    def __del__(self):
        if self._valid:
            _logger.warning(f'{self.name} NOT call release')
            self.release()

    def release(self):
        self._valid = False
        self._key_storage_info_list = None

    def acquire(self):
        assert not self._valid
        assert not self.is_empty()
        try:
            self._key_storage_info_list = self._query_key_storage_info_list()
            self._valid = True
        except Exception:
            self.release()
            raise

    def _query_key_storage_info_list(self):
        """获取“关键”storage列表"""
        array = list()
        storage_info_list_len = len(self._storage_info_list)
        storage_info_list_max_i = storage_info_list_len - 1

        for i in range(storage_info_list_len):
            storage_info = self._storage_info_list[i]
            assert storage_info['storage_status'] != m.DiskSnapshotStorage.RECYCLED

            if i == storage_info_list_max_i:
                array.append(storage_info)  # 最后一个节点
                continue
            if i == 0 and storage_info['file_level_deduplication']:
                assert storage_info['parent_snapshot'] is None
                array.append(storage_info)  # 根节点且有文件级去重
                continue
            if storage_info['image_path'] != self._storage_info_list[i + 1]['image_path']:
                array.append(storage_info)  # 与下一个节点不在同一个文件中
                continue
            if self._storage_info_list[i + 1]['storage_status'] in m.DiskSnapshotStorage.STATUS_WRITING:
                array.append(storage_info)  # 下一个节点正在写入数据中
                continue

        return array

    def insert_head(self, storage_obj):
        assert not self._valid
        self._storage_info_list.insert(0, DiskSnapshotStorageSerializer(storage_obj).data)

    def insert_tail(self, storage_obj):
        assert not self._valid
        self._storage_info_list.append(DiskSnapshotStorageSerializer(storage_obj).data)

    def is_empty(self):
        return len(self._storage_info_list) == 0

    @property
    def storage_info_list(self) -> list:
        """获取所有快照存储节点数组"""
        assert self._valid
        assert self._storage_info_list
        return self._storage_info_list.copy()

    @property
    @abc.abstractmethod
    def storages(self) -> list:
        raise NotImplementedError()


class StorageChainForRead(StorageChain):
    """供读取时使用的快照存储链"""

    def __init__(self, timestamp: decimal.Decimal, storage_reference_manager, caller_name: str):
        super(StorageChainForRead, self).__init__(timestamp, storage_reference_manager, caller_name, 'r')

    def release(self):
        super(StorageChainForRead, self).release()
        self.storage_reference_manager.remove_reading_record(self.name)

    def acquire(self):
        assert self._storage_info_list[-1]['storage_status'] != m.DiskSnapshotStorage.CREATING
        try:
            super(StorageChainForRead, self).acquire()
            self.storage_reference_manager.add_reading_record(self.name, self._key_storage_info_list)
            return self
        except Exception:
            self.release()
            raise

    @property
    def storages(self) -> list:
        """读取快照存储链需要打开的storage列表"""
        assert self._valid
        assert self._key_storage_info_list
        return self._key_storage_info_list.copy()


class StorageChainForWrite(StorageChain):
    """供写入时使用的快照存储链（仅供内部特殊逻辑使用）

    :remark:
        链中的最后一个元素为将要写入数据的快照存储
        该写入链不支持边读边写模式
    """

    def __init__(self, timestamp: decimal.Decimal, storage_reference_manager, caller_name: str):
        super(StorageChainForWrite, self).__init__(timestamp, storage_reference_manager, caller_name, 'w')
        self._key_storage_info_list_for_write = None  # 关键快照存储链

    def _query_key_storage_info_list_for_write(self):
        """获取写入时的关键storage列表"""
        assert self._storage_info_list[-1]['storage_status'] == m.DiskSnapshotStorage.CREATING
        writing_image_path = self._storage_info_list[-1]['image_path']
        return [info for info in self._key_storage_info_list if info['image_path'] == writing_image_path]

    def release(self):
        super(StorageChainForWrite, self).release()
        self.storage_reference_manager.remove_writing_record(self.name)
        self._key_storage_info_list_for_write = None

    def acquire(self):
        try:
            super(StorageChainForWrite, self).acquire()
            self._key_storage_info_list_for_write = self._query_key_storage_info_list_for_write()
            self.storage_reference_manager.add_writing_record(self.name, self._storage_info_list[-1])
            return self
        except Exception:
            self.release()
            raise

    @property
    def storages(self) -> list:
        """写入快照存储时需要打开的storage列表"""
        assert self._valid
        assert self._key_storage_info_list_for_write
        return self._key_storage_info_list_for_write.copy()


class StorageChainForRW(StorageChain):
    """供可读写使用的快照存储链

    :remark:
        链中的最后一个元素为将要写入数据的快照存储
    """

    def __init__(self, timestamp: decimal.Decimal, storage_reference_manager, caller_name: str):
        super(StorageChainForRW, self).__init__(timestamp, storage_reference_manager, caller_name, 'rw')

    def release(self):
        super(StorageChainForRW, self).release()
        self.storage_reference_manager.remove_writing_record(self.name)
        self.storage_reference_manager.remove_reading_record(self.name)

    def acquire(self):
        try:
            super(StorageChainForRW, self).acquire()
            self.storage_reference_manager.add_reading_record(self.name, self._key_storage_info_list)
            self.storage_reference_manager.add_writing_record(self.name, self._storage_info_list[-1])
            return self
        except Exception:
            self.release()
            raise

    @property
    def storages(self) -> list:
        """可读写快照存储，需要打开的storage列表"""
        assert self._valid
        assert self._key_storage_info_list
        return self._key_storage_info_list.copy()
