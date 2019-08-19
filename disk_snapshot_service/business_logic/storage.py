import typing

from cpkt.core import xlogging as lg

from data_access import models as m
from data_access import storage

_logger = lg.get_logger(__name__)


class StorageItem(object):
    """快照存储元素项

    数据库对象不能保证线程安全，从数据库对象中复制数据作全局使用
    """

    def __init__(self, storage_obj: m.SnapshotStorage):
        self._ident = storage_obj.ident
        self._parent_ident = storage_obj.parent_ident
        self._parent_timestamp = storage_obj.parent_timestamp
        self._type = storage_obj.type
        self._disk_bytes = storage_obj.disk_bytes
        self._status = storage_obj.status
        self._image_path = storage_obj.image_path
        self._file_level_deduplication = storage_obj.file_level_deduplication
        self._is_cdp = storage_obj.is_cdp
        self._is_qcow = storage_obj.is_qcow

    @property
    def ident(self):
        return self._ident

    @property
    def parent_ident(self):
        return self._parent_ident

    @property
    def parent_timestamp(self):
        return self._parent_timestamp

    @property
    def type(self):
        return self._type

    @property
    def disk_bytes(self):
        return self._disk_bytes

    @property
    def status(self):
        return self._status

    @property
    def image_path(self):
        return self._image_path

    @property
    def file_level_deduplication(self):
        return self._file_level_deduplication

    @property
    def is_cdp(self):
        return self._is_cdp

    @property
    def is_qcow(self):
        return self._is_qcow


class Storage(object):
    def __init__(self, storage_obj: m.SnapshotStorage):
        self.storage_obj: m.SnapshotStorage = storage_obj

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return f'Snapshot: {self.storage_obj.ident}-{self.storage_obj.status_display}-{self.storage_obj.parent_ident}'

    def update_status(self, new_status):
        old_status = self.status
        storage.update_obj_status(self.storage_obj, new_status)
        _logger.info(f'update [{self.ident}] status from {old_status} to {self.status}')

    def update_parent(self, parent_storage):
        old_parent_ident = self.parent_ident
        if parent_storage:
            storage.update_obj_parent(self.storage_obj, parent_storage.storage_obj)
        else:
            storage.update_obj_parent(self.storage_obj, None)
        _logger.info(f'update [{self.ident}] parent from {old_parent_ident} to {self.parent_ident}')

    @property
    def ident(self):
        return self.storage_obj.ident

    @property
    def parent_ident(self):
        return self.storage_obj.parent_ident

    @property
    def parent_timestamp(self):
        return self.storage_obj.parent_timestamp

    @property
    def status(self):
        return self.storage_obj.status

    @property
    def tree_ident(self):
        return self.storage_obj.tree_ident

    @property
    def disk_bytes(self):
        return self.storage_obj.disk_bytes

    @property
    def start_timestamp(self):
        return self.storage_obj.start_timestamp

    @property
    def finish_timestamp(self):
        return self.storage_obj.finish_timestamp

    @property
    def is_cdp(self):
        return self.storage_obj.is_cdp

    @property
    def is_qcow(self):
        return self.storage_obj.is_qcow

    @property
    def image_path(self):
        return self.storage_obj.image_path


def create_new_storage(
        storage_ident, parent_ident, parent_timestamp, is_cdp, disk_bytes, image_path, tree_ident) -> Storage:
    storage_obj = storage.create_obj(
        storage_ident=storage_ident,
        parent_ident=parent_ident,
        parent_timestamp=parent_timestamp,
        storage_type=m.SnapshotStorage.TYPE_CDP if is_cdp else m.SnapshotStorage.TYPE_QCOW,
        disk_bytes=disk_bytes,
        status=m.SnapshotStorage.STATUS_CREATING,
        image_path=image_path,
        tree_ident=tree_ident,
    )
    _logger.info(f'new storage obj created : <{storage_obj}>')
    return Storage(storage_obj)


def query_by_ident(ident: str) -> typing.Union[Storage, None]:
    storage_obj = storage.get_obj_by_ident(ident)
    if storage_obj:
        return Storage(storage_obj)
    else:
        return None


def is_image_path_using(image_path) -> bool:
    return storage.query_image_path_using_count(image_path) != 0


def count_exist_in_file(image_path) -> int:
    return storage.query_image_path_exist_count(image_path)
