import threading
import typing
from functools import lru_cache

from cpkt.core import exc
from cpkt.core import rwlock
from cpkt.core import xlogging as lg

from basic_library import xdata
from basic_library import xfunctions
from business_logic import storage

_logger = lg.get_logger(__name__)

_storage_reference_manager = None
_storage_reference_manager_locker = threading.Lock()


class StorageReferenceManager(object):
    """快照存储引用管理器

     :remark:
        使用中(含逻辑上将要使用)的快照存储进行统一管理

        接口：
            1 支持业务逻辑获知："某快照/某文件"是否处于"读取/写入"状态
            2 创建"快照存储引用管理器"对象
    """

    @staticmethod
    def get_storage_reference_manager():

        global _storage_reference_manager

        if _storage_reference_manager is None:
            with _storage_reference_manager_locker:
                if _storage_reference_manager is None:
                    _storage_reference_manager = StorageReferenceManager()
        return _storage_reference_manager

    class Record(object):

        def __init__(self, storage_item: storage.StorageItem):
            self.storage_ident = storage_item.ident
            self.storage_path = storage_item.image_path
            self.timestamp = xfunctions.current_timestamp()

        def __str__(self):
            return f'{xfunctions.humanize_timestamp(self.timestamp)}|{self.storage_path}|{self.storage_ident}'

        def __repr__(self):
            return self.__str__()

    def __init__(self):

        self.reading_record_dict = dict()
        self.reading_record_locker = rwlock.RWLockWrite()
        self.writing_record_dict = dict()
        self.writing_record_locker = rwlock.RWLockWrite()

    @lru_cache(None)
    def is_storage_using(self, storage_ident):

        with self.reading_record_locker.gen_rlock():
            for record_list in self.reading_record_dict.values():
                for record in record_list:
                    if record.storage_ident == storage_ident:
                        return True
        with self.writing_record_locker.gen_rlock():
            for record in self.writing_record_dict.values():
                if record.storage_ident == storage_ident:
                    return True
        return False

    @lru_cache(None)
    def is_storage_writing(self, storage_path):

        with self.writing_record_locker.gen_rlock():
            for record in self.writing_record_dict.values():
                if record.storage_path == storage_path:
                    return True
        return False

    def add_reading_record(self, caller_name: str, storage_items: typing.List[storage.StorageItem]):

        assert caller_name
        with self.reading_record_locker.gen_wlock():
            assert caller_name not in self.reading_record_dict
            self.reading_record_dict[caller_name] = [self.Record(storage_item) for storage_item in storage_items]
            self.is_storage_using.cache_clear()

    def remove_reading_record(self, caller_name: str):

        assert caller_name
        with self.reading_record_locker.gen_wlock():
            if self.reading_record_dict.pop(caller_name, None):
                self.is_storage_using.cache_clear()

    def add_writing_record(self, caller_name: str, storage_item: storage.StorageItem):

        def _check_reference_repeated():
            for record in self.writing_record_dict.values():
                if record.storage_path == storage_item.image_path:
                    raise exc.generate_exception_and_logger(
                        '快照镜像文件正在写入中', f'repeat add writing storage ref : {record}', 0,
                        exception_class=xdata.StorageReferenceRepeated)

        assert caller_name
        with self.writing_record_locker.gen_wlock():
            assert caller_name not in self.writing_record_dict
            _check_reference_repeated()

            self.writing_record_dict[caller_name] = self.Record(storage_item)
            self.is_storage_using.cache_clear()
            self.is_storage_writing.cache_clear()

    def remove_writing_record(self, caller_name: str):

        assert caller_name
        with self.writing_record_locker.gen_wlock():
            if self.writing_record_dict.pop(caller_name, None):
                self.is_storage_using.cache_clear()
                self.is_storage_writing.cache_clear()


def get_srm() -> StorageReferenceManager:
    return StorageReferenceManager.get_storage_reference_manager()
