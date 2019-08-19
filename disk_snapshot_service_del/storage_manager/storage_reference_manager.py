import threading
from functools import lru_cache

from basic_library import rwlock
from basic_library import xdata
from basic_library import xfunctions
from basic_library import xlogging

_logger = xlogging.getLogger(__name__)

_storage_reference_manager = None
_storage_reference_manager_locker = threading.Lock()


class StorageReferenceManager(object):
    """快照存储引用管理器

    使用中（含逻辑上将要使用）的快照存储进行统一管理
    提供查询接口，支持业务逻辑获知 某快照 或 某文件 当前是否被引用
    引用有两类：1. 读取 2. 写入
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
        def __init__(self, storage_info):
            self.storage_ident = storage_info['disk_snapshot_storage_ident']
            self.storage_path = storage_info['image_path']
            self.timestamp = xfunctions.current_timestamp()

        def __str__(self):
            return f'{xfunctions.humanize_timestamp(self.timestamp)}|{self.storage_path}|{self.storage_ident}'

        def __repr__(self):
            return self.__str__()

    def __init__(self):
        self.reading_record_dict = dict()
        self.rr_locker = rwlock.RWLockWrite()
        self.writing_record_dict = dict()
        self.wr_locker = rwlock.RWLockWrite()

    def add_reading_record(self, caller_name: str, storage_info_list: list):
        assert caller_name
        with self.rr_locker.gen_wlock():
            assert caller_name not in self.reading_record_dict
            self.reading_record_dict[caller_name] = [self.Record(storage_info) for storage_info in storage_info_list]
            self.is_storage_using.cache_clear()

    def remove_reading_record(self, caller_name: str):
        assert caller_name
        with self.rr_locker.gen_wlock():
            if self.reading_record_dict.pop(caller_name, None):
                self.is_storage_using.cache_clear()

    def add_writing_record(self, caller_name: str, storage_info: dict):
        assert caller_name
        with self.wr_locker.gen_wlock():
            assert caller_name not in self.writing_record_dict
            for record in self.writing_record_dict.values():
                if record.storage_path == storage_info['image_path']:
                    xlogging.raise_and_logging_error(
                        '快照镜像文件正在写入中', f'repeat add writing storage ref : {record}',
                        print_args=False, exception_class=xdata.StorageReferenceRepeated)
            self.writing_record_dict[caller_name] = self.Record(storage_info)
            self.is_storage_using.cache_clear()
            self.is_storage_writing.cache_clear()

    def remove_writing_record(self, caller_name: str):
        assert caller_name
        with self.wr_locker.gen_wlock():
            if self.writing_record_dict.pop(caller_name, None):
                self.is_storage_using.cache_clear()
                self.is_storage_writing.cache_clear()

    @lru_cache(None)
    def is_storage_using(self, storage_ident):
        with self.rr_locker.gen_rlock():
            for record_list in self.reading_record_dict.values():
                for record in record_list:
                    if record.storage_ident == storage_ident:
                        return True
        with self.wr_locker.gen_rlock():
            for record in self.writing_record_dict.values():
                if record.storage_ident == storage_ident:
                    return True
        return False

    @lru_cache(None)
    def is_storage_writing(self, storage_path):
        with self.wr_locker.gen_rlock():
            for record in self.writing_record_dict.values():
                if record.storage_path == storage_path:
                    return True
        return False
