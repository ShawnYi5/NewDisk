import os
from functools import lru_cache
from pathlib import Path

from basic_library import rwlock
from basic_library import xdata
from basic_library import xfunctions
from basic_library import xlogging

_logger = xlogging.getLogger(__name__)

_valid_storage_directory_cache = set()
_valid_storage_directory_locker = rwlock.RWLockWrite()


class ValidStorageDirectory(object):
    """有效的快照存储目录

    :remark:
        设计上所有的 storage 都存储在有效的快照存储目录中
        快照存储目录由其他组件负责挂载与检测其有效性，并由其他组件管理该组件中记录
    """

    def __init__(self, storage_directory):
        self.storage_directory = storage_directory
        self._storage_directory_str_len = len(storage_directory)
        assert os.path.isdir(storage_directory)
        assert len(Path(storage_directory).parents) > 2

    def __str__(self):  # pragma: no cover
        return f'ValidStorageDirectory : {self.storage_directory}'

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash(self.storage_directory)

    def __eq__(self, other):
        return self.storage_directory == other.storage_directory

    @lru_cache(maxsize=1024 * 100)  # 按照一个缓存数据0.5K计算，最大缓存50M的数据
    @xfunctions.convert_exception_to_value(False, _logger.debug)
    def is_include(self, file_path):
        return self._storage_directory_str_len == len(os.path.commonpath([self.storage_directory, file_path, ]))


def check_path(file_path, raise_exception=True):
    """检测路径是否在有效的快照存储目录中

    :raises:
        xdata.StorageDirectoryInvalid 路径不在有效的快照存储目录中
    """
    assert os.path.isabs(file_path)

    with _valid_storage_directory_locker.gen_rlock():
        for valid in _valid_storage_directory_cache:
            if valid.is_include(file_path):
                return True

    if raise_exception:
        xlogging.raise_and_logging_error('数据存储目录未挂载', f'{file_path} not in "valid storage directory"',
                                         print_args=False, exception_class=xdata.StorageDirectoryInvalid)
    else:
        return False


def add_directory(directory_path):
    with _valid_storage_directory_locker.gen_wlock():
        directory = ValidStorageDirectory(directory_path)
        _valid_storage_directory_cache.add(directory)


def remove_directory(directory_path):
    with _valid_storage_directory_locker.gen_wlock():
        directory = ValidStorageDirectory(directory_path)
        _valid_storage_directory_cache.discard(directory)
