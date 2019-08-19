import threading

from django.db.models.signals import post_save
from django.dispatch import receiver

from basic_library import rwlock
from basic_library import xdata
from basic_library import xlogging
from storage_manager import models as m

_logger = xlogging.getLogger(__name__)

_storage_locker_manager = None
_storage_locker_manager_locker = threading.Lock()


class StorageLocker(object):
    """快照存储锁"""

    def __init__(self, manager, locker: threading.Lock, root_ident: str, caller_ident: str):
        self.manager = manager
        self.locker = locker
        self.root_ident = root_ident
        self.caller_ident = caller_ident

    def __del__(self):
        self._destroy()

    def _destroy(self):
        if self.manager:
            self.manager.release_locker(self)
            self.manager = None
            self.locker = None
        else:
            pass  # do nothing

    def __enter__(self):
        assert self.locker
        self.locker.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        r = self.locker.__exit__(exc_type, exc_val, exc_tb)
        self._destroy()
        return r


class StorageLockerManager(object):
    """快照存储锁管理器

    读写快照存储的数据库对象时，需要进入锁空间
    remark：
        该锁空间内的IO操作仅限数据库操作
    """

    @staticmethod
    def get_storage_locker_manager():
        global _storage_locker_manager

        if _storage_locker_manager is None:
            with _storage_locker_manager_locker:
                if _storage_locker_manager is None:
                    _storage_locker_manager = StorageLockerManager()
        return _storage_locker_manager

    def __init__(self):
        self.locker_cache_dict = dict()
        self.lr_locker = rwlock.RWLockWrite()
        self._load_all_locker()

    def _load_all_locker(self):
        for root_obj in m.DiskSnapshotStorageRoot.get_valid_objs().all():
            self.locker_cache_dict[root_obj.root_ident] = {
                'locker': threading.Lock(),
                'caller': set(),
                'caller_locker': threading.Lock(),
            }

    def get_locker(self, storage_root_ident: str, caller_ident: str) -> StorageLocker:
        """获取锁对象

        :remark:
            获取的对象仅能使用with语法调用，且调用一次后就会无效
        :param storage_root_ident:
            快照存储标识
        :param caller_ident:
            调用者标识
        :return:
            快照存储锁对象
        :raises:
            xdata.StorageLockerNotExist
                快照存储锁不存在
            xdata.StorageLockerRepeatGet
                重复获取快照存储锁
        """
        with self.lr_locker.gen_rlock():
            locker_cache = self.locker_cache_dict.get(storage_root_ident, None)
            if not locker_cache:
                xlogging.raise_and_logging_error(f'无法通过标识符“{storage_root_ident}”获取快照存储锁',
                                                 f'get_locker : {storage_root_ident} not in locker_cache',
                                                 print_args=False, exception_class=xdata.StorageLockerNotExist)

        with locker_cache['caller_locker']:
            if caller_ident in locker_cache['caller']:
                xlogging.raise_and_logging_error(f'不支持重复获取快照存储锁',
                                                 f'get_locker : {caller_ident} in {locker_cache}',
                                                 print_args=False, exception_class=xdata.StorageLockerRepeatGet)
            else:
                locker_cache['caller'].add(caller_ident)

        return StorageLocker(self, locker_cache['locker'], storage_root_ident, caller_ident)

    def release_locker(self, locker: StorageLocker):
        with self.lr_locker.gen_rlock():
            locker_cache = self.locker_cache_dict.get(locker.root_ident, None)
            if not locker_cache:
                return

        assert locker_cache['locker'] == locker.locker

        with locker_cache['caller_locker']:
            locker_cache['caller'].discard(locker.caller_ident)

    def add_locker(self, storage_root_ident):
        with self.lr_locker.gen_wlock():
            locker_cache = self.locker_cache_dict.get(storage_root_ident, None)
            if locker_cache:
                _logger.debug(f'repeat add locker : {storage_root_ident}')
                return

            self.locker_cache_dict[storage_root_ident] = {
                'locker': threading.Lock(),
                'caller': set(),
                'caller_locker': threading.Lock(),
            }
        _logger.info(f'add locker : {storage_root_ident}')

    def remove_locker(self, storage_root_ident):
        with self.lr_locker.gen_wlock():
            locker_cache = self.locker_cache_dict.pop(storage_root_ident, None)
            if not locker_cache:
                return
            with locker_cache['caller_locker']:
                if locker_cache['caller']:
                    _logger.warning(f"caller not empty when remove locker : {locker_cache['caller']}")
        _logger.info(f'remove locker : {storage_root_ident}')


@receiver(post_save, sender=m.DiskSnapshotStorageRoot)
def storage_root_post_save(sender, instance, **kwargs):
    _ = sender
    _ = kwargs
    storage_root_obj = instance
    if storage_root_obj.root_valid:
        StorageLockerManager.get_storage_locker_manager().add_locker(storage_root_obj.root_ident)
    else:
        StorageLockerManager.get_storage_locker_manager().remove_locker(storage_root_obj.root_ident)
