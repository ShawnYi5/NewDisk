import pytest

from basic_library import xdata
from storage_manager import models
from storage_manager import storage_locker_manager as slm

pytestmark = pytest.mark.django_db


def test_multi_caller():
    """多个调用者"""

    m = slm.StorageLockerManager.get_storage_locker_manager()
    root_ident = models.DiskSnapshotStorageRoot.get_recycle_root_obj().root_ident

    with m.get_locker(root_ident, 'caller_one'):
        two = m.get_locker(root_ident, 'caller_two')

    with two:
        one = m.get_locker(root_ident, 'caller_one')

    two = m.get_locker(root_ident, 'caller_two')

    with one:
        pass  # do nothing

    with two:
        pass  # do nothing


def test_invalid_param_one():
    """传入无效的root_ident，触发 xdata.StorageLockerNotExist"""

    m = slm.StorageLockerManager.get_storage_locker_manager()

    with pytest.raises(xdata.StorageLockerNotExist):
        m.get_locker('!!!never exist root ident!!!', __name__)


def test_invalid_param_two():
    """触发 xdata.StorageLockerRepeatGet"""

    m = slm.StorageLockerManager.get_storage_locker_manager()
    root_ident = models.DiskSnapshotStorageRoot.get_recycle_root_obj().root_ident

    locker = m.get_locker(root_ident, __name__)
    locker.__enter__()

    with pytest.raises(xdata.StorageLockerRepeatGet):
        locker = m.get_locker(root_ident, __name__)

    locker.__exit__(None, None, None)

    with m.get_locker(root_ident, __name__):
        pass


def test_remove_root_ident_before_release_locker():
    """测试在释放锁以前就释放掉root_ident"""

    m = slm.StorageLockerManager.get_storage_locker_manager()
    storage_root_obj = models.DiskSnapshotStorageRoot.get_recycle_root_obj()
    root_ident = storage_root_obj.root_ident

    locker = m.get_locker(root_ident, __name__)
    locker.__enter__()

    storage_root_obj.root_valid = False
    storage_root_obj.save(update_fields=['root_valid', ])

    locker.__exit__(None, None, None)

    storage_root_obj.root_valid = True
    storage_root_obj.save(update_fields=['root_valid', ])

    with m.get_locker(root_ident, __name__):
        pass


def test_remove_locker_multi():
    """多次移除root_ident"""
    storage_root_obj = models.DiskSnapshotStorageRoot.get_recycle_root_obj()

    storage_root_obj.root_valid = False
    storage_root_obj.save(update_fields=['root_valid', ])

    storage_root_obj.root_valid = False
    storage_root_obj.save(update_fields=['root_valid', ])
