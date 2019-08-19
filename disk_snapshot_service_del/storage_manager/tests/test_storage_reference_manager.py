from unittest.mock import MagicMock, patch

import pytest

from basic_library import xdata
from storage_manager import storage_reference_manager as srm


def test_normal_one():
    manage = srm.StorageReferenceManager.get_storage_reference_manager()

    reading = {
        'caller_one': [
            {'disk_snapshot_storage_ident': 'test_srm_ident_one', 'image_path': 'test_srm_ident_image_path_one'},
            {'disk_snapshot_storage_ident': 'test_srm_ident_two', 'image_path': 'test_srm_ident_image_path_two'},
        ],
        'caller_two': [
            {'disk_snapshot_storage_ident': 'test_srm_ident_three', 'image_path': 'test_srm_ident_image_path_one'},
            {'disk_snapshot_storage_ident': 'test_srm_ident_four', 'image_path': 'test_srm_ident_image_path_three'},
        ],
    }

    writing = {
        'caller_one':
            {'disk_snapshot_storage_ident': 'test_srm_ident_five', 'image_path': 'test_srm_ident_image_path_four'},
        'caller_three':
            {'disk_snapshot_storage_ident': 'test_srm_ident_six', 'image_path': 'test_srm_ident_image_path_three'},
    }

    assert not manage.is_storage_using('never_exist_storage_ident')
    assert not manage.is_storage_writing('never_exist_image_path')

    def _add_items():
        for _k, _v in reading.items():
            manage.add_reading_record(_k, _v)
        for _k, _v in writing.items():
            manage.add_writing_record(_k, _v)

    _add_items()

    assert not manage.is_storage_using('never_exist_storage_ident')
    assert not manage.is_storage_writing('never_exist_image_path')

    assert manage.is_storage_using('test_srm_ident_one')
    assert manage.is_storage_using('test_srm_ident_two')
    assert manage.is_storage_using('test_srm_ident_three')
    assert manage.is_storage_using('test_srm_ident_four')
    assert manage.is_storage_using('test_srm_ident_five')
    assert manage.is_storage_using('test_srm_ident_six')

    assert not manage.is_storage_writing('test_srm_ident_image_path_one')
    assert not manage.is_storage_writing('test_srm_ident_image_path_two')
    assert manage.is_storage_writing('test_srm_ident_image_path_three')
    assert manage.is_storage_writing('test_srm_ident_image_path_four')

    def _remove_reading():
        for _k in reading.keys():
            manage.remove_reading_record(_k)

    _remove_reading()

    assert not manage.is_storage_using('never_exist_storage_ident')
    assert not manage.is_storage_writing('never_exist_image_path')

    assert not manage.is_storage_using('test_srm_ident_one')
    assert not manage.is_storage_using('test_srm_ident_two')
    assert not manage.is_storage_using('test_srm_ident_three')
    assert not manage.is_storage_using('test_srm_ident_four')
    assert manage.is_storage_using('test_srm_ident_five')
    assert manage.is_storage_using('test_srm_ident_six')

    assert not manage.is_storage_writing('test_srm_ident_image_path_one')
    assert not manage.is_storage_writing('test_srm_ident_image_path_two')
    assert manage.is_storage_writing('test_srm_ident_image_path_three')
    assert manage.is_storage_writing('test_srm_ident_image_path_four')

    def _remove_writing():
        for _k in writing.keys():
            manage.remove_writing_record(_k)

    _remove_writing()

    assert not manage.is_storage_using('test_srm_ident_one')
    assert not manage.is_storage_using('test_srm_ident_two')
    assert not manage.is_storage_using('test_srm_ident_three')
    assert not manage.is_storage_using('test_srm_ident_four')
    assert not manage.is_storage_using('test_srm_ident_five')
    assert not manage.is_storage_using('test_srm_ident_six')

    assert not manage.is_storage_writing('test_srm_ident_image_path_one')
    assert not manage.is_storage_writing('test_srm_ident_image_path_two')
    assert not manage.is_storage_writing('test_srm_ident_image_path_three')
    assert not manage.is_storage_writing('test_srm_ident_image_path_four')


def test_normal_two():
    srm._storage_reference_manager = None
    manage = srm.StorageReferenceManager.get_storage_reference_manager()

    reading = {
        'caller_one': [
            {'disk_snapshot_storage_ident': 'test_srm_ident_one', 'image_path': 'test_srm_ident_image_path_one'},
            {'disk_snapshot_storage_ident': 'test_srm_ident_two', 'image_path': 'test_srm_ident_image_path_two'},
        ],
        'caller_two': [
            {'disk_snapshot_storage_ident': 'test_srm_ident_three', 'image_path': 'test_srm_ident_image_path_one'},
            {'disk_snapshot_storage_ident': 'test_srm_ident_four', 'image_path': 'test_srm_ident_image_path_three'},
        ],
    }

    writing = {
        'caller_one':
            {'disk_snapshot_storage_ident': 'test_srm_ident_five', 'image_path': 'test_srm_ident_image_path_four'},
        'caller_three':
            {'disk_snapshot_storage_ident': 'test_srm_ident_six', 'image_path': 'test_srm_ident_image_path_three'},
    }

    is_storage_using_cache_clear = MagicMock()
    is_storage_writing_cache_clear = MagicMock()

    @patch.object(target=srm.StorageReferenceManager.is_storage_using,
                  attribute='cache_clear', new=is_storage_using_cache_clear, )
    @patch.object(target=srm.StorageReferenceManager.is_storage_writing,
                  attribute='cache_clear', new=is_storage_writing_cache_clear, )
    def _add_items():
        for _k, _v in reading.items():
            manage.add_reading_record(_k, _v)
        for _k, _v in writing.items():
            manage.add_writing_record(_k, _v)

    _add_items()

    assert not manage.is_storage_using('never_exist_storage_ident')
    assert not manage.is_storage_writing('never_exist_image_path')

    assert manage.is_storage_using('test_srm_ident_one')
    assert manage.is_storage_using('test_srm_ident_two')
    assert manage.is_storage_using('test_srm_ident_three')
    assert manage.is_storage_using('test_srm_ident_four')
    assert manage.is_storage_using('test_srm_ident_five')
    assert manage.is_storage_using('test_srm_ident_six')

    assert not manage.is_storage_writing('test_srm_ident_image_path_one')
    assert not manage.is_storage_writing('test_srm_ident_image_path_two')
    assert manage.is_storage_writing('test_srm_ident_image_path_three')
    assert manage.is_storage_writing('test_srm_ident_image_path_four')

    is_storage_using_cache_clear.reset_mock()

    @patch.object(target=srm.StorageReferenceManager.is_storage_using,
                  attribute='cache_clear', new=is_storage_using_cache_clear, )
    @patch.object(target=srm.StorageReferenceManager.is_storage_writing,
                  attribute='cache_clear', new=is_storage_writing_cache_clear, )
    def _remove_reading():
        for _k in reading.keys():
            manage.remove_reading_record(_k)

    _remove_reading()

    assert is_storage_using_cache_clear.call_count == len(reading)

    """这里测试缓存是否生效"""

    assert not manage.is_storage_using('never_exist_storage_ident')
    assert not manage.is_storage_writing('never_exist_image_path')

    assert manage.is_storage_using('test_srm_ident_one')
    assert manage.is_storage_using('test_srm_ident_two')
    assert manage.is_storage_using('test_srm_ident_three')
    assert manage.is_storage_using('test_srm_ident_four')
    assert manage.is_storage_using('test_srm_ident_five')
    assert manage.is_storage_using('test_srm_ident_six')

    assert not manage.is_storage_writing('test_srm_ident_image_path_one')
    assert not manage.is_storage_writing('test_srm_ident_image_path_two')
    assert manage.is_storage_writing('test_srm_ident_image_path_three')
    assert manage.is_storage_writing('test_srm_ident_image_path_four')

    is_storage_writing_cache_clear.reset_mock()

    @patch.object(target=srm.StorageReferenceManager.is_storage_using,
                  attribute='cache_clear', new=is_storage_using_cache_clear, )
    @patch.object(target=srm.StorageReferenceManager.is_storage_writing,
                  attribute='cache_clear', new=is_storage_writing_cache_clear, )
    def _remove_writing():
        for _k in writing.keys():
            manage.remove_writing_record(_k)

    _remove_writing()

    assert is_storage_writing_cache_clear.call_count == len(writing)

    is_storage_using_cache_clear.reset_mock()
    is_storage_writing_cache_clear.reset_mock()

    @patch.object(target=srm.StorageReferenceManager.is_storage_using,
                  attribute='cache_clear', new=is_storage_using_cache_clear, )
    @patch.object(target=srm.StorageReferenceManager.is_storage_writing,
                  attribute='cache_clear', new=is_storage_writing_cache_clear, )
    def _remove_not_exist():
        for _k in reading.keys():
            manage.remove_reading_record(_k)
        for _k in writing.keys():
            manage.remove_writing_record(_k)

    _remove_not_exist()

    assert is_storage_using_cache_clear.call_count == 0
    assert is_storage_writing_cache_clear.call_count == 0


def test_add_repeat_writing():
    srm._storage_reference_manager = None
    manage = srm.StorageReferenceManager.get_storage_reference_manager()

    writing_one = {
        'caller_same_one':
            {'disk_snapshot_storage_ident': 'test_srm_ident_same_one', 'image_path': 'test_srm_ident_image_path_same'},
    }

    writing_two = {
        'caller_same_two':
            {'disk_snapshot_storage_ident': 'test_srm_ident_same_two', 'image_path': 'test_srm_ident_image_path_same'},
    }

    for _k, _v in writing_one.items():
        manage.add_writing_record(_k, _v)

    with pytest.raises(xdata.StorageReferenceRepeated):
        for _k, _v in writing_two.items():
            manage.add_writing_record(_k, _v)

    for _k in writing_one.keys():
        manage.remove_writing_record(_k)
