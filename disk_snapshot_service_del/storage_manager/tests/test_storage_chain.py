import pytest
from storage_manager import storage_chain as sc
from storage_manager import storage_reference_manager as srm
from storage_manager import storage_tree as tree
from storage_manager import models as m

pytestmark = pytest.mark.django_db


def _test_box(storage_chain_class, root_uuid, storage_id, timestamp, caller_name, assert_result_storage_id_list,
              set_storage_status_recycled=False):
    storage_root_obj = m.DiskSnapshotStorageRoot.objects.get(root_uuid=root_uuid)
    storage_obj = m.DiskSnapshotStorage.objects.get(id=storage_id)
    storage_tree = tree.DiskSnapshotStorageTree.create_instance_by_storage_root(storage_root_obj=storage_root_obj)
    storage_chain = storage_chain_class(
        timestamp=timestamp,
        storage_reference_manager=srm.StorageReferenceManager(),
        caller_name=caller_name
    )
    for _node in tree.dfs_to_root(storage_tree.get_node_by_storage_obj(storage_obj)):
        if set_storage_status_recycled:
            _node.storage_obj.storage_status = m.DiskSnapshotStorage.RECYCLED
        storage_chain.insert_head(_node.storage_obj)

    storage_id_list = list()
    storage_dict_list = storage_chain.acquire().key_storage_items
    for storage_dict in storage_dict_list:
        storage_id_list.append(storage_dict['id'])
    assert storage_id_list == assert_result_storage_id_list

    storage_chain.release()


def test_storage_chain_for_read_97():
    _test_box(
        storage_chain_class=sc.StorageChainForRead,
        root_uuid='68dbc38eae7d4fddab06be14d907ae96',
        storage_id=97,
        timestamp='',
        caller_name='r',
        assert_result_storage_id_list=[91, 92, 96, 97],  # 被读取的关键节点

    )


def test_storage_chain_for_read_95():
    _test_box(
        storage_chain_class=sc.StorageChainForRead,
        root_uuid='68dbc38eae7d4fddab06be14d907ae96',
        storage_id=95,
        timestamp='',
        caller_name='r',
        assert_result_storage_id_list=[91, 92, 95],  # 被读取的关键节点

    )


def test_storage_chain_for_read_exception():
    with pytest.raises(Exception):
        _test_box(
            storage_chain_class=sc.StorageChainForRead,
            root_uuid='68dbc38eae7d4fddab06be14d907ae96',
            storage_id=97,
            timestamp='',
            caller_name='r',
            assert_result_storage_id_list=[91, 92, 96, 97],  # 被读取的关键节点
            set_storage_status_recycled=True
        )


def test_storage_chain_for_read_and_write_92():
    _test_box(
        storage_chain_class=sc.StorageChainForRW,
        root_uuid='68dbc38eae7d4fddab06be14d907ae96',
        storage_id=92,
        timestamp='',
        caller_name='rw',
        assert_result_storage_id_list=[91, 92]  # storage 91 被读取,storage 92 被写入
    )


def test_storage_chain_for_read_and_write_98():
    _test_box(
        storage_chain_class=sc.StorageChainForRW,
        root_uuid='68dbc38eae7d4fddab06be14d907ae96',
        storage_id=98,
        timestamp='',
        caller_name='rw',
        assert_result_storage_id_list=[91, 92, 96, 97, 98]  # 被读写的关键节点
    )


def test_storage_chain_for_read_and_write_exception():
    with pytest.raises(Exception):
        _test_box(
            storage_chain_class=sc.StorageChainForRW,
            root_uuid='68dbc38eae7d4fddab06be14d907ae96',
            storage_id=92,
            timestamp='',
            caller_name='rw',
            assert_result_storage_id_list=[91, 92],  # storage 91 被读取,storage 92 被写入
            set_storage_status_recycled=True
        )


def test_storage_chain_for_write_92():
    _test_box(
        storage_chain_class=sc.StorageChainForWrite,
        root_uuid='68dbc38eae7d4fddab06be14d907ae96',
        storage_id=92,
        timestamp='',
        caller_name='w',
        assert_result_storage_id_list=[92]  # storage 92 被写入
    )


def test_storage_chain_for_write_98():
    _test_box(
        storage_chain_class=sc.StorageChainForWrite,
        root_uuid='68dbc38eae7d4fddab06be14d907ae96',
        storage_id=98,
        timestamp='',
        caller_name='w',
        assert_result_storage_id_list=[98]  # storage 92 被写入
    )


def test_storage_chain_for_write_exception():
    with pytest.raises(Exception):
        _test_box(
            storage_chain_class=sc.StorageChainForWrite,
            root_uuid='68dbc38eae7d4fddab06be14d907ae96',
            storage_id=92,
            timestamp='',
            caller_name='w',
            assert_result_storage_id_list=[92],  # storage 92 被写入
            set_storage_status_recycled=True
        )
