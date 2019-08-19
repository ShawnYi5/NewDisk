import decimal
import pytest
from basic_library import xdata
from storage_manager import storage_query as sq
from storage_manager import storage_locker_manager as slm
from storage_manager import storage_reference_manager as srm
from storage_manager import storage_chain as sc
from storage_manager import storage_tree as tree
from storage_manager import models as m

pytestmark = pytest.mark.django_db


def _get_chain(host_snapshot_ident, timestamp):
    storage_chain_query_by_host_snapshot_obj = sq.StorageChainQueryByHostSnapshot(
        host_snapshot_ident=host_snapshot_ident, timestamp=timestamp,
        storage_reference_manager=srm.StorageReferenceManager(),
        storage_locker_manager=slm.StorageLockerManager())
    return storage_chain_query_by_host_snapshot_obj


def _test_box(host_snapshot_ident, timestamp, assert_result):
    get_chain = _get_chain(host_snapshot_ident, timestamp)
    chain_list = get_chain.get_storage_chain_list()
    chain_list_cache = get_chain.get_storage_chain_list()

    assert id(chain_list) == id(chain_list_cache)

    for chain in chain_list:
        _get_disk_index_id = chain['disk_index']
        chain_result = assert_result.pop(_get_disk_index_id)
        assert chain['storage_chain'].timestamp == chain_result['timestamp']
        _get_storage_id_list = list()
        for storage in chain['storage_chain'].storage_items:
            _get_storage_id_list.append(storage['id'])
        assert _get_storage_id_list == chain_result['storage_id_list']

    assert not assert_result


def test_normal_host_snapshot_ident_1():
    """ root_uuid = f1089e3def274194a1ac622e252d590f """
    _test_box("host_snapshot_ident_1", None,  # normal, timestamp 默认为 2005-03-18 09:58:31.111110
              {
                  1: {
                      'storage_id_list': [7],  # leaf storage is qcow
                      'timestamp': decimal.Decimal('1111111111.11111'),  # 2005-03-18 09:58:31.111110
                  },
              }
              )


def test_cdp_host_snapshot_ident_3_a():
    """root_uuid = 579734322ea14ff3a9dfcf6df9c4716c
        测试：timestamp 超出了 host_snapshot 的时间范围(小于host_snapshot 3 的开始时间)
    """
    with pytest.raises(xdata.HostSnapshotInvalid):
        _test_box("host_snapshot_ident_3", decimal.Decimal('1111111300.000000'),  # cdp, 2005-03-18 10:01:40.000000
                  {
                      1: {
                          'storage_id_list': [7, 8],
                          'timestamp': decimal.Decimal('1111111300.000000'),  # 2005-03-18 10:01:40.000000
                      },
                  }
                  )


def test_cdp_host_snapshot_ident_3_b():
    """root_uuid = 579734322ea14ff3a9dfcf6df9c4716c
         测试：timestamp 超出了 host_snapshot 的时间范围(大于host_snapshot 3 的结束时间)
    """
    with pytest.raises(xdata.HostSnapshotInvalid):
        _test_box("host_snapshot_ident_3", decimal.Decimal('1111111800.000000'),  # cdp, 2005-03-18 10:10:00.000000
                  {
                      1: {
                          'storage_id_list': [7, 8, 9, 10, 11, 12],
                          'timestamp': decimal.Decimal('1111111800.000000'),  # 2005-03-18 10:10:00.000000
                      },
                  }
                  )


def test_cdp_host_snapshot_ident_3_c():
    """root_uuid = 579734322ea14ff3a9dfcf6df9c4716c
        测试：timestamp 在 storage 11 与 12 的缝隙间
    """
    _test_box("host_snapshot_ident_3", decimal.Decimal('1111111640.000000'),  # cdp, 2005-03-18 10:07:20.000000
              {
                  1: {
                      'storage_id_list': [7, 8, 9, 10, 11],  # leaf storage is cdp
                      'timestamp': decimal.Decimal('1111111640.000000'),  # 2005-03-18 10:07:20.000000
                  },
              }
              )


def test_cdp_host_snapshot_ident_3_d():
    """root_uuid = 579734322ea14ff3a9dfcf6df9c4716c
        测试：参数timestamp=None 时候，默认使用 host_snapshot 3 的 end_time:2005-03-18 10:08:30.000000
    """
    _test_box("host_snapshot_ident_3", None,  # cdp, 2005-03-18 10:08:30.0
              {
                  1: {
                      'storage_id_list': [7, 8, 9, 10, 11, 12],  # leaf storage is cdp
                      'timestamp': decimal.Decimal('1111111710.000000'),  # 2005-03-18 10:08:30.0
                  },
              }
              )


def test_cdp_host_snapshot_ident_3_e():
    """root_uuid = 579734322ea14ff3a9dfcf6df9c4716c"""
    _test_box("host_snapshot_ident_3", decimal.Decimal('1111111700.000000'),  # cdp, 2005-03-18 10:08:20.000000
              {
                  1: {
                      'storage_id_list': [7, 8, 9, 10, 11, 12],  # leaf storage is cdp
                      'timestamp': decimal.Decimal('1111111700.000000'),  # 2005-03-18 10:08:20.000000
                  },
              }
              )


def test_normal_host_snapshot_ident_73():
    """root_uuid = 684cf170342f4504bb14c6e30f70646c
        测试：获取到的storage in STATUS_NOT_READABLE
    """
    with pytest.raises(xdata.DiskSnapshotStorageInvalid):
        _test_box("host_snapshot_ident_73", None,  # normal, timestamp 默认为 2005-03-18 09:58:31.0
                  {
                      1: {
                          'storage_id_list': [73],
                          'timestamp': decimal.Decimal('1111111111.000000'),  # 2005-03-18 09:58:31.111110
                      },
                  }
                  )


def test_normal_host_snapshot_ident_74():
    """root_uuid = e7514d9f78f34a95b98c0d86346e01a9
        测试：normal host_snapshot 74 invalid
    """
    with pytest.raises(xdata.HostSnapshotInvalid):
        _test_box("host_snapshot_ident_74", None,  # normal
                  {
                      1: {
                          'storage_id_list': [87],
                          'timestamp': decimal.Decimal('1111111111.000000'),
                      },
                  }
                  )


def test_cdp_host_snapshot_ident_75():
    """root_uuid = e7514d9f78f34a95b98c0d86346e01a9
        测试：normal host_snapshot 75 invalid
    """
    with pytest.raises(xdata.HostSnapshotInvalid):
        _test_box("host_snapshot_ident_75", None,  # cdp
                  {
                      1: {
                          'storage_id_list': [87, 88],
                          'timestamp': decimal.Decimal('1111111112.000000'),
                      },
                  }
                  )


def test_normal_host_snapshot_ident_76():
    """root_uuid = 74d1e80dc44445479deffd377941a1ee
        测试：normal host_snapshot 76 的 timestamp 与 storage 的begin_timestamp 不一致
    """
    with pytest.raises(xdata.DiskSnapshotStorageInvalid):
        _test_box("host_snapshot_ident_76", None,  # normal, timestamp 默认为 2005-03-18 09:58:33.000000
                  {
                      1: {
                          'storage_id_list': [89],
                          'timestamp': decimal.Decimal('1111111114.000000'),  # 2005-03-18 09:58:34.000000
                      },
                  }
                  )


def test_():
    """通过DiskSnapshotStorage查询StorageChain时，storage 已被回收，应该抛异常"""
    root_uuid = '74d1e80dc44445479deffd377941a1ee'
    storage_root_obj = m.DiskSnapshotStorageRoot.objects.get(root_uuid=root_uuid)
    storage_chain_class = sc.StorageChainForRead
    storage_tree = tree.DiskSnapshotStorageTree.create_instance_by_storage_root(storage_root_obj)
    storage_obj = m.DiskSnapshotStorage.objects.get(id=89)
    storage_chain_query_by_disk_snapshot_storage_obj = sq.StorageChainQueryByDiskSnapshotStorage(
        storage_chain_class=storage_chain_class,
        storage_tree=storage_tree,
        storage_reference_manager=srm.StorageReferenceManager(),
        storage_obj=storage_obj,
        caller_name='test'
    )

    with pytest.raises(xdata.DiskSnapshotStorageInvalid):
        storage_obj.storage_status = m.DiskSnapshotStorage.RECYCLED
        storage_obj.save(update_fields=('storage_status',))
        storage_chain_query_by_disk_snapshot_storage_obj.get_storage_chain()
