import uuid
from unittest.mock import MagicMock, patch

import pytest

from storage_manager import models as m
from storage_manager import storage_action as action
from storage_manager import storage_collection as sc
from storage_manager import storage_locker_manager as slm
from storage_manager import storage_reference_manager as srm

pytestmark = pytest.mark.django_db

mock_setting = {
    'action_remove_cdp_file': {
        'target': action,
        'attribute': 'remove_cdp_file',
        'new': MagicMock(),
    },
    'action_remove_qcow_file': {
        'target': action,
        'attribute': 'remove_qcow_file',
        'new': MagicMock(),
    },
    'action_delete_qcow_snapshot': {
        'target': action,
        'attribute': 'delete_qcow_snapshot',
        'new': MagicMock(),
    },
    'action_merge_cdp_to_qcow': {
        'target': action,
        'attribute': 'merge_cdp_to_qcow',
        'new': MagicMock(),
    },
    'action_merge_qcow_snapshot_type_a': {
        'target': action,
        'attribute': 'merge_qcow_snapshot_type_a',
        'new': MagicMock(),
    },
    'action_merge_qcow_snapshot_type_b': {
        'target': action,
        'attribute': 'merge_qcow_snapshot_type_b',
        'new': MagicMock(),
    },
    'srm_is_storage_using': {
        'target': srm.StorageReferenceManager,
        'attribute': 'is_storage_using',
        'new': MagicMock(return_value=False),
    },
    'srm_is_storage_writing': {
        'target': srm.StorageReferenceManager,
        'attribute': 'is_storage_writing',
        'new': MagicMock(return_value=False),
    },
}


def get_collection(root_uuid):
    storage_root_obj = m.DiskSnapshotStorageRoot.objects.get(root_uuid=root_uuid)
    collection = sc.StorageCollection(storage_root_obj, srm.StorageReferenceManager(),
                                      slm.StorageLockerManager())

    return storage_root_obj, collection


def run_collect(collection, _mock_setting=None):
    _ = str(collection)

    if not _mock_setting:
        _mock_setting = mock_setting

    for _s in _mock_setting.values():
        _s['new'].reset_mock()

    @patch.object(**_mock_setting['action_remove_cdp_file'])
    @patch.object(**_mock_setting['action_remove_qcow_file'])
    @patch.object(**_mock_setting['action_delete_qcow_snapshot'])
    @patch.object(**_mock_setting['action_merge_cdp_to_qcow'])
    @patch.object(**_mock_setting['action_merge_qcow_snapshot_type_a'])
    @patch.object(**_mock_setting['action_merge_qcow_snapshot_type_b'])
    @patch.object(**_mock_setting['srm_is_storage_using'])
    @patch.object(**_mock_setting['srm_is_storage_writing'])
    def _run_collect():
        return collection.collect()

    r = _run_collect()
    return r, _mock_setting


def _get_mock_setting(setting):
    result = mock_setting.copy()
    result.update(setting)
    return result


def _assert_call_count(s, action_remove_cdp_file, action_remove_qcow_file, action_delete_qcow_snapshot,
                       action_merge_cdp_to_qcow, action_merge_qcow_snapshot_type_a, action_merge_qcow_snapshot_type_b):
    assert s['action_remove_cdp_file']['new'].call_count == action_remove_cdp_file
    assert s['action_remove_qcow_file']['new'].call_count == action_remove_qcow_file
    assert s['action_delete_qcow_snapshot']['new'].call_count == action_delete_qcow_snapshot
    assert s['action_merge_cdp_to_qcow']['new'].call_count == action_merge_cdp_to_qcow
    assert s['action_merge_qcow_snapshot_type_a']['new'].call_count == action_merge_qcow_snapshot_type_a
    assert s['action_merge_qcow_snapshot_type_b']['new'].call_count == action_merge_qcow_snapshot_type_b


def _assert_do_nothing(r, s):
    assert not r
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)


def test_00001_root_set_invalid():
    """测试00001_root中的storage全部被回收"""

    root_uuid = m.DiskSnapshotStorageRoot.RECYCLE_ROOT_UUID  # root_id = 2

    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    """
    本次回收后，所有 storage 的 status 都为 recycled
    本次回收有实际变更 storage
    注意回收方式与 e89fbabc-e5da-47b2-8ba1-01752d0b5ab0 有关
    """
    r, s = run_collect(collection)
    assert (m.DiskSnapshotStorage.objects
            .filter(storage_root=storage_root_obj)
            .exclude(storage_status=m.DiskSnapshotStorage.RECYCLED)
            .count() == 0
            )
    assert r
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=2,
                       action_delete_qcow_snapshot=1, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)


def test_2715614d48f04726bb81cc188e1b4850_root_without_storage():
    """测试空 root(没有storage)"""

    root_uuid = '2715614d48f04726bb81cc188e1b4850'  # root_id = 1

    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    """经过回收后，预期  root 被置为 invalid"""
    r, s = run_collect(collection)

    _assert_do_nothing(r, s)

    storage_root_obj.refresh_from_db()
    assert not storage_root_obj.root_valid


def test_56db24134ebc45ce86ead75c9fe53d95_empty_tree():
    """测试 root 有 storage 但是没有形成 tree"""

    root_uuid = '56db24134ebc45ce86ead75c9fe53d95'  # root_id = 3
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    """经过回收后，预期 root 被置为 invalid"""
    r, s = run_collect(collection)

    _assert_do_nothing(r, s)

    storage_root_obj.refresh_from_db()
    assert not storage_root_obj.root_valid


def test_579734322ea14ff3a9dfcf6df9c4716c_all_storage_pass():
    """存储快照链都不可回收"""

    root_uuid = '579734322ea14ff3a9dfcf6df9c4716c'  # root_id = 5
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    """经过回收后，预期：1.root为valid     2.未被删除的storage数量为6"""
    r, s = run_collect(collection)

    _assert_do_nothing(r, s)

    storage_root_obj.refresh_from_db()
    assert storage_root_obj.root_valid
    assert (m.DiskSnapshotStorage.objects
            .filter(storage_root=storage_root_obj)
            .exclude(storage_status=m.DiskSnapshotStorage.RECYCLED)
            .count() == 6)


def test_579734322ea14ff3a9dfcf6df9c4716c_all_host_snapshot_invalid():
    """快照存储链全部被回收"""

    root_uuid = '579734322ea14ff3a9dfcf6df9c4716c'  # root_id = 5
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    """将 host_snapshot 1 2 3 置为 invalid"""
    m.HostSnapshot.objects.filter(id__in=[1, 2, 3]).update(host_snapshot_valid=False)

    """经过回收后，预期：被删除的storage数量为6"""
    r, s = run_collect(collection)
    assert r
    _assert_call_count(s, action_remove_cdp_file=4, action_remove_qcow_file=2,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)
    assert storage_root_obj.root_valid
    storage_root_obj.refresh_from_db()
    assert (m.DiskSnapshotStorage.objects
            .filter(storage_root=storage_root_obj)
            .filter(storage_status=m.DiskSnapshotStorage.RECYCLED)
            .count() == 6)


def test_579734322ea14ff3a9dfcf6df9c4716c_host_snapshot_3_invalid():
    """回收快照存储链尾部"""

    root_uuid = '579734322ea14ff3a9dfcf6df9c4716c'  # root_id = 5
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    """将 host_snapshot 3 置为 invalid"""
    m.HostSnapshot.objects.filter(id=3).update(host_snapshot_valid=False)

    """经过回收后，预期：1.host_snapshot 3被置为 invalid    2.storage 9 10 11 12 被删除"""
    r, s = run_collect(collection)
    assert r
    _assert_call_count(s, action_remove_cdp_file=3, action_remove_qcow_file=1,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)

    storage_root_obj.refresh_from_db()
    assert not m.HostSnapshot.objects.get(id=3).host_snapshot_valid
    assert (m.DiskSnapshotStorage.objects
            .filter(storage_root=storage_root_obj)
            .filter(id__in=[9, 10, 11, 12])
            .exclude(storage_status=m.DiskSnapshotStorage.RECYCLED)
            .count() == 0)


def test_579734322ea14ff3a9dfcf6df9c4716c_spec_one():
    """回收快照存储链头部"""

    loop_one_new_storage_ident = uuid.uuid4().hex

    new_setting = {
        'srm_is_storage_writing': {
            'target': srm.StorageReferenceManager,
            'attribute': 'is_storage_writing',
            'new': MagicMock(return_value=True),
        },
    }

    root_uuid = r'579734322ea14ff3a9dfcf6df9c4716c'  # root_id = 5
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    """将 host_snapshot 1 2 置为invalid"""
    m.HostSnapshot.objects.filter(id__in=[1, 2]).update(host_snapshot_valid=False)

    """因为可回收的cdp storage的父正在写入，所以不进行回收"""
    r, s = run_collect(collection, _get_mock_setting(new_setting))
    _assert_do_nothing(r, s)

    @patch.object(sc.MergeCdpWork, '_generate_storage_ident', MagicMock(return_value=loop_one_new_storage_ident))
    def _loop_one():
        return run_collect(collection)

    """第一轮回收后，预期 storage 8 被合并"""
    r, s = _loop_one()
    assert r
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=1,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)

    assert m.DiskSnapshotStorage.objects.get(id=8).storage_status == m.DiskSnapshotStorage.RECYCLING
    assert (m.DiskSnapshotStorage.objects
            .filter(storage_root=storage_root_obj)
            .exclude(id=8)
            .exclude(storage_status=m.DiskSnapshotStorage.STORAGE)
            .count() == 0)

    assert (m.DiskSnapshotStorage.objects
            .filter(storage_root=storage_root_obj)
            .get(id=9).parent_snapshot
            .disk_snapshot_storage_ident == loop_one_new_storage_ident)

    """第二轮回收后，预期 storage 8 被删除"""
    r, s = run_collect(collection)
    assert r
    _assert_call_count(s, action_remove_cdp_file=1, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)

    storage_root_obj.refresh_from_db()

    assert m.DiskSnapshotStorage.objects.get(id=8).storage_status == m.DiskSnapshotStorage.RECYCLED
    assert (m.DiskSnapshotStorage.objects
            .filter(storage_root=storage_root_obj)
            .exclude(id=8)
            .exclude(storage_status=m.DiskSnapshotStorage.STORAGE)
            .count() == 0
            )

    assert (m.DiskSnapshotStorage.objects
            .filter(storage_root=storage_root_obj)
            .get(id=9)
            .parent_snapshot.disk_snapshot_storage_ident == loop_one_new_storage_ident)

    """第三轮回收后，预期：
        1.storage 7 被移动到 000...001 的 root 下
        2.storage loop_one_new_storage_ident 会成为新的根
    """
    r, s = run_collect(collection)
    assert r
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=1, action_merge_qcow_snapshot_type_b=0)
    storage_root_obj.refresh_from_db()
    assert m.DiskSnapshotStorage.objects.get(id=7).storage_root.is_recycle_root
    assert not m.DiskSnapshotStorage.objects.get(disk_snapshot_storage_ident=loop_one_new_storage_ident).parent_snapshot

    """第四轮无可回收"""
    r, s = run_collect(collection)
    _assert_do_nothing(r, s)


def test_88bfefb1fec04f7f927ee2dfd4750c4a_root_bifurcate():
    """回收快照存储链根分叉情况"""

    loop_one_new_storage_ident = uuid.uuid4().hex

    root_uuid = '88bfefb1fec04f7f927ee2dfd4750c4a'  # root_id = 6
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    """将 host_snapshot 4 置为invalid"""
    m.HostSnapshot.objects.filter(id__in=[4]).update(host_snapshot_valid=False)

    @patch.object(sc.MergeCdpWork, '_generate_storage_ident', MagicMock(return_value=loop_one_new_storage_ident))
    def _loop_one():
        return run_collect(collection)

    """第一轮回收，预期：1.storage 15 不能被删除，status == STORAGE"""
    r, s = _loop_one()
    _assert_do_nothing(r, s)
    assert m.DiskSnapshotStorage.objects.get(id=15).storage_status == m.DiskSnapshotStorage.STORAGE


def test_qcow_storage_file_level_deduplication():
    """测试: 带有文件级去重qcow storage不能被回收"""

    loop_one_new_storage_ident = uuid.uuid4().hex

    root_uuid = '0cde730387d94807ad9bb6b6e891240b'  # root_id = 8
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    """将 host_snapshot 8 设为 invalid"""
    m.HostSnapshot.objects.filter(id=8).update(host_snapshot_valid=False)

    @patch.object(sc.MergeCdpWork, '_generate_storage_ident', MagicMock(return_value=loop_one_new_storage_ident))
    def _loop_one():
        return run_collect(collection)

    """经过回收，预期：
        1.storage 22 带有文件级去重
        2.storage 22 状态不变
    """
    r, s = _loop_one()
    _assert_do_nothing(r, s)
    assert m.DiskSnapshotStorage.objects.get(id=22).file_level_deduplication
    assert m.DiskSnapshotStorage.objects.get(id=22).storage_status == m.DiskSnapshotStorage.STORAGE


def test_6ee815bd132e4b52b75b3f94e4876487_qcow_storage():
    """回收qcow快照存储"""
    loop_one_new_storage_ident = uuid.uuid4().hex

    root_uuid = '6ee815bd132e4b52b75b3f94e4876487'  # root_id = 9, hash_type=ROOT_HASH_TYPE_MD4_CRC32
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    m.HostSnapshot.objects.filter(id__in=[10, 11]).update(host_snapshot_valid=False)

    @patch.object(sc.MergeCdpWork, '_generate_storage_ident', MagicMock(return_value=loop_one_new_storage_ident))
    def _loop_one():
        return run_collect(collection)

    """经过第一轮回收，预期：storage 25 被合并，标记 RECYCLING"""
    r, s = _loop_one()
    assert r
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=1)
    assert m.DiskSnapshotStorage.objects.get(id=25).storage_status == m.DiskSnapshotStorage.RECYCLING

    """经过第二轮回收，预期：storage 25 被删除，标记 RECYCLED"""
    r, s = run_collect(collection)
    assert r
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=1,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)
    assert m.DiskSnapshotStorage.objects.get(id=25).storage_status == m.DiskSnapshotStorage.RECYCLED

    """经过两轮回收，因为根storage24 与其子都为qcow且不在同一个image_path，预期：根 storage 24不能被回收，状态保持 STORAGE"""
    assert m.DiskSnapshotStorage.objects.get(id=24).storage_status == m.DiskSnapshotStorage.STORAGE


def test_0526f1a2ec5e492bb9cf24266b005d95_cdp_storage():
    """测试qcow storage28 正在写入时候，不能被合并"""

    """修改cdp storage28 父快照的文件写入状态为 True"""
    _is_storage_writing_setting = {
        'srm_is_storage_writing': {
            'target': srm.StorageReferenceManager,
            'attribute': 'is_storage_writing',
            'new': MagicMock(return_value=True),
        }
    }

    loop_one_new_storage_ident = uuid.uuid4().hex

    root_uuid = '0526f1a2ec5e492bb9cf24266b005d95'  # root_id = 10
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    m.HostSnapshot.objects.filter(id__in=[13, 14]).update(host_snapshot_valid=False)

    @patch.object(sc.MergeCdpWork, '_generate_storage_ident', MagicMock(return_value=loop_one_new_storage_ident))
    def _loop_one():
        return run_collect(collection, _mock_setting=_get_mock_setting(_is_storage_writing_setting))

    """执行一轮回收，预期：storage 28 状态保持不变，为 STORAGE"""
    r, s = _loop_one()
    _assert_do_nothing(r, s)
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)
    assert m.DiskSnapshotStorage.objects.get(id=28).storage_status == m.DiskSnapshotStorage.STORAGE


def test_c503cbd3bb9e49789a8eecd82864e1a6_qcow_storage():
    """测试qcow storage 31 与父 storage 30虚拟磁盘大小不一致，不能被回收"""

    loop_one_new_storage_ident = uuid.uuid4().hex

    root_uuid = 'c503cbd3bb9e49789a8eecd82864e1a6'  # root_id = 11
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    @patch.object(sc.MergeCdpWork, '_generate_storage_ident', MagicMock(return_value=loop_one_new_storage_ident))
    def _loop_one():
        return run_collect(collection)

    m.HostSnapshot.objects.filter(id__in=[16, 17]).update(host_snapshot_valid=False)

    """经过回收后，预言：
        1.storage 30 与 storage 31 磁盘大小不相等
        1.storage 31的状态不变
    """
    r, s = _loop_one()
    _assert_do_nothing(r, s)
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)
    assert m.DiskSnapshotStorage.objects.get(id=30).disk_bytes != m.DiskSnapshotStorage.objects.get(id=31).disk_bytes
    assert m.DiskSnapshotStorage.objects.get(id=31).storage_status == m.DiskSnapshotStorage.STORAGE


def test_c559281007f64828bef19ddcf7c1c065_qcow_storage():
    """测试：
        1.带有文件级去重 cdp storage 不能被回收
        2.父为 cdp storage 的 qcow storage 不能被回收
    """

    loop_one_new_storage_ident = uuid.uuid4().hex

    root_uuid = 'c559281007f64828bef19ddcf7c1c065'  # root_id = 12
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    @patch.object(sc.MergeCdpWork, '_generate_storage_ident', MagicMock(return_value=loop_one_new_storage_ident))
    def _loop_one():
        return run_collect(collection)

    m.HostSnapshot.objects.filter(id__in=[19, 20]).update(host_snapshot_valid=False)

    """经过回收后，预言：
        1.cdp storage 34 带有文件级去重
        2.cdp storage 34 的 status 为 STORAGE
        3.qcow storage 35 的 status 为 STORAGE
    """
    r, s = _loop_one()
    _assert_do_nothing(r, s)
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)
    assert m.DiskSnapshotStorage.objects.get(id=34).file_level_deduplication
    assert m.DiskSnapshotStorage.objects.get(id=34).storage_status == m.DiskSnapshotStorage.STORAGE
    assert m.DiskSnapshotStorage.objects.get(id=35).storage_status == m.DiskSnapshotStorage.STORAGE


def test_2f0c53c19747415cbd00faf5e0d2f2f7_qcow_storage():
    """测试：
        1.状态为 data_writing 的 qcow storage 不能被回收
        2.父快照处于改写中的状态的 qcow storage 不能被回收
    """

    loop_one_new_storage_ident = uuid.uuid4().hex

    root_uuid = '2f0c53c19747415cbd00faf5e0d2f2f7'  # root_id = 13
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    @patch.object(sc.MergeCdpWork, '_generate_storage_ident', MagicMock(return_value=loop_one_new_storage_ident))
    def _loop_one():
        return run_collect(collection)

    m.HostSnapshot.objects.filter(id__in=[22, 23]).update(host_snapshot_valid=False)

    """经过回收后，预言：
        1.storage 37的状态为 DATA_WRITING
        2.storage 38的状态为 STORAGE
    """
    r, s = _loop_one()
    _assert_do_nothing(r, s)
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)
    assert m.DiskSnapshotStorage.objects.get(id=37).storage_status == m.DiskSnapshotStorage.DATA_WRITING
    assert m.DiskSnapshotStorage.objects.get(id=38).storage_status == m.DiskSnapshotStorage.STORAGE


def test_811b326dc0df450fa1fd80a671250bca_qcow_storage():
    """测试：在相同 iamge_path 的 qcow storage 不能被回收"""

    loop_one_new_storage_ident = uuid.uuid4().hex

    root_uuid = '811b326dc0df450fa1fd80a671250bca'  # root_id = 14
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    @patch.object(sc.MergeCdpWork, '_generate_storage_ident', MagicMock(return_value=loop_one_new_storage_ident))
    def _loop_one():
        return run_collect(collection)

    m.HostSnapshot.objects.filter(id__in=[26, 27]).update(host_snapshot_valid=False)

    """经过回收，预言：
        1.storage 41与父storage 40同在一个image_path
        2.storage 42与子storage 43同在一个image_path
        3.storage 42与子storage 44不在同一个image_path
        3.storage 41不会被回收，状态值为 STORAGE
        4.storage 42不会被回收，状态值为 STORAGE
    """
    r, s = _loop_one()
    _assert_do_nothing(r, s)
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)
    assert m.DiskSnapshotStorage.objects.get(id=41).image_path == m.DiskSnapshotStorage.objects.get(id=40).image_path
    assert m.DiskSnapshotStorage.objects.get(id=42).image_path == m.DiskSnapshotStorage.objects.get(id=43).image_path
    assert m.DiskSnapshotStorage.objects.get(id=42).image_path != m.DiskSnapshotStorage.objects.get(id=44).image_path
    assert m.DiskSnapshotStorage.objects.get(id=41).storage_status == m.DiskSnapshotStorage.STORAGE
    assert m.DiskSnapshotStorage.objects.get(id=42).storage_status == m.DiskSnapshotStorage.STORAGE


def test_780784b8354247b4995a894b18f19ad4_cdp_storage():
    """测试：父快照存储正在生成中的cdp storage 不被回收"""

    loop_one_new_storage_ident = uuid.uuid4().hex

    root_uuid = '780784b8354247b4995a894b18f19ad4'  # root_id = 15
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    @patch.object(sc.MergeCdpWork, '_generate_storage_ident', MagicMock(return_value=loop_one_new_storage_ident))
    def _loop_one():
        return run_collect(collection)

    m.HostSnapshot.objects.filter(id__in=[30, 31]).update(host_snapshot_valid=False)

    """经过回收，预言：
        1.storage 44的状态为 HASHING
        2.storage 45的状态为 STORAGE
    """
    r, s = _loop_one()
    _assert_do_nothing(r, s)
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)
    assert m.DiskSnapshotStorage.objects.get(id=45).storage_status == m.DiskSnapshotStorage.HASHING
    assert m.DiskSnapshotStorage.objects.get(id=46).storage_status == m.DiskSnapshotStorage.STORAGE


def test_2088369f992f4fd9b42626f8679c8d85_cdp_storage():
    """测试：中间有依赖的情况的cdp storage 不被回收"""

    loop_one_new_storage_ident = uuid.uuid4().hex

    root_uuid = '2088369f992f4fd9b42626f8679c8d85'  # root_id = 16
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    @patch.object(sc.MergeCdpWork, '_generate_storage_ident', MagicMock(return_value=loop_one_new_storage_ident))
    def _loop_one():
        return run_collect(collection)

    m.HostSnapshot.objects.filter(id__in=[34]).update(host_snapshot_valid=False)

    """经过回收，预言：
        1.storage 49的 parent_timestamp 不为空
        2.storage 49的状态为 STORAGE
    """
    r, s = _loop_one()
    _assert_do_nothing(r, s)
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)
    assert m.DiskSnapshotStorage.objects.get(id=50).parent_timestamp
    assert m.DiskSnapshotStorage.objects.get(id=49).storage_status == m.DiskSnapshotStorage.STORAGE


def test_351e7ed91f5b4d489660c382ca5f2527_qcow_storage():
    """测试：父节点为 RECYCLING 的 storage 不可以回收"""

    loop_one_new_storage_ident = uuid.uuid4().hex

    root_uuid = '351e7ed91f5b4d489660c382ca5f2527'  # root_id = 17
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    @patch.object(sc.MergeCdpWork, '_generate_storage_ident', MagicMock(return_value=loop_one_new_storage_ident))
    def _loop_one():
        return run_collect(collection)

    m.HostSnapshot.objects.filter(id__in=[37]).update(host_snapshot_valid=False)

    """经过回收，预言：
         1.storage 51的状态为 RECYCLING
         2.storage 52的状态为 STORAGE
     """
    r, s = _loop_one()
    _assert_do_nothing(r, s)
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)
    assert m.DiskSnapshotStorage.objects.get(id=51).storage_status == m.DiskSnapshotStorage.RECYCLING
    assert m.DiskSnapshotStorage.objects.get(id=52).storage_status == m.DiskSnapshotStorage.STORAGE


def test_fc998016513245a8aeb7d300e559725f_qcow_storage():
    """测试：正在写入的 qcow storage 58，不可删除"""

    """将写入状态改为 True"""
    new_setting = {
        'srm_is_storage_writing': {
            'target': srm.StorageReferenceManager,
            'attribute': 'is_storage_writing',
            'new': MagicMock(return_value=True),
        }
    }

    loop_one_new_storage_ident = uuid.uuid4().hex

    root_uuid = 'fc998016513245a8aeb7d300e559725f'  # root_id = 19
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    @patch.object(sc.MergeCdpWork, '_generate_storage_ident', MagicMock(return_value=loop_one_new_storage_ident))
    def _loop_one():
        return run_collect(collection, _mock_setting=_get_mock_setting(new_setting))

    m.HostSnapshot.objects.filter(id__in=[43]).update(host_snapshot_valid=False)

    """经过回收，预言：storage 58状态不变，为 STORAGE"""
    r, s = _loop_one()
    _assert_do_nothing(r, s)
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)
    assert m.DiskSnapshotStorage.objects.get(id=58).storage_status == m.DiskSnapshotStorage.STORAGE


def test_539694eda9f74664961ca2ea808003de_qcow_storage():
    """测试：正在使用的 qcow storage"""

    """将使用状态改为 True"""
    new_setting = {
        'srm_is_storage_using': {
            'target': srm.StorageReferenceManager,
            'attribute': 'is_storage_using',
            'new': MagicMock(return_value=True),
        }
    }

    loop_one_new_storage_ident = uuid.uuid4().hex

    root_uuid = '539694eda9f74664961ca2ea808003de'  # root_id = 18
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    @patch.object(sc.MergeCdpWork, '_generate_storage_ident', MagicMock(return_value=loop_one_new_storage_ident))
    def _loop_one():
        return run_collect(collection, _mock_setting=_get_mock_setting(new_setting))

    m.HostSnapshot.objects.filter(id__in=[41]).update(host_snapshot_valid=False)

    """经过第一轮回收，预言：storage 56 状态被标记为回收，为 RECYCLING"""
    r, s = _loop_one()
    _assert_do_nothing(r, s)
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)
    assert m.DiskSnapshotStorage.objects.get(id=56).storage_status == m.DiskSnapshotStorage.STORAGE


def test_e2abfe46d44f4d2e91422e930101b684_qcow_storage():
    """测试：状态为非 STATUS_CAN_DELETE 的 storage 60 不可删除"""

    loop_one_new_storage_ident = uuid.uuid4().hex

    root_uuid = 'e2abfe46d44f4d2e91422e930101b684'  # root_id = 20
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    @patch.object(sc.MergeCdpWork, '_generate_storage_ident', MagicMock(return_value=loop_one_new_storage_ident))
    def _loop_one():
        return run_collect(collection)

    m.HostSnapshot.objects.filter(id__in=[45]).update(host_snapshot_valid=False)

    """经过回收，预言：storage 60 状态不在 STATUS_CAN_DELETE，为 DATA_WRITING"""
    r, s = _loop_one()
    _assert_do_nothing(r, s)
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)
    assert m.DiskSnapshotStorage.objects.get(id=60).storage_status == m.DiskSnapshotStorage.DATA_WRITING


def test_0d4a464a33924f268721d6ae42b155af_qcow_storage():
    """测试：删除没有定位器的 storage 62"""

    loop_one_new_storage_ident = uuid.uuid4().hex

    root_uuid = '0d4a464a33924f268721d6ae42b155af'  # root_id = 21
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    @patch.object(sc.MergeCdpWork, '_generate_storage_ident', MagicMock(return_value=loop_one_new_storage_ident))
    def _loop_one():
        return run_collect(collection)

    """经过回收，预言：storage 62被删除"""
    r, s = _loop_one()
    assert r
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=1,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)
    assert m.DiskSnapshotStorage.objects.get(id=62).storage_status == m.DiskSnapshotStorage.RECYCLED


def test_0d5bccfc23204df1a912910658624503_cdp_storage():
    """测试：合并 cdp storage 64 ,但是它的子 cdp storage 65 为不可合并"""

    loop_one_new_storage_ident = uuid.uuid4().hex

    root_uuid = '0d5bccfc23204df1a912910658624503'  # root_id = 22
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    @patch.object(sc.MergeCdpWork, '_generate_storage_ident', MagicMock(return_value=loop_one_new_storage_ident))
    def _loop_one():
        return run_collect(collection)

    m.HostSnapshot.objects.filter(id__in=[48, 49]).update(host_snapshot_valid=False)

    """第一轮回收，预言：
        1.storage 64被标记为回收
        2.storage 65状态为 HASHING
    """
    r, s = _loop_one()
    assert r
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=1,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)
    assert m.DiskSnapshotStorage.objects.get(id=64).storage_status == m.DiskSnapshotStorage.RECYCLING
    assert m.DiskSnapshotStorage.objects.get(id=65).storage_status == m.DiskSnapshotStorage.HASHING

    """第一轮回收，预言：
            1.storage 64被标记为删除
            2.storage 65状态为 HASHING
    """
    r, s = _loop_one()
    assert r
    _assert_call_count(s, action_remove_cdp_file=1, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)
    assert m.DiskSnapshotStorage.objects.get(id=64).storage_status == m.DiskSnapshotStorage.RECYCLED
    assert m.DiskSnapshotStorage.objects.get(id=65).storage_status == m.DiskSnapshotStorage.HASHING


def test_9064db86c2a84275b4803e1eabe9db51_cdp_storage():
    """测试：合并的 cdp storage 68，但它的父 qcow storage 67为不可合并"""

    loop_one_new_storage_ident = uuid.uuid4().hex

    root_uuid = '9064db86c2a84275b4803e1eabe9db51'  # root_id = 23
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    @patch.object(sc.MergeCdpWork, '_generate_storage_ident', MagicMock(return_value=loop_one_new_storage_ident))
    def _loop_one():
        return run_collect(collection)

    m.HostSnapshot.objects.filter(id__in=[51, 52]).update(host_snapshot_valid=False)

    r, s = _loop_one()
    _assert_do_nothing(r, s)
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)
    assert m.DiskSnapshotStorage.objects.get(id=68).storage_status == m.DiskSnapshotStorage.STORAGE


def test_00001_root_storage_is_storage_using():
    """测试回收 00001_root 时，storage._is_storage_using"""

    """将使用状态改为 True"""
    new_setting = {
        'srm_is_storage_using': {
            'target': srm.StorageReferenceManager,
            'attribute': 'is_storage_using',
            'new': MagicMock(return_value=True),
        }
    }

    root_uuid = m.DiskSnapshotStorageRoot.RECYCLE_ROOT_UUID  # root_id = 2

    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    """执行回收，没有storage被删除"""
    r, s = run_collect(collection, _mock_setting=_get_mock_setting(new_setting))
    _assert_do_nothing(r, s)
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)


def test_95faf4d7de6044239a69903946367b13_cdp_storage():
    """测试：合并 cdp storage 72，它的父也是 cdp ,这种情况就需要创建新的qcow来存放合并后的数据"""

    root_uuid = '95faf4d7de6044239a69903946367b13'  # root_id = 24
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    m.HostSnapshot.objects.filter(id__in=[56]).update(host_snapshot_valid=False)

    """回收前，预言：root 24 包含四个 storage"""
    assert len(m.DiskSnapshotStorage.objects.filter(storage_root=storage_root_obj)) == 4

    """经过第一轮回收操作，预言：
        1.storage 72 被置为 RECYCLING
        2.root 24 新创建了 qcow storage，此时包含五个 storage
    """
    r, s = run_collect(collection)
    assert r
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=1,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)
    assert m.DiskSnapshotStorage.objects.get(id=72).storage_status == m.DiskSnapshotStorage.RECYCLING
    assert len(m.DiskSnapshotStorage.objects.filter(storage_root=storage_root_obj)) == 5

    """经过第二轮回收操作，预言：
        1.storage 72 被置为 RECYCLING
    """
    r, s = run_collect(collection)
    assert r
    _assert_call_count(s, action_remove_cdp_file=1, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)
    assert m.DiskSnapshotStorage.objects.get(id=72).storage_status == m.DiskSnapshotStorage.RECYCLED


def test_194fdccb8f564271b8867eac383c75e3_cdp_storage():
    """测试：合并cdp storage 时候抛出异常"""

    root_uuid = '194fdccb8f564271b8867eac383c75e3'  # root_id = 25
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    def _loop_zero():
        new_mock = MagicMock()
        new_mock.side_effect = MagicMock(side_effect=Exception('Test'))
        new_setting = {
            'action_merge_cdp_to_qcow': {
                'target': action,
                'attribute': 'merge_cdp_to_qcow',
                'new': new_mock,
            }
        }
        return run_collect(collection, _get_mock_setting(new_setting))

    m.HostSnapshot.objects.filter(id__in=[59]).update(host_snapshot_valid=False)

    """第一轮合并，预言：
        1.action_merge_cdp_to_qcow 操作异常，
        1.storage 75被置为回收中
    """
    r, s = _loop_zero()
    assert not r
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=1,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)
    assert m.DiskSnapshotStorage.objects.get(id=75).storage_status == m.DiskSnapshotStorage.RECYCLING


def test_70ddbb96ebcd46fcaafd2db689205288_qcow_storage():
    """测试：合并type_b qcow storage时，抛出异常"""

    root_uuid = '70ddbb96ebcd46fcaafd2db689205288'  # root_id = 26
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    def _loop_zero():
        new_mock = MagicMock()
        new_mock.side_effect = MagicMock(side_effect=Exception('Test'))
        new_setting = {
            'action_merge_qcow_snapshot_type_b': {
                'target': action,
                'attribute': 'merge_qcow_snapshot_type_b',
                'new': new_mock,
            },
        }
        return run_collect(collection, _get_mock_setting(new_setting))

    m.HostSnapshot.objects.filter(id__in=[61, 62]).update(host_snapshot_valid=False)

    """经过回收，预言：
        1.回收结果返回值为 False
        2.
    """
    r, s = _loop_zero()
    assert not r
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=1)
    assert m.DiskSnapshotStorage.objects.get(id=77).storage_status == m.DiskSnapshotStorage.STORAGE
    assert m.DiskSnapshotStorage.objects.get(id=78).storage_status == m.DiskSnapshotStorage.RECYCLING


def test_875847899f4b428a8bb8094dd74960f4_qcow_storage():
    """测试：合并type_a qcow storage时，抛出异常"""

    root_uuid = '875847899f4b428a8bb8094dd74960f4'  # root_id = 27
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    def _loop_zero():
        new_mock = MagicMock()
        new_mock.side_effect = MagicMock(side_effect=Exception('Test'))
        new_setting = {
            'action_merge_qcow_snapshot_type_a': {
                'target': action,
                'attribute': 'merge_qcow_snapshot_type_a',
                'new': new_mock,
            },
        }
        return run_collect(collection, _get_mock_setting(new_setting))

    m.HostSnapshot.objects.filter(id__in=[64, 65]).update(host_snapshot_valid=False)

    """经过回收，预言：
        1.回收结果返回值为 False
        2.storage 80标记为回收中
        3.storage 81状态不变，为 STORAGE
    """
    r, s = _loop_zero()
    assert not r
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=1, action_merge_qcow_snapshot_type_b=0)
    assert m.DiskSnapshotStorage.objects.get(id=80).storage_status == m.DiskSnapshotStorage.RECYCLING
    assert m.DiskSnapshotStorage.objects.get(id=81).storage_status == m.DiskSnapshotStorage.STORAGE


def test_00001_root_delete_qcow_snapshot():
    """测试：回收00001_root中，action_delete_qcow_snapshot时，抛出异常
    """

    root_uuid = m.DiskSnapshotStorageRoot.RECYCLE_ROOT_UUID  # root_id = 2

    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    def _loop_zero():
        new_mock = MagicMock()
        new_mock.side_effect = MagicMock(side_effect=Exception('Test'))
        new_setting = {
            'action_delete_qcow_snapshot': {
                'target': action,
                'attribute': 'delete_qcow_snapshot',
                'new': new_mock,
            }
        }
        return run_collect(collection, _get_mock_setting(new_setting))

    """经过回收，预言：
        1.与 storage 3 同在一个 iamge_path 的storage 数量大于1
        2.storage 3未被删除，状态为 RECYCLING
        3.storage 1 2 4被删除，状态为 RECYCLED
        
    """

    r, s = _loop_zero()
    assert r  # action_remove_qcow_file 执行了2次
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=2,
                       action_delete_qcow_snapshot=1, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)
    assert (m.DiskSnapshotStorage.objects
            .filter(image_path=m.DiskSnapshotStorage.objects.get(id=3).image_path)
            .count() > 1)
    assert m.DiskSnapshotStorage.objects.get(id=3).storage_status == m.DiskSnapshotStorage.RECYCLING
    assert m.DiskSnapshotStorage.objects.get(id=1).storage_status == m.DiskSnapshotStorage.RECYCLED
    assert m.DiskSnapshotStorage.objects.get(id=2).storage_status == m.DiskSnapshotStorage.RECYCLED
    assert m.DiskSnapshotStorage.objects.get(id=4).storage_status == m.DiskSnapshotStorage.RECYCLED


def test_6ee815bd132e4b52b75b3f94e4876487_action_remove_qcow_file():
    """回收 action_remove_qcow_file 快照存储，抛异常"""
    loop_one_new_storage_ident = uuid.uuid4().hex

    root_uuid = '6ee815bd132e4b52b75b3f94e4876487'  # root_id = 9
    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    def _loop_zero():
        new_mock = MagicMock()
        new_mock.side_effect = MagicMock(side_effect=Exception('Test'))
        new_setting = {
            'action_remove_qcow_file': {
                'target': action,
                'attribute': 'remove_qcow_file',
                'new': new_mock,
            }
        }
        return run_collect(collection, _get_mock_setting(new_setting))

    m.HostSnapshot.objects.filter(id__in=[10, 11]).update(host_snapshot_valid=False)

    @patch.object(sc.MergeCdpWork, '_generate_storage_ident', MagicMock(return_value=loop_one_new_storage_ident))
    def _loop_one():
        return run_collect(collection)

    """经过第一轮回收，预期：storage 25 被合并，标记 RECYCLING"""
    r, s = _loop_one()
    assert r
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=1)
    assert m.DiskSnapshotStorage.objects.get(id=25).storage_status == m.DiskSnapshotStorage.RECYCLING

    """经过第二轮回收，预期：
        1.回收结果返回值为 False
        2.storage 25 未被删除，状态为 RECYCLING
    """
    r, s = _loop_zero()
    assert not r
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=1,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)
    assert m.DiskSnapshotStorage.objects.get(id=25).storage_status == m.DiskSnapshotStorage.RECYCLING


def test_00001_root_delete_action_remove_qcow_file():
    """测试：回收00001_root中，action_remove_qcow_file时，抛出异常
    """

    root_uuid = m.DiskSnapshotStorageRoot.RECYCLE_ROOT_UUID  # root_id = 2

    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    def _loop_zero():
        new_mock = MagicMock()
        new_mock.side_effect = MagicMock(side_effect=Exception('Test'))
        new_setting = {
            'action_remove_qcow_file': {
                'target': action,
                'attribute': 'remove_qcow_file',
                'new': new_mock,
            }
        }
        return run_collect(collection, _get_mock_setting(new_setting))

    r, s = _loop_zero()
    assert r
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=2,
                       action_delete_qcow_snapshot=1, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)
    assert (m.DiskSnapshotStorage.objects
            .filter(image_path=m.DiskSnapshotStorage.objects.get(id=3).image_path)
            .count() > 1)
    assert m.DiskSnapshotStorage.objects.get(id=3).storage_status == m.DiskSnapshotStorage.RECYCLED
    assert m.DiskSnapshotStorage.objects.get(id=1).storage_status == m.DiskSnapshotStorage.RECYCLING
    assert m.DiskSnapshotStorage.objects.get(id=2).storage_status == m.DiskSnapshotStorage.RECYCLING
    assert m.DiskSnapshotStorage.objects.get(id=4).storage_status == m.DiskSnapshotStorage.RECYCLING


def test_f1089e3def274194a1ac622e252d590f_qcow_storage():
    """测试：正在写入，且与子sotage同一个image_path的qcow storage，不能被回收"""

    root_uuid = "f1089e3def274194a1ac622e252d590f"  # root_id = 28

    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    new_setting = {
        'srm_is_storage_writing': {
            'target': srm.StorageReferenceManager,
            'attribute': 'is_storage_writing',
            'new': MagicMock(return_value=True),
        }
    }

    m.HostSnapshot.objects.filter(id__in=[68]).update(host_snapshot_valid=False)

    """经过回收，预言：storage 84 状态保持不变，为STORAGE"""
    r, s = run_collect(collection, _get_mock_setting(new_setting))
    _assert_do_nothing(r, s)
    assert m.DiskSnapshotStorage.objects.get(id=84).storage_status == m.DiskSnapshotStorage.STORAGE


def test_257f6dbf064749dc999b63cc1b867049_qcow_storage():
    root_uuid = "257f6dbf064749dc999b63cc1b867049"  # root_id = 34  hash_type=ROOT_HASH_TYPE_MD4_CRC32

    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    """第一轮回收，预言：
        1.storage 100 合并，status 为 RECYCLING
        2.storage 101 状态不变，为 STORAGE
    """
    r, s = run_collect(collection)
    assert r
    assert m.DiskSnapshotStorage.objects.get(id=100).storage_status == m.DiskSnapshotStorage.RECYCLING
    assert m.DiskSnapshotStorage.objects.get(id=101).storage_status == m.DiskSnapshotStorage.STORAGE
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=1)

    """第二轮回收，预言：
        1.storage 100 被删除，status 为 RECYCLED
        2.storage 101 状态不变，为 STORAGE
    """
    r, s = run_collect(collection)
    assert r
    assert m.DiskSnapshotStorage.objects.get(id=100).storage_status == m.DiskSnapshotStorage.RECYCLED
    assert m.DiskSnapshotStorage.objects.get(id=101).storage_status == m.DiskSnapshotStorage.STORAGE
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=1,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)


def test_1aefb07138b040bcad3cc809891435c4_cdp_storage():
    root_uuid = "1aefb07138b040bcad3cc809891435c4"  # root_id = 35

    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    """第一轮回收，预言：
        1.storage 102 状态不变，为 STORAGE
        2.storage 103 被合并，status 为 RECYCLING
        3.storage 104 状态不变，为 STORAGE
    """
    r, s = run_collect(collection)
    assert r
    assert m.DiskSnapshotStorage.objects.get(id=102).storage_status == m.DiskSnapshotStorage.STORAGE
    assert m.DiskSnapshotStorage.objects.get(id=103).storage_status == m.DiskSnapshotStorage.RECYCLING
    assert m.DiskSnapshotStorage.objects.get(id=104).storage_status == m.DiskSnapshotStorage.STORAGE
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=1,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)

    """第二轮回收，预言：
        1.storage 102 状态不变，为 STORAGE
        2.storage 103 被删除
        3.storage 104 状态不变，为 STORAGE
    """
    r, s = run_collect(collection)
    assert r
    assert m.DiskSnapshotStorage.objects.get(id=102).storage_status == m.DiskSnapshotStorage.STORAGE
    assert m.DiskSnapshotStorage.objects.get(id=103).storage_status == m.DiskSnapshotStorage.RECYCLED
    assert m.DiskSnapshotStorage.objects.get(id=104).storage_status == m.DiskSnapshotStorage.STORAGE
    _assert_call_count(s, action_remove_cdp_file=1, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)


def test_c4be093f98d54f91b4eee7699aa5138a_cdp_storage():
    root_uuid = "c4be093f98d54f91b4eee7699aa5138a"  # root_id = 36

    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    """经过扫描，预言：
        1.storage 106 状态不变
        2.storage 107 状态不变
    """
    r, s = run_collect(collection)
    _assert_do_nothing(r, s)
    assert m.DiskSnapshotStorage.objects.get(id=106).storage_status == m.DiskSnapshotStorage.STORAGE
    assert m.DiskSnapshotStorage.objects.get(id=107).storage_status == m.DiskSnapshotStorage.STORAGE


def test_599c274bd17d4aae82d855f28883ad74_cdp_storage():
    """测试：cdp host_snapshot 90 .begin_time > 末尾 cdp storage 110 .end_time，而此没有child，该storage 110 不能被回收"""
    root_uuid = "599c274bd17d4aae82d855f28883ad74"  # root_id = 37

    storage_root_obj, collection = get_collection(root_uuid)
    assert storage_root_obj.root_valid

    """经过第一轮扫描：预言：
        1.storage 109 被回收
        2.storage 110 未被回收，status 为 STORAGE
    """
    r, s = run_collect(collection)
    assert r
    assert m.DiskSnapshotStorage.objects.get(id=109).storage_status == m.DiskSnapshotStorage.RECYCLING
    assert m.DiskSnapshotStorage.objects.get(id=110).storage_status == m.DiskSnapshotStorage.STORAGE
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=0,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=1)

    """经过第二轮扫描，预言：
        1.storage 109 状态被删除，status 为 RECYCLED
        2.storage 110 状态不变，status 为 STORAGE
    """
    r, s = run_collect(collection)
    assert r
    assert m.DiskSnapshotStorage.objects.get(id=109).storage_status == m.DiskSnapshotStorage.RECYCLED
    assert m.DiskSnapshotStorage.objects.get(id=110).storage_status == m.DiskSnapshotStorage.STORAGE
    _assert_call_count(s, action_remove_cdp_file=0, action_remove_qcow_file=1,
                       action_delete_qcow_snapshot=0, action_merge_cdp_to_qcow=0,
                       action_merge_qcow_snapshot_type_a=0, action_merge_qcow_snapshot_type_b=0)
