import decimal
import functools
import glob
import os

from basic_library import xdata
from basic_library import xlogging
from ice_service import service
from storage_manager import models as m
from storage_manager import valid_storage_directory as vsd

_logger = xlogging.getLogger(__name__)


def vsd_check_path(file_path):
    """检测文件路径是否在有效的快照存储目录中"""

    def _real_decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kv):
            vsd.check_path(file_path)
            r = fn(*args, **kv)
            vsd.check_path(file_path)
            return r

        return wrapper

    return _real_decorator


def _remove_glob(path_glob):
    for path in glob.iglob(path_glob):
        os.remove(path)


def remove_cdp_file(file_path):
    """删除CDP文件，及其相关辅助文件"""

    @vsd_check_path(file_path)
    def _remove_cdp_file():
        if os.path.isfile(file_path):
            os.remove(file_path)
        _remove_glob([
            f'{file_path}_*.readmap',
            f'{file_path}_*.map',
        ])

    _remove_cdp_file()


def remove_qcow_file(file_path):
    """删除QCOW文件，及其相关辅助文件"""

    @vsd_check_path(file_path)
    def _remove_qcow_file():
        if os.path.isfile(file_path):
            os.remove(file_path)
        _remove_glob([
            f'{file_path}_*.hash',
            f'{file_path}_*.full_hash',
            f'{file_path}_*.map',
            f'{file_path}_*.snmap',
            f'{file_path}_*.binmap',
        ])

    _remove_qcow_file()


def delete_qcow_snapshot(file_path, snapshot_name):
    """删除QCOW文件中的快照点，及其相关辅助文件"""

    @vsd_check_path(file_path)
    def _delete_qcow_snapshot():
        service.ImageService.get_image_service().delete_snapshot_in_qcow_file(file_path, snapshot_name)

        _remove_glob([
            f'{file_path}_{snapshot_name}.hash',
            f'{file_path}_{snapshot_name}.full_hash',
            f'{file_path}_{snapshot_name}.map',
            f'{file_path}_{snapshot_name}.snmap',
            f'{file_path}_{snapshot_name}.binmap',
        ])

    _delete_qcow_snapshot()


def query_cdp_file_last_timestamp(path: str) -> decimal.Decimal:
    timestamp_range = service.LogicService.get_logic_service().query_cdp_file_timestamp_range(path)
    return timestamp_range[1]


def relocate_cdp_timestamp(path: str, timestamp: decimal.Decimal) -> decimal.Decimal:
    timestamp_range = service.LogicService.get_logic_service().query_cdp_file_timestamp_range(path)
    if timestamp_range[0] is None or timestamp <= timestamp_range[0]:
        return timestamp_range[0]
    elif timestamp >= timestamp_range[1]:
        return timestamp_range[1]
    else:
        return service.LogicService.get_logic_service().query_cdp_file_timestamp(path, timestamp)


def format_cdp_timestamp_for_read(timestamp_begin: decimal.Decimal = None,
                                  timestamp_end: decimal.Decimal = None) -> str:
    if timestamp_begin is None and timestamp_end is None:
        return 'all'
    elif timestamp_begin is None and timestamp_end is not None:
        return r'$~{}'.format(service.LogicService.get_logic_service().format_cdp_file_timestamp(timestamp_end))
    elif timestamp_begin is not None and timestamp_end is None:
        return r'{}~$'.format(service.LogicService.get_logic_service().format_cdp_file_timestamp(timestamp_begin))
    else:
        return r'{}~{}'.format(service.LogicService.get_logic_service().format_cdp_file_timestamp(timestamp_begin),
                               service.LogicService.get_logic_service().format_cdp_file_timestamp(timestamp_end))


def convert_chain_to_images(chain) -> tuple:
    """将 chain 转换为 image service 使用的参数

    :param chain: StorageChain 的派生类
        磁盘快照链
    :return: (int, list[{'file_path': , 'snapshot_name': },])
        虚拟磁盘字节大小，符合 ImageService 使用的数据（文件名+快照名）
    """
    disk_bytes = None
    images = list()

    storages = chain.key_storage_items
    storages_max_i = len(storages) - 1
    assert storages_max_i >= 0

    def _append_cdp_with_timestamp(_storage, _timestamp, need_relocate_timestamp):
        if _timestamp:
            if need_relocate_timestamp:
                images.append({
                    'file_path': _storage['image_path'],
                    'snapshot_name': format_cdp_timestamp_for_read(
                        None, relocate_cdp_timestamp(_storage['image_path'], _timestamp)),
                })
            else:
                images.append({
                    'file_path': _storage['image_path'],
                    'snapshot_name': format_cdp_timestamp_for_read(None, _timestamp),
                })
        else:
            if need_relocate_timestamp:
                images.append({
                    'file_path': _storage['image_path'],
                    'snapshot_name': format_cdp_timestamp_for_read(
                        None, query_cdp_file_last_timestamp(_storage['image_path'])),
                })
            else:
                images.append({
                    'file_path': _storage['image_path'],
                    'snapshot_name': format_cdp_timestamp_for_read(),
                })

    def _append_qcow_snapshot(_storage):
        images.append({
            'file_path': _storage['image_path'],
            'snapshot_name': _storage['disk_snapshot_storage_ident'],
        })

    for idx, storage in enumerate(storages):
        if idx == storages_max_i:
            disk_bytes = storage['disk_bytes']
            if storage['is_cdp_file']:
                _append_cdp_with_timestamp(storage, chain.timestamp, True)
            else:
                _append_qcow_snapshot(storage)
        else:
            if storage['is_cdp_file']:
                _append_cdp_with_timestamp(storage, storages[idx + 1]['parent_timestamp'], False)
            else:
                _append_qcow_snapshot(storage)

    return disk_bytes, images, storages


def merge_cdp_to_qcow(hash_type, rw_chain, merge_cdp_snapshot_storage_objs):
    def _check_params():
        assert len(params['cdp_files']) != 0
        for storage_obj in merge_cdp_snapshot_storage_objs:
            assert storage_obj.is_cdp_file
            assert params['disk_bytes'] == storage_obj.disk_bytes
        assert len(params['rw_chain_images']) != 0
        assert not new_storage['is_cdp_file']
        assert (hash_type == m.DiskSnapshotStorageRoot.ROOT_HASH_TYPE_NONE
                or params['new_snapshot_qcow_hash_path'])

    rw_chain_disk_bytes, rw_chain_images, rw_storages = convert_chain_to_images(rw_chain)
    new_storage = rw_storages[-1]

    params = {
        'disk_bytes': rw_chain_disk_bytes,
        'cdp_files': [{'file_path': storage_obj.image_path} for storage_obj in merge_cdp_snapshot_storage_objs],
        'new_snapshot_qcow_hash_path': (
            None if hash_type == m.DiskSnapshotStorageRoot.ROOT_HASH_TYPE_NONE
            else new_storage['inc_hash_path']),
        'rw_chain_images': rw_chain_images,
    }

    _check_params()

    pass  # TODO


def merge_qcow_snapshot_type_a(hash_type, children_snapshot_storage_objs, merge_storage_obj):
    if hash_type == m.DiskSnapshotStorageRoot.ROOT_HASH_TYPE_NONE:
        return  # do nothing

    if merge_storage_obj.full_hash_path:
        src_hash_path = merge_storage_obj.full_hash_path
    else:
        src_hash_path = merge_storage_obj.inc_hash_path
    assert src_hash_path

    for child_storage_obj in children_snapshot_storage_objs:
        assert not child_storage_obj.is_cdp_file
        if child_storage_obj.full_hash_path:
            continue  # 子快照具有全量数据hash，无需合并hash数据
        service.LogicService.get_logic_service().merge_qcow_hash_file(
            src_hash_path, child_storage_obj.inc_hash_path, merge_storage_obj.disk_bytes)


def merge_qcow_snapshot_type_b(hash_type, write_chain, merge_storage_obj):
    def _check_params():
        if hash_type == m.DiskSnapshotStorageRoot.ROOT_HASH_TYPE_NONE:
            assert not params['new_snapshot_qcow_full_hash_path']
            assert not params['new_snapshot_qcow_hash_path']
            assert not params['current_snapshot_qcow_full_hash_path']
            assert not params['current_snapshot_qcow_hash_path']
        else:
            assert ((params['new_snapshot_qcow_full_hash_path'] and params['current_snapshot_qcow_full_hash_path'])
                    or (params['new_snapshot_qcow_hash_path'] and params['current_snapshot_qcow_hash_path']))
        for storage in storages:
            assert storage['disk_bytes'] == params['disk_bytes']

    assert not merge_storage_obj.is_cdp_file
    storages = write_chain.key_storage_items
    assert len(storages) == 2
    prev_storage = storages[0]
    new_storage = storages[1]

    params = {
        'disk_bytes': merge_storage_obj.disk_bytes,
        'new_snapshot_qcow_file': new_storage['image_path'],
        'new_snapshot_qcow_ident': new_storage['disk_snapshot_storage_ident'],
        'new_snapshot_qcow_full_hash_path': new_storage['full_hash_path'],
        'new_snapshot_qcow_hash_path': new_storage['hash_path'],
        'current_snapshot_qcow_file': merge_storage_obj.image_path,
        'current_snapshot_qcow_ident': merge_storage_obj.ident,
        'current_snapshot_qcow_full_hash_path': merge_storage_obj.full_hash_path,
        'current_snapshot_qcow_hash_path': merge_storage_obj.hash_path,
        'prev_snapshot_qcow_file': prev_storage['image_path'],
        'prev_snapshot_qcow_ident': prev_storage['disk_snapshot_storage_ident'],
    }

    _check_params()

    pass  # TODO


def is_file_exist(file_path: str, raise_exception=False) -> bool:
    r = vsd.check_path(file_path, raise_exception) and os.path.isfile(file_path)
    if (not r) and raise_exception:
        xlogging.raise_and_logging_error(
            '存储文件不存在', f'file [{file_path}] not exist', print_args=False,
            exception_class=xdata.StorageImageFileNotExist, logger_level='info')
    return r


def is_all_files_exist(files: list, raise_exception=False) -> bool:
    """检查所有文件是否都存在

    :param files:
        [file_path: str, ...]
    :param raise_exception
    :remark:
        函数内部会进行去重优化
    """
    for file_path in set(files):
        if not is_file_exist(file_path, raise_exception):
            return False
    else:
        return True


def is_all_images_in_storage_info_exist(storage_info_list: list, raise_exception=False) -> bool:
    """检查所有 storage_info 的 image_path 文件是否都存在

    :param storage_info_list:
        参考 StorageChain 注释
    :param raise_exception
    """
    return is_all_files_exist([info['image_path'] for info in storage_info_list], raise_exception)
