import os
import typing
import uuid

from cpkt.core import exc
from cpkt.core import rt
from cpkt.core import xlogging as lg
from cpkt.rpc import ice

from business_logic import storage
from business_logic import storage_chain as chain
from business_logic import storage_reference_manager as srm
from data_access import models as m
from ice_service import service

_logger = lg.get_logger(__name__)


def _to_img_ident(storage_item: storage.StorageItem, timestamp=None):
    if storage_item.is_qcow:
        return ice.IMG.ImageSnapshotIdent(storage_item.image_path, storage_item.ident)
    if timestamp:  # TODO
        assert False, ('TODO ', 'TODO', 0)
    else:
        return ice.IMG.ImageSnapshotIdent(storage_item.image_path, 'all')


class DiskSnapshotAction(object):

    @staticmethod
    def generate_flag(trace_debug):
        """flag 为不超过255字符的字符串，表明调用者的身份，格式为 "PiD十六进制pid 模块名|创建原因"""

        return "PiD{} {}".format(str(hex(os.getpid())), trace_debug)[:255]

    @staticmethod
    def create_qcow_snapshot(chain_for_create: chain.StorageChain, raw_flag):
        new_storage_item = chain_for_create.last_storage_item
        write_img_prx = service.get_write_img_prx()
        handle = write_img_prx.create(
            _to_img_ident(new_storage_item),
            [_to_img_ident(item) for item in chain_for_create.key_storage_items[:-1]],
            new_storage_item.disk_bytes,
            raw_flag
        )
        if handle == 0 or handle == -1:
            raise exc.generate_exception_and_logger(
                '创建快照磁盘存储文件失败', f'create qcow {new_storage_item.ident} - {handle} failed', 0)
        else:
            _logger.info(r'create_qcow_snapshot ok {} {}'.format(new_storage_item.ident, handle))
        return handle, service.convert_proxy_to_string(write_img_prx)

    @staticmethod
    def create_cdp_snapshot(new_storage_item: storage.StorageItem, raw_flag):
        cdp_prx = service.get_cdp_prx()
        handle = cdp_prx.create(
            _to_img_ident(new_storage_item),
            [],
            new_storage_item.disk_bytes,
            raw_flag
        )
        if handle == 0 or handle == -1:
            raise exc.generate_exception_and_logger(
                '创建快照磁盘存储文件失败', f'create cdp {new_storage_item.ident} - {handle} failed', 0)
        else:
            _logger.info(r'create_cdp_snapshot ok {} {}'.format(new_storage_item.ident, handle))
        return handle, service.convert_proxy_to_string(cdp_prx)

    @staticmethod
    def close_disk_snapshot(raw_handle, ice_endpoint):
        return service.convert_string_to_prx(ice_endpoint).close(raw_handle, True)

    @staticmethod
    def open_disk_snapshot(acquired_chain: chain.StorageChain, raw_flag):
        read_img_prx = service.get_read_img_prx()
        return read_img_prx.open(
            [_to_img_ident(item) for item in acquired_chain.key_storage_items],
            raw_flag
        ), service.convert_proxy_to_string(read_img_prx)

    @staticmethod
    def move_data_from_qcow(source_storage: storage.Storage, target_chain: chain.StorageChain, raw_flag,
                            hash_version=0):
        pass

    @staticmethod
    def merge_cdp_to_qcow(rw_chain: chain.StorageChain, merge_cdp_snapshot_storages: typing.List[storage.Storage],
                          raw_flag, hash_version=0):
        pass

    @staticmethod
    def merge_qcow_hash(children_snapshot_storage: typing.List[storage.Storage], merge_storage: storage.Storage,
                        hash_version=0):
        pass

    @staticmethod
    def delete_qcow_snapshot(file_path, snapshot_name):
        returned = service.get_write_img_prx().DelSnaport(ice.IMG.ImageSnapshotIdent(file_path, snapshot_name))
        if returned == -2:
            _logger.error(
                r'快照磁盘镜像({})正在使用中，无法回收'.format(snapshot_name),
                r'delete snapshot {} - {} failed, using'.format(file_path, snapshot_name))
        elif returned != 0:
            _logger.error(
                r'回收快照磁盘镜像({})失败'.format(snapshot_name),
                r'delete snapshot {} - {} failed, {}'.format(file_path, snapshot_name, returned))

    @staticmethod
    def remove_cdp_file(file_path):
        """删除CDP文件，及其相关辅助文件"""

        if rt.PathInMount.is_in_not_mount(file_path):
            return False

        if not rt.delete_file(file_path):
            return False

        try:
            rt.remove_glob([
                f'{file_path}_*.readmap',
                f'{file_path}_*.map',
            ])
        except Exception as e:
            _logger.warning(f'remove_cdp_file remove_glob {file_path} failed. {e}')
            return False

        return True

    @staticmethod
    def remove_qcow_file(file_path) -> bool:
        """删除QCOW文件，及其相关辅助文件"""

        if rt.PathInMount.is_in_not_mount(file_path):
            return False

        if not rt.delete_file(file_path):
            return False

        try:
            rt.remove_glob([
                f'{file_path}_*.hash',
                f'{file_path}_*.full_hash',
                f'{file_path}_*.map',
                f'{file_path}_*.snmap',
                f'{file_path}_*.binmap',
            ])
        except Exception as e:
            _logger.warning(f'remove_qcow_file remove_glob {file_path} failed. {e}')
            return False

        return True


class ImagePathGenerator(object):

    @staticmethod
    def generate_cdp(folder, new_ident):
        """生成一个指定文件夹中的文件名，后缀cdp"""

        return os.path.join(folder, (new_ident + '.cdp'))

    @staticmethod
    def generate_qcow(parent_storage_obj: m.SnapshotStorage, folder, new_disk_bytes):
        if parent_storage_obj is None:
            return ImagePathGenerator.generate_new_qcow(folder)

        """
        需要创建新的文件的情况
        1. 如果与父的disk_bytes不同，
        2. 如果与父的存储目录路径不同，
        3. 如果父文件不是qcow，
        4. 如果父文件正在创建或写入中，

        其他情况
        1. 复用父快照文件 
        """
        if (
                new_disk_bytes != parent_storage_obj.disk_bytes
                or parent_storage_obj.type != m.SnapshotStorage.TYPE_QCOW
                or folder != os.path.split(parent_storage_obj.image_path)[0]
                or srm.get_srm().is_storage_writing(parent_storage_obj.image_path)
        ):
            return ImagePathGenerator.generate_new_qcow(folder)
        else:
            return parent_storage_obj.image_path

    @staticmethod
    def generate_new_qcow(folder):
        """生成一个指定文件夹中的文件名，后缀qcow"""

        return os.path.join(folder, (uuid.uuid4().hex + '.qcow'))
