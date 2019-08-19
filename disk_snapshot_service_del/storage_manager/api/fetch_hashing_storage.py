from basic_library import xlogging
from storage_manager import models as m
from storage_manager import storage_action as action
from storage_manager import storage_chain as chain
from storage_manager import storage_query as query
from storage_manager import storage_tree as tree
from task_manager import task_item_abc

_logger = xlogging.getLogger(__name__)


class StorageStatusNotHashing(Exception):
    pass


class HashingTask(task_item_abc.TaskItem):
    TASK_TYPE = 'hashing'

    def __init__(self, storage_ident, storage_root_obj, storage_locker_manager, storage_reference_manager):
        super(HashingTask, self).__init__()
        self.storage_ident = storage_ident
        self.storage_root_obj = storage_root_obj
        self.storage_locker_manager = storage_locker_manager
        self.storage_reference_manager = storage_reference_manager
        self.chain = None

    @property
    def name(self):
        return f'{self}'

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f'task {self.TASK_TYPE} {self.storage_ident}'

    def __eq__(self, other):
        return isinstance(other, HashingTask) and self.storage_ident == other.storage_ident

    def warn(self, msg, exc_info=False):
        _logger.warning(f'{self} - failed : {msg}', exc_info=exc_info)

    def _check_storage(self, storage_obj):
        assert not storage_obj.is_cdp_file

        if storage_obj.storage_status != m.DiskSnapshotStorage.HASHING:
            # 发生临界状态，进入锁空间前数据发生变更
            xlogging.raise_and_logging_error(
                '存储状态不为hashing', f'{self} occur StorageStatusNotHashing, maybe multi-thread race condition',
                print_args=False, exception_class=StorageStatusNotHashing, logger_level='info')

    def acquire(self):
        """获取chain对象"""
        super(HashingTask, self).acquire()
        try:
            with self.storage_locker_manager.get_locker(self.storage_root_obj.root_ident, self.name):
                storage_obj = m.DiskSnapshotStorage.objects.get(disk_snapshot_storage_ident=self.storage_ident)
                self._check_storage(storage_obj)

                storage_tree = tree.DiskSnapshotStorageTree.create_instance_by_storage_root(self.storage_root_obj)
                assert not storage_tree.is_empty()

                self.chain = query.StorageChainQueryByDiskSnapshotStorage(
                    chain.StorageChainForRead, storage_tree, self.storage_reference_manager,
                    storage_obj, None, self.name).get_storage_chain()

                return self.chain.acquire()
        except Exception:
            self.release()
            raise

    def release(self):
        if super(HashingTask).release() and self.chain:
            self.chain.release()
            self.chain = None


class FetchHashingStorage(object):
    """获取需要进行hash处理的storage"""

    def __init__(self, storage_reference_manager, storage_locker_manager, task_container, task_ident):
        self.storage_reference_manager = storage_reference_manager
        self.storage_locker_manager = storage_locker_manager
        self.task_container = task_container
        self.task_ident = task_ident

    def fetch(self):
        for storage_obj in m.DiskSnapshotStorage.objects.filter(
                storage_type=m.DiskSnapshotStorage.QCOW,
                storage_status=m.DiskSnapshotStorage.HASHING).all():

            hashing_task = HashingTask(
                storage_obj.disk_snapshot_storage_ident,
                storage_obj.storage_root,
                self.storage_locker_manager, self.storage_reference_manager)
            if not self.task_container.add_task(HashingTask.TASK_TYPE, self.task_ident, hashing_task):
                continue

            try:
                _chain = hashing_task.acquire()
                return self._fetch_hashing_and_parent_which_in_same_file(_chain)
            except Exception as e:
                if not isinstance(e, StorageStatusNotHashing):
                    hashing_task.warn(f'hashing_task.acquire failed : {e}', True)
                self.task_container.remove_task(HashingTask.TASK_TYPE, self.task_ident, True)
        else:
            return None

    @staticmethod
    def _fetch_hashing_and_parent_which_in_same_file(_chain) -> dict:
        """获取需要hashing的快照点，以及在同一个文件中前一个快照点

        :return:
            {
                'disk_bytes': int,  磁盘字节大小
                'key_storage_items': [   至少有一个元素，最后一个元素为需要 hashing 的 storage。
                                仅有与 hashing storage 同文件的 storage，也就是说最多两个 storage
                    {
                        'image_path': str,
                        'storage_ident': str,
                    },
                    ...
                ],
                'read_storages': [  读取快照数据需要的 key_storage_items
                    {
                        'image_path': str,
                        'storage_ident': str,
                    },
                    ...
                ],
            }
        """

        def _not_in_same_file():
            return storage_info_list[0]['image_path'] != storage_info_list[1]['image_path']

        storage_info_list = _chain.storage_items[-2:]
        assert len(storage_info_list) in (1, 2,)
        if len(storage_info_list) == 2 and _not_in_same_file():
            storage_info_list.pop(0)

        action.is_all_images_in_storage_info_exist(storage_info_list, True)

        return {
            'disk_bytes': storage_info_list[-1]['disk_bytes'],
            'key_storage_items': [
                {'image_path': info['image_path'], 'storage_ident': info['disk_snapshot_storage_ident'], }
                for info in storage_info_list
            ],
            'read_storages': [
                {'image_path': info['image_path'], 'storage_ident': info['disk_snapshot_storage_ident'], }
                for info in _chain.key_storage_items
            ]
        }
