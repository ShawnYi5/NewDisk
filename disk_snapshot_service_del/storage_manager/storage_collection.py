import abc
import os
import uuid
from functools import lru_cache

from django.db import transaction

from basic_library import xfunctions
from basic_library import xlogging
from storage_manager import models as m
from storage_manager import storage_action as action
from storage_manager import storage_chain as chain
from storage_manager import storage_query as query
from storage_manager import storage_reference_manager as srm
from storage_manager import storage_tree as tree

_logger = xlogging.getLogger(__name__)


class RecyclingWorkBase(abc.ABC):
    """回收作业基类"""

    def __init__(self):
        super(RecyclingWorkBase, self).__init__()
        self.work_successful = False

    @abc.abstractmethod
    def work(self):
        """作业逻辑

        :remark:
            完成实际处理数据的过程，该过程将在锁空间外执行
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def save_work_result(self):
        """保存作业结果

        :remark:
            实际处理数据成功后更新数据库，该过程将在锁空间内执行，不可有除数据库访问以外的IO
        """
        raise NotImplementedError()


class DeleteWork(RecyclingWorkBase):
    """删除作业基类

    :remark:
        save_work_result 中需要修改快照存储的状态，标记已经回收完毕
        需要重写__eq__，支持去除重复任务。例如：qcow格式中，一个文件可存储多个快照；
            那么当该文件中所有快照都需要删除时，仅仅需要一个删除文件作业
    """

    def __init__(self, storage_obj):
        super(DeleteWork, self).__init__()
        self.storage_obj = storage_obj
        assert self.storage_obj.storage_status == m.DiskSnapshotStorage.RECYCLING

    @property
    @abc.abstractmethod
    def worker_ident(self):
        raise NotImplementedError()

    def __eq__(self, other):
        return self.worker_ident == other.worker_ident


class DeleteFileWork(DeleteWork):
    """删除文件作业

    :remark:
        支持删除 qcow 与 cdp 文件
    """

    def __init__(self, storage_obj):
        super(DeleteFileWork, self).__init__(storage_obj)
        self.storage_objs = (m.DiskSnapshotStorage.objects
                             .filter(image_path=self.file_path)
                             .exclude(storage_status=m.DiskSnapshotStorage.RECYCLED))
        assert self.storage_objs.exclude(storage_status=m.DiskSnapshotStorage.RECYCLING).count() == 0

    def __str__(self):
        return f'delete_file_work:<{self.file_path}>'

    def __repr__(self):
        return self.__str__()

    @property
    def worker_ident(self):
        return f'{self.file_path}:delete_file_work'

    def work(self):
        @xfunctions.convert_exception_to_value(False, self.warn)
        def _work():
            if self.storage_obj.is_cdp_file:
                action.remove_cdp_file(self.file_path)
            else:
                action.remove_qcow_file(self.file_path)
            return True

        self.work_successful = _work()

    def save_work_result(self):
        if self.work_successful:
            for storage_obj in self.storage_objs.all():
                storage_obj.set_storage_status(m.DiskSnapshotStorage.RECYCLED)
        return self.work_successful

    def warn(self, msg, **kwargs):
        _logger.warning(f'{self} failed\n{msg}\n', **kwargs)

    @property
    def file_path(self):
        return self.storage_obj.image_path


class DeleteQcowSnapshotWork(DeleteWork):
    """删除qcow文件中的快照点作业

    :remark:
        该逻辑不负责合并快照点相关数据（例如：hash数据）；在执行该逻辑前，应该保证快照点相关数据已经合并或确实不再需要
    """

    def __init__(self, storage_obj):
        super(DeleteQcowSnapshotWork, self).__init__(storage_obj)
        assert not storage_obj.is_cdp_file

    def __str__(self):
        return f'delete_qcow_snapshot_work:<{self.file_path}:{self.snapshot_name}>'

    def __repr__(self):
        return self.__str__()

    @property
    def worker_ident(self):
        return f'{self.snapshot_name}:{self.file_path}:delete_qcow_snapshot_work'

    def work(self):
        @xfunctions.convert_exception_to_value(False, self.warn)
        def _work():
            action.delete_qcow_snapshot(self.file_path, self.snapshot_name)
            return True

        self.work_successful = _work()

    def save_work_result(self):
        if self.work_successful:
            self.storage_obj.set_storage_status(m.DiskSnapshotStorage.RECYCLED)
        return self.work_successful

    def warn(self, msg, **kwargs):
        _logger.warning(f'{self} failed\n{msg}', **kwargs)

    @property
    def file_path(self):
        return self.storage_obj.image_path

    @property
    def snapshot_name(self):
        return self.storage_obj.disk_snapshot_storage_ident


class MergeWork(RecyclingWorkBase):
    """合并作业基类

    :remark:
        save_work_result 中需要修改快照存储的依赖关系，使被合并的源节点成为叶子
    """

    def __init__(self, parent_storage_obj, children_snapshot_storage_objs):
        super(MergeWork, self).__init__()
        self.parent_storage_obj = parent_storage_obj
        self.children_snapshot_storage_objs = children_snapshot_storage_objs
        self.new_storage_obj = self._create_or_get_new_storage_obj()

    @abc.abstractmethod
    def _create_or_get_new_storage_obj(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def _fill_more_detail_info(self, info_list):
        raise NotImplementedError()

    def _set_new_storage_obj_status_exception(self):
        if self.new_storage_obj.storage_status != m.DiskSnapshotStorage.EXCEPTION:
            self.new_storage_obj.set_storage_status(m.DiskSnapshotStorage.EXCEPTION)

    def _update_children_storage_objs(self):
        for child_storage_obj in self.children_snapshot_storage_objs:
            child_storage_obj.parent_snapshot = self.new_storage_obj
            child_storage_obj.save(update_fields=['parent_snapshot', ])

    def warn(self, msg, **kwargs):
        msg_list = [f'{self} failed', '\n', msg, ]
        self._fill_detail_info(msg_list)
        self._fill_more_detail_info(msg_list)
        msg_list.append('\n')
        _logger.warning('\n'.join(msg_list), **kwargs)

    def _fill_detail_info(self, info_list):
        info_list.append(f'  parent_storage_obj : {self.parent_storage_obj}')
        info_list.append(f'  new_storage_obj    : {self.new_storage_obj}')
        info_list.append(f'  children_snapshot_storage_objs:')
        for storage_obj in self.children_snapshot_storage_objs:
            info_list.append(f'    {storage_obj}')

    @staticmethod
    def _update_storage_obj_locator_to_none(storage_obj):
        storage_obj.locator = None
        storage_obj.save(update_fields=('locator',))


class MergeCdpWork(MergeWork):
    """合并CDP快照存储到新快照点作业"""

    def __init__(
            self, parent_storage_obj, merge_cdp_snapshot_storage_objs, children_snapshot_storage_objs, storage_tree):
        super(MergeCdpWork, self).__init__(parent_storage_obj, children_snapshot_storage_objs)
        self.merge_cdp_snapshot_storage_objs = merge_cdp_snapshot_storage_objs
        self.rw_chain = self._create_rw_chain(storage_tree)

    def __del__(self):
        if self.rw_chain:  # pragma: no cover
            self.rw_chain.release()

    def _create_rw_chain(self, storage_tree):
        rw_chain = query.StorageChainQueryByDiskSnapshotStorage(
            chain.StorageChainForRW, storage_tree, srm.StorageReferenceManager.get_storage_reference_manager(),
            self.parent_storage_obj, None, str(self)
        ).get_storage_chain()
        rw_chain.insert_tail(self.new_storage_obj)
        return rw_chain.acquire()

    @staticmethod
    def _generate_storage_file_name():
        return f'{uuid.uuid4().hex}.qcow'

    @staticmethod
    def _generate_storage_ident():
        return uuid.uuid4().hex

    def _generate_storage_image_path(self):
        return os.path.join(
            os.path.dirname(self.parent_storage_obj.image_path), self._generate_storage_file_name()
        )

    def _create_or_get_new_storage_obj(self):
        assert self.parent_storage_obj
        assert len(self.children_snapshot_storage_objs) > 0

        last_cdp_storage_obj = self.children_snapshot_storage_objs[-1]
        storage_ident = self._generate_storage_ident()

        new_obj_dict = {
            'storage_root': self.parent_storage_obj.storage_root,
            'source_disk': self.parent_storage_obj.source_disk,
            'locator': None,
            'storage_type': m.DiskSnapshotStorage.QCOW,
            'storage_status': m.DiskSnapshotStorage.CREATING,
            'disk_snapshot_storage_ident': storage_ident,
            'disk_bytes': self.parent_storage_obj.disk_bytes,
            'image_path': None,
            'full_hash_path': None,
            'inc_hash_path': None,
            'storage_begin_timestamp': last_cdp_storage_obj.storage_end_timestamp,
            'storage_end_timestamp': last_cdp_storage_obj.storage_end_timestamp,
            'parent_snapshot': self.parent_storage_obj,
            'parent_timestamp': None,
            'file_level_deduplication': False,
        }

        if self.parent_storage_obj.is_cdp_file:  # 如果父节点为cdp，那么就需要创建新的qcow来存放合并后的数据
            new_obj_image_path = self._generate_storage_image_path()
        else:  # 如果父节点为qcow，那么就直接将合并后的数据存放到父节点qcow中
            new_obj_image_path = self.parent_storage_obj.image_path

        new_obj_dict['image_path'] = new_obj_image_path
        new_obj_dict['inc_hash_path'] = f'{new_obj_image_path}_{storage_ident}.hash'

        return m.DiskSnapshotStorage.objects.create(**new_obj_dict)

    def __str__(self):
        return f'merge_cdp_work:<{self.new_storage_obj}>'

    def __repr__(self):
        return self.__str__()

    def work(self):
        @xfunctions.convert_exception_to_value(False, self.warn)
        def _work():
            action.merge_cdp_to_qcow(
                self.parent_storage_obj.storage_root.hash_type, self.rw_chain, self.merge_cdp_snapshot_storage_objs)
            self.new_storage_obj.set_storage_status(m.DiskSnapshotStorage.STORAGE)
            return True

        try:
            self.work_successful = _work()
        finally:
            self.rw_chain.release()
            self.rw_chain = None

    def save_work_result(self):
        if self.work_successful:
            self._update_children_storage_objs()
            self._update_cdp_storage_objs_locator_to_none()
        else:
            self._set_new_storage_obj_status_exception()
        return self.work_successful

    def _update_cdp_storage_objs_locator_to_none(self):
        for cdp_storage_obj in self.merge_cdp_snapshot_storage_objs:
            assert cdp_storage_obj.is_cdp_file
            self._update_storage_obj_locator_to_none(cdp_storage_obj)

    def _fill_more_detail_info(self, info_list):
        info_list.append(f'  merge_cdp_snapshot_storage_objs:')
        for storage_obj in self.merge_cdp_snapshot_storage_objs:
            info_list.append(f'    {storage_obj}')


class MergeQcowSnapshotTypeAWork(MergeWork):
    """合并qcow文件中的快照

    :remark:
        合并过程仅涉及单个qcow文件
        没有实体数据搬迁
    """

    def __init__(self, parent_storage_obj, merge_storage_obj, children_snapshot_storage_objs):
        self.merge_storage_obj = merge_storage_obj
        super(MergeQcowSnapshotTypeAWork, self).__init__(parent_storage_obj, children_snapshot_storage_objs)
        if self._is_merge_root_storage:
            # 如果被合并的根节点，那么子节点的数量必须为 1
            assert len(children_snapshot_storage_objs) == 1
            assert merge_storage_obj.parent_snapshot is None

    @property
    def _is_merge_root_storage(self) -> bool:
        return self.parent_storage_obj is None

    def _create_or_get_new_storage_obj(self):
        return self.parent_storage_obj

    def __str__(self):
        return f'merge_qcow_snapshot_type_a_work:<{self.merge_storage_obj}>'

    def __repr__(self):
        return self.__str__()

    def work(self):
        @xfunctions.convert_exception_to_value(False, self.warn)
        def _work():
            action.merge_qcow_snapshot_type_a(self.merge_storage_obj.storage_root.hash_type,
                                              self.children_snapshot_storage_objs, self.merge_storage_obj)
            return True

        self.work_successful = _work()

    def save_work_result(self):
        if self.work_successful:
            if self._is_merge_root_storage:  # 防止出现树分裂，将原始根移动到特殊 storage_root 中
                self.merge_storage_obj.storage_root = m.DiskSnapshotStorageRoot.get_recycle_root_obj()
                self.merge_storage_obj.save(update_fields=['storage_root', ])
            self._update_children_storage_objs()
            self._update_storage_obj_locator_to_none(self.merge_storage_obj)
        return self.work_successful

    def _fill_more_detail_info(self, info_list):
        info_list.append(f'  merge_storage_obj  : {self.merge_storage_obj}')


class MergeQcowSnapshotTypeBWork(MergeWork):
    """跨qcow文件合并快照

    :remark:
        合并过程涉及两个qcow文件
        实体数据将从一个qcow文件搬迁到另一个qcow文件中
    """

    def __init__(self, parent_storage_obj, merge_storage_obj, children_snapshot_storage_objs, storage_tree):
        self.merge_storage_obj = merge_storage_obj
        super(MergeQcowSnapshotTypeBWork, self).__init__(parent_storage_obj, children_snapshot_storage_objs)
        self.write_chain = self._create_write_chain(storage_tree)

    def __del__(self):
        if self.write_chain:  # pragma: no cover
            self.write_chain.release()

    def _create_write_chain(self, storage_tree):
        write_chain = query.StorageChainQueryByDiskSnapshotStorage(
            chain.StorageChainForWrite, storage_tree, srm.StorageReferenceManager.get_storage_reference_manager(),
            self.parent_storage_obj, None, str(self)
        ).get_storage_chain()
        write_chain.insert_tail(self.new_storage_obj)
        return write_chain.acquire()

    def _generate_hash_path(self, new_obj_dict):
        if self.merge_storage_obj.storage_root.hash_type == m.DiskSnapshotStorageRoot.ROOT_HASH_TYPE_NONE:
            return

        if self.merge_storage_obj.full_hash_path:
            new_obj_dict['full_hash_path'] = (
                f"{new_obj_dict['image_path']}_{new_obj_dict['disk_snapshot_storage_ident']}.full_hash")
        else:
            new_obj_dict['inc_hash_path'] = (
                f"{new_obj_dict['image_path']}_{new_obj_dict['disk_snapshot_storage_ident']}.hash")

        assert new_obj_dict['full_hash_path'] or new_obj_dict['inc_hash_path']

    def _create_or_get_new_storage_obj(self):
        assert self.parent_storage_obj
        assert not self.parent_storage_obj.is_cdp_file
        assert self.merge_storage_obj
        assert m.DiskSnapshotStorage.objects.filter(image_path=self.merge_storage_obj.image_path).count() == 1
        assert len(self.children_snapshot_storage_objs) > 0

        new_obj_dict = {
            'storage_root': self.parent_storage_obj.storage_root,
            'source_disk': self.parent_storage_obj.source_disk,
            'locator': None,
            'storage_type': m.DiskSnapshotStorage.QCOW,
            'storage_status': m.DiskSnapshotStorage.CREATING,
            'disk_snapshot_storage_ident': uuid.uuid4().hex,
            'disk_bytes': self.parent_storage_obj.disk_bytes,
            'image_path': self.parent_storage_obj.image_path,
            'full_hash_path': None,
            'inc_hash_path': None,
            'storage_begin_timestamp': self.merge_storage_obj.storage_end_timestamp,
            'storage_end_timestamp': self.merge_storage_obj.storage_end_timestamp,
            'parent_snapshot': self.parent_storage_obj,
            'parent_timestamp': None,
            'file_level_deduplication': False,
        }

        self._generate_hash_path(new_obj_dict)

        return m.DiskSnapshotStorage.objects.create(**new_obj_dict)

    def __str__(self):
        return f'merge_qcow_snapshot_type_b_work:<{self.merge_storage_obj}>'

    def __repr__(self):
        return self.__str__()

    def work(self):
        @xfunctions.convert_exception_to_value(False, self.warn)
        def _work():
            action.merge_qcow_snapshot_type_b(
                self.merge_storage_obj.storage_root.hash_type, self.write_chain, self.merge_storage_obj)
            self.new_storage_obj.set_storage_status(m.DiskSnapshotStorage.STORAGE)
            return True

        try:
            self.work_successful = _work()
        finally:
            self.write_chain.release()
            self.write_chain = None

    def save_work_result(self):
        if self.work_successful:
            self._update_children_storage_objs()
            self._update_storage_obj_locator_to_none(self.merge_storage_obj)
        else:
            self._set_new_storage_obj_status_exception()
        return self.work_successful

    def _fill_more_detail_info(self, info_list):
        info_list.append(f'  merge_storage_obj  : {self.merge_storage_obj}')


class QueryHostSnapshotObjsByLocatorWithCache(object):
    def __init__(self, storage_root_ident):
        self.name = f'QueryHostSnapshotsByLocatorWithCache:{storage_root_ident}'
        self._valid = False

    def __str__(self):  # pragma: no cover
        return self.name

    def __repr__(self):  # pragma: no cover
        return self.__str__()

    def __enter__(self):
        self._valid = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _ = exc_type
        _ = exc_val
        _ = exc_tb
        self._valid = False
        self.query.cache_clear()

    @lru_cache()
    def query(self, locator_id):
        assert self._valid
        return m.HostSnapshot.objects.filter(disk_snapshots__locator_id=locator_id).all()


class StorageCollection(object):
    """快照存储回收逻辑"""

    def __init__(self, storage_root_obj, storage_reference_manager, storage_locker_manager):
        """
        :param storage_root_obj:
            存储镜像依赖树标识
        :param storage_reference_manager: StorageReferenceManager
            引用管理器
        :param storage_locker_manager: StorageLockerManager
            存储镜像锁管理器
        """
        self.name = f'storage_collection:[{storage_root_obj.root_ident}]'
        self.storage_root_obj = storage_root_obj
        self.storage_reference_manager = storage_reference_manager
        self.storage_locker_manager = storage_locker_manager

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.__str__()

    def collect(self):
        """执行一轮回收逻辑

        :remark:
            同步阻塞
        :raises:
            StorageLockerNotExist
        """

        assert self.storage_root_obj.root_valid
        assert self.storage_root_obj.hash_type != m.DiskSnapshotStorageRoot.ROOT_HASH_TYPE_UNKNOWN

        if self.storage_root_obj.is_recycle_root:
            works = self._analyze_recycle_root()
        else:
            with QueryHostSnapshotObjsByLocatorWithCache(self.storage_root_obj.root_ident) as query_host_snapshots:
                works = self._analyze_storage_and_create_recycling_works(query_host_snapshots)

        if works:
            for work in works:
                work.work()
            return self._save_works_result(works)
        else:
            return False

    def _save_works_result(self, works):
        work_successful = False
        with self.storage_locker_manager.get_locker(self.storage_root_obj.root_ident, self.name):
            for work in works:
                if work.save_work_result():
                    work_successful = True
        return work_successful

    def _analyze_recycle_root(self) -> list:
        with self.storage_locker_manager.get_locker(self.storage_root_obj.root_ident, self.name), transaction.atomic():
            delete_storage_objs = list()

            for storage_obj in m.DiskSnapshotStorage.valid_storage_objs(self.storage_root_obj).all():
                if self.storage_reference_manager.is_storage_using(storage_obj.disk_snapshot_storage_ident):
                    continue
                delete_storage_objs.append(storage_obj)
            return self._create_delete_works(delete_storage_objs)

    def _analyze_storage_and_create_recycling_works(self, query_host_snapshots) -> list:
        """分析存储快照并获取回收存储快照的作业

        :remark:
            为了优化性能，禁止使用ORM对象去查找父与子，改为使用Node对象查找
        """
        with self.storage_locker_manager.get_locker(self.storage_root_obj.root_ident, self.name), transaction.atomic():
            storage_tree = tree.DiskSnapshotStorageTree.create_instance_by_storage_root(self.storage_root_obj)
            if storage_tree.is_empty():
                self.storage_root_obj.set_invalid()
                return list()

            delete_storage_objs = self._fetch_and_mark_delete_storage_objs(storage_tree, query_host_snapshots)
            if delete_storage_objs:
                return self._create_delete_works(delete_storage_objs)  # 生成删除作业

            for node in storage_tree.nodes_by_bfs:
                # 从根向叶子做广度优先遍历，找到可回收的快照存储
                if node.is_root and len(node.children) > 1:
                    continue  # 不支持：此时如果合并，那么快照树会分裂为两棵树

                if node.is_leaf:
                    continue  # 不支持：当前节点为叶子，应该走删除逻辑，而非回收逻辑

                storage_obj = node.storage_obj

                if storage_obj.file_level_deduplication:
                    # 逻辑为 not storage_obj.is_cdp_file and storage_obj.file_level_deduplication
                    # 不支持：带有文件级去重
                    continue

                if not self._can_disk_snapshot_storage_merge(
                        storage_obj, query_host_snapshots, node, self._get_parent_storage_obj_by_node(node)):
                    continue

                if storage_obj.is_cdp_file:
                    merge_cdp_snapshot_storage_objs = self._fetch_and_mark_merge_cdp_snapshot_storage_objs(
                        query_host_snapshots, node)
                    if merge_cdp_snapshot_storage_objs:
                        return [
                            MergeCdpWork(
                                node.parent.storage_obj, merge_cdp_snapshot_storage_objs,
                                [n.storage_obj for n in node.children], storage_tree),
                        ]
                elif self._is_children_in_other_file(node):
                    if node.is_root:
                        continue  # 不支持：没有父快照
                    elif node.parent.storage_obj.is_cdp_file:
                        continue  # 不支持：父快照是CDP文件
                    elif node.parent.storage_obj.disk_bytes != storage_obj.disk_bytes:
                        continue  # 不支持：虚拟磁盘大小不一致
                    elif node.parent.storage_obj.storage_status != m.DiskSnapshotStorage.STORAGE:
                        continue  # 不支持：父快照处于改写中的状态
                    elif self._is_multi_snapshot_in_the_qcow(node):
                        continue  # 不支持：还有其他快照点在该qcow
                    elif self.storage_reference_manager.is_storage_writing(node.parent.storage_obj.image_path):
                        continue  # 不支持：父快照的文件正在写入中
                    else:
                        self._set_status_to_recycling(storage_obj)
                        return [
                            MergeQcowSnapshotTypeBWork(
                                node.parent.storage_obj, storage_obj, [n.storage_obj for n in node.children],
                                storage_tree),
                        ]
                elif self.storage_reference_manager.is_storage_writing(node.storage_obj.image_path):
                    continue  # 不支持：该快照的文件正在写入中
                else:
                    self._set_status_to_recycling(storage_obj)
                    return [
                        MergeQcowSnapshotTypeAWork(
                            self._get_parent_storage_obj_by_node(node),
                            storage_obj,
                            [n.storage_obj for n in node.children]),
                    ]

        return list()

    def _fetch_and_mark_delete_storage_objs(self, storage_tree, query_host_snapshots) -> list:
        delete_storage_objs = list()
        for leaf in storage_tree.leaves:
            # 从叶子向根深度优先遍历，找到可以直接删除的快照存储
            for node in tree.dfs_to_root(leaf):
                storage_obj = node.storage_obj
                if self._can_disk_snapshot_storage_delete(storage_obj, node, query_host_snapshots):
                    delete_storage_objs.append(storage_obj)
                    self._set_status_to_recycling(storage_obj)
                else:
                    break
        return delete_storage_objs

    def _fetch_and_mark_merge_cdp_snapshot_storage_objs(self, query_host_snapshots, node) -> list:
        merge_cdp_snapshot_storage_objs = list()
        current_node = node

        while True:
            storage_obj = current_node.storage_obj
            assert storage_obj.is_cdp_file

            parent_storage_obj = current_node.parent.storage_obj  # cdp一定有父快照

            if parent_storage_obj.storage_status not in m.DiskSnapshotStorage.STATUS_CAN_MERGE:
                break  # 如果父快照存储正在生成中，那么就不进入回收流程
            if self._is_child_depend_with_timestamp(current_node):
                break  # 不支持cdp文件的中间有依赖的情况
            if ((not parent_storage_obj.is_cdp_file)
                    and self.storage_reference_manager.is_storage_writing(parent_storage_obj.image_path)):
                break  # 如果父快照存储正在写入中，那么就不进入回收流程

            self._set_status_to_recycling(storage_obj)
            merge_cdp_snapshot_storage_objs.append(storage_obj)

            current_node = self._get_child_node_with_cdp_disk_snapshot_storage(current_node)
            if current_node is None:
                break

            # remark： current_node 已经变更, 不可再使用 storage_obj
            if not self._can_disk_snapshot_storage_merge(current_node.storage_obj, query_host_snapshots, node):
                break

        return merge_cdp_snapshot_storage_objs

    @staticmethod
    def _set_status_to_recycling(storage_obj):
        if storage_obj.storage_status == m.DiskSnapshotStorage.RECYCLING:
            return
        storage_obj.set_storage_status(m.DiskSnapshotStorage.RECYCLING)

    @staticmethod
    def _is_all_locator_invalid(storage_obj, query_host_snapshots, node) -> bool:
        if not storage_obj.locator_id:
            return True

        for host_snapshot_obj in query_host_snapshots.query(storage_obj.locator_id):
            if not host_snapshot_obj.host_snapshot_valid:
                continue

            # 判断storage是否在host snapshot的描述范围内
            if not (storage_obj.storage_begin_timestamp > host_snapshot_obj.host_snapshot_end_timestamp
                    or storage_obj.storage_end_timestamp < host_snapshot_obj.host_snapshot_begin_timestamp):
                return False

            # 这里特别处理 cdp host snapshot 描述范围不包含 storage， 但 storage 又没有同 locator 的子。
            # 在CDP备份时，目标机多块磁盘，其中某块磁盘几乎没有写入的情况下，可能出现
            if host_snapshot_obj.host_snapshot_type == m.HostSnapshot.CDP:
                for child_node in node.children:
                    if child_node.storage_obj.locator_id == storage_obj.locator_id:
                        break
                else:
                    return False
        else:
            return True

    def _can_disk_snapshot_storage_delete(self, storage_obj, node, query_host_snapshots) -> bool:
        if storage_obj.storage_status not in m.DiskSnapshotStorage.STATUS_CAN_DELETE:
            return False

        if not self._is_all_locator_invalid(storage_obj, query_host_snapshots, node):
            return False

        if self.storage_reference_manager.is_storage_using(storage_obj.disk_snapshot_storage_ident):
            return False

        if (not storage_obj.is_cdp_file) and self.storage_reference_manager.is_storage_writing(storage_obj.image_path):
            return False

        for child_node in node.children:
            if child_node.storage_obj.storage_status not in m.DiskSnapshotStorage.STATUS_RECYCLE:
                return False

        return True

    @staticmethod
    def _get_child_node_with_cdp_disk_snapshot_storage(node):
        children = node.children
        assert len(children) == 1  # 逻辑上可回收的CDP子快照仅能是一个

        child_node = children[0]
        storage_obj = child_node.storage_obj

        if storage_obj.is_cdp_file and (not child_node.is_leaf):  # 叶子应该走删除逻辑，回收逻辑不处理
            assert storage_obj.locator_id == node.storage_obj.locator_id
            return child_node
        else:
            return None

    @staticmethod
    def _create_delete_works(deleting_storage_obj_list) -> list:
        works = list()

        def insert_work(_work):
            if _work not in works:
                works.append(_work)

        for storage_obj in deleting_storage_obj_list:
            if storage_obj.is_cdp_file:
                insert_work(DeleteFileWork(storage_obj))
            else:
                image_path = storage_obj.image_path
                valid_snapshot_cnt = (m.DiskSnapshotStorage.objects
                                      .filter(image_path=image_path)
                                      .exclude(storage_status__in=m.DiskSnapshotStorage.STATUS_RECYCLE).count()
                                      )
                if valid_snapshot_cnt:
                    insert_work(DeleteQcowSnapshotWork(storage_obj))
                else:
                    insert_work(DeleteFileWork(storage_obj))
        return works

    def _can_disk_snapshot_storage_merge(
            self, storage_obj, query_host_snapshots, node, parent_storage_obj=None) -> bool:
        if storage_obj.storage_status not in m.DiskSnapshotStorage.STATUS_CAN_MERGE:
            return False

        if (parent_storage_obj and (not parent_storage_obj.is_cdp_file)
                and (parent_storage_obj.storage_status == m.DiskSnapshotStorage.RECYCLING)):
            return False

        if not self._is_all_locator_invalid(storage_obj, query_host_snapshots, node):
            return False

        return True

    @staticmethod
    def _is_child_depend_with_timestamp(node):
        for child in node.children:
            storage_obj = child.storage_obj
            if storage_obj.parent_timestamp is not None:
                return True
        else:
            return False

    @staticmethod
    def _is_children_in_other_file(node):
        storage_obj = node.storage_obj
        for child in node.children:
            if storage_obj.image_path != child.storage_obj.image_path:
                return True
        else:
            return False

    @staticmethod
    def _is_multi_snapshot_in_the_qcow(node):
        #  外部逻辑保证 node.is_root is False
        if node.parent.storage_obj.image_path == node.storage_obj.image_path:
            return True
        for child in node.children:
            if child.storage_obj.image_path == node.storage_obj.image_path:
                return True
        else:
            return False

    @staticmethod
    def _get_parent_storage_obj_by_node(node):
        return None if node.is_root else node.parent.storage_obj
