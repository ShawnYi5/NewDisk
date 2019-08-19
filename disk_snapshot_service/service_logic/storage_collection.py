import abc
import os
import typing
import uuid

from cpkt.core import rt
from cpkt.core import xlogging as lg

from business_logic import locker_manager as lm
from business_logic import storage
from business_logic import storage_action as action
from business_logic import storage_chain as chain
from business_logic import storage_reference_manager as srm
from business_logic import storage_tree as tree
from data_access import models as m
from data_access import session as s

_logger = lg.get_logger(__name__)


class RecyclingWorkBase(abc.ABC):
    """回收作业基类"""

    def __init__(self):
        super(RecyclingWorkBase, self).__init__()
        self.work_successful = False

    def __repr__(self):
        return self.__str__()

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

    @abc.abstractmethod
    def alloc_resource(self):
        """分配作业需要的资源

        :remark:
            在进行作业逻辑前执行，该过程将在锁空间内执行，不可有除数据库访问以外的IO
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def free_resource(self):
        """释放作业持有的资源

        :remark:
            在作业的生命周期结束时，被执行。
            可能没有调用 alloc_resource 也直接调用 free_resource
        """
        raise NotImplementedError()


class DeleteWork(RecyclingWorkBase):
    """删除作业基类

    :remark:
        save_work_result 中需要修改快照存储的状态，标记已经回收完毕
        需要重写__eq__，支持去除重复任务。例如：qcow格式中，一个文件可存储多个快照；
            那么当该文件中所有快照都需要删除时，仅仅需要一个删除文件作业
    """

    def __init__(self, storage_obj: m.SnapshotStorage, call_name: str):
        super(DeleteWork, self).__init__()
        assert storage_obj.status in (m.SnapshotStorage.STATUS_RECYCLING, m.SnapshotStorage.STATUS_ABNORMAL,)
        self.duplicated = False
        self.w_chain = chain.StorageChainForWrite(srm.get_srm(), call_name).insert_tail(storage_obj)

    @property
    @abc.abstractmethod
    def worker_ident(self):
        raise NotImplementedError()

    def __eq__(self, other):
        return self.worker_ident == other.worker_ident

    @property
    def storage_item(self):
        return self.w_chain.last_storage_item

    @property
    def file_path(self):
        return self.storage_item.image_path

    @property
    def snapshot_name(self):
        return self.storage_item.ident

    def alloc_resource(self):
        self.w_chain.acquire()

    def free_resource(self):
        self.w_chain.release()

    def set_duplicated(self):
        self.duplicated = True

    def save_work_result(self):
        if self.work_successful:
            storage.query_by_ident(self.storage_item.ident).update_status(m.SnapshotStorage.STATUS_DELETED)
        return self.work_successful


class DeleteFileWork(DeleteWork):
    """删除文件作业

    :remark:
        支持删除 qcow 与 cdp 文件
    """

    def __init__(self, storage_obj: m.SnapshotStorage, call_name: str):
        call_name += f' DeleteFileWork {storage_obj.image_path}'
        super(DeleteFileWork, self).__init__(storage_obj, call_name)
        assert not storage.is_image_path_using(self.file_path)

    def __str__(self):
        return f'delete_file_work:<{self.file_path}>'

    @property
    def worker_ident(self):
        return f'{self.file_path}:delete_file_work'

    def work(self):
        if self.duplicated:
            _logger.info(f'DeleteFileWork duplicated : {self}')
            self.work_successful = True
            return

        try:
            if self.storage_item.is_cdp:
                self.work_successful = action.DiskSnapshotAction.remove_cdp_file(self.file_path)
            else:
                self.work_successful = action.DiskSnapshotAction.remove_qcow_file(self.file_path)
        except Exception as e:
            _logger.warning(lg.format_exception(e))
            self.work_successful = False


class DeleteQcowSnapshotWork(DeleteWork):
    """删除qcow文件中的快照点作业

    :remark:
        该逻辑不负责合并快照点相关数据（例如：hash数据）；在执行该逻辑前，应该保证快照点相关数据已经合并或确实不再需要
    """

    def __init__(self, storage_obj: m.SnapshotStorage, call_name: str):
        call_name += f' DeleteQcowSnapshotWork {storage_obj.ident}'
        super(DeleteQcowSnapshotWork, self).__init__(storage_obj, call_name)
        assert storage_obj.is_qcow
        assert not srm.get_srm().is_storage_writing(storage_obj.image_path)

    def __str__(self):
        return f'delete_qcow_snapshot_work:<{self.file_path}:{self.snapshot_name}>'

    @property
    def worker_ident(self):
        return f'{self.snapshot_name}:{self.file_path}:delete_qcow_snapshot_work'

    def work(self):
        if self.duplicated:
            self.work_successful = True
            _logger.info(f'DeleteQcowSnapshotWork duplicated : {self}')
            return

        try:
            action.DiskSnapshotAction.delete_qcow_snapshot(self.file_path, self.snapshot_name)
            self.work_successful = True
        except Exception as e:
            _logger.warning(lg.format_exception(e))
            self.work_successful = False


class MergeWork(RecyclingWorkBase):
    """合并作业基类

    :remark:
        save_work_result 中需要修改快照存储的依赖关系，使被合并的源节点成为叶子
    """

    def __init__(self, parent_storage_obj: m.SnapshotStorage,
                 children_snapshot_storage_objs: typing.List[m.SnapshotStorage]):
        super(MergeWork, self).__init__()
        self.parent_storage: storage.Storage = storage.Storage(parent_storage_obj) if parent_storage_obj else None
        self.children_snapshot_storage: typing.List[storage.Storage] = [storage.Storage(_)
                                                                        for _ in children_snapshot_storage_objs]
        self.new_storage: storage.Storage = self._create_or_get_new_storage()

    @abc.abstractmethod
    def _create_or_get_new_storage(self) -> storage.Storage:
        raise NotImplementedError()

    @abc.abstractmethod
    def _fill_more_detail_info(self, info_list):
        raise NotImplementedError()

    @staticmethod
    def _generate_storage_ident():
        return uuid.uuid4().hex

    def _update_children_storage_objs(self):
        for child_storage in self.children_snapshot_storage:
            child_storage.update_parent(self.new_storage)

    def msg_when_exception(self, msg) -> str:
        msg_list = [f'{self} failed', msg, ]
        self._fill_detail_info(msg_list)
        self._fill_more_detail_info(msg_list)
        return '\n'.join(msg_list) + '\n'

    def _fill_detail_info(self, info_list):
        info_list.append(f'  parent_storage : {self.parent_storage}')
        info_list.append(f'  new_storage    : {self.new_storage}')
        info_list.append(f'  children_snapshot_storages:')
        for child_storage in self.children_snapshot_storage:
            info_list.append(f'    {child_storage}')


class MergeCdpWork(MergeWork):
    """合并CDP快照存储到新快照点作业"""

    def __init__(self, parent_storage_obj: m.SnapshotStorage,
                 merge_cdp_snapshot_storage_objs: typing.List[m.SnapshotStorage],
                 children_snapshot_storage_objs: typing.List[m.SnapshotStorage],
                 storage_tree: tree.DiskSnapshotStorageTree, call_name: str):
        # 校验入参数据
        assert len(merge_cdp_snapshot_storage_objs) > 0
        assert merge_cdp_snapshot_storage_objs[0].parent_ident == parent_storage_obj.ident
        for child in children_snapshot_storage_objs:
            assert child.parent_ident == children_snapshot_storage_objs[-1].parent_ident
        # 构造
        super(MergeCdpWork, self).__init__(parent_storage_obj, children_snapshot_storage_objs)
        self.merge_cdp_snapshot_storages: typing.List[storage.Storage] = [storage.Storage(_)
                                                                          for _ in merge_cdp_snapshot_storage_objs]
        call_name += f' MergeCdpWork {self.new_storage.ident}'
        self.rw_chain: chain.StorageChainForRW = self._create_rw_chain(storage_tree, call_name)

    def _create_rw_chain(self, storage_tree, call_name) -> chain.StorageChainForRW:
        rw_chain = chain.StorageChainForRW(srm.get_srm(), call_name)
        depend_nodes = storage_tree.fetch_nodes_to_root(self.parent_storage.ident)
        for node in depend_nodes:
            rw_chain.insert_tail(node.storage)
        rw_chain.insert_tail(self.new_storage.storage_obj)
        return rw_chain

    def _create_or_get_new_storage(self) -> storage.Storage:
        assert self.parent_storage
        assert len(self.children_snapshot_storage) > 0
        assert len(self.merge_cdp_snapshot_storages) > 0

        if self.parent_storage.is_cdp:  # 如果父节点为cdp，那么就需要创建新的qcow来存放合并后的数据
            new_image_path = action.ImagePathGenerator.generate_new_qcow(
                os.path.dirname(self.parent_storage.image_path))
        else:  # 如果父节点为qcow，那么就直接将合并后的数据存放到父节点qcow中
            new_image_path = self.parent_storage.image_path

        return storage.create_new_storage(
            self._generate_storage_ident(), self.parent_storage.ident, None, False, self.parent_storage.disk_bytes,
            new_image_path, self.parent_storage.tree_ident)

    def __str__(self):
        return f'merge_cdp_work:<{self.new_storage}>'

    def alloc_resource(self):
        self.rw_chain.acquire()

    def free_resource(self):
        self.rw_chain.release()

    def work(self):
        try:
            raw_flag = action.DiskSnapshotAction.generate_flag(f'{self}')
            hash_version = 0  # TODO hash version
            action.DiskSnapshotAction.merge_cdp_to_qcow(
                self.rw_chain, self.merge_cdp_snapshot_storages, raw_flag, hash_version)

            self.work_successful = True
        except Exception as e:
            self.work_successful = False
            _logger.warning(self.msg_when_exception(f'{e}'))
            _logger.warning(lg.format_exception(e))

    def save_work_result(self):
        if self.work_successful:
            self.new_storage.update_status(m.SnapshotStorage.STATUS_STORAGE)
            self._update_children_storage_objs()
        else:
            self.new_storage.update_status(m.SnapshotStorage.STATUS_ABNORMAL)
        return self.work_successful

    def _fill_more_detail_info(self, info_list):
        info_list.append(f'  merge_cdp_snapshot_storages:')
        for merge_cdp_storage in self.merge_cdp_snapshot_storages:
            info_list.append(f'    {merge_cdp_storage}')


class MergeQcowSnapshotTypeAWork(MergeWork):
    """合并qcow文件中的快照

    :remark:
        合并过程仅涉及单个qcow文件
        没有实体数据搬迁
    """

    def __init__(self, parent_storage_obj: m.SnapshotStorage, merge_storage_obj: m.SnapshotStorage,
                 children_snapshot_storage_objs: typing.List[m.SnapshotStorage]):
        # 校验入参数据
        if parent_storage_obj is None:  # 被合并的快照是根节点
            assert len(children_snapshot_storage_objs) == 1  # 子节点的数量必须为 1
            assert merge_storage_obj.parent_ident is None
        else:
            assert merge_storage_obj.parent_ident == parent_storage_obj.ident
        for child in children_snapshot_storage_objs:
            assert child.parent_ident == merge_storage_obj.parent_ident
        # 构造
        self.merge_storage: storage.Storage = storage.Storage(merge_storage_obj)
        super(MergeQcowSnapshotTypeAWork, self).__init__(parent_storage_obj, children_snapshot_storage_objs)

    @property
    def _is_merge_root_storage(self) -> bool:
        return self.parent_storage is None

    def _create_or_get_new_storage(self):
        assert len(self.children_snapshot_storage) > 0
        return self.parent_storage

    def __str__(self):
        return f'merge_qcow_snapshot_type_a_work:<{self.merge_storage}>'

    def alloc_resource(self):
        pass  # do nothing

    def free_resource(self):
        pass  # do nothing

    def work(self):
        try:
            hash_version = 0  # TODO hash version
            action.DiskSnapshotAction.merge_qcow_hash(self.children_snapshot_storage, self.merge_storage, hash_version)

            self.work_successful = True
        except Exception as e:
            self.work_successful = False
            _logger.warning(self.msg_when_exception(f'{e}'))
            _logger.warning(lg.format_exception(e))

    def save_work_result(self):
        if self.work_successful:
            if self._is_merge_root_storage:
                # 被合并的快照是根节点，防止出现树分裂，将父与子颠倒
                child_storage: storage.Storage = self.children_snapshot_storage[0]
                child_storage.update_parent(None)
            else:
                self._update_children_storage_objs()

        return self.work_successful

    def _fill_more_detail_info(self, info_list):
        info_list.append(f'  merge_storage  : {self.merge_storage}')


class MergeQcowSnapshotTypeBWork(MergeWork):
    """跨qcow文件合并快照

    :remark:
        合并过程涉及两个qcow文件
        实体数据将从一个qcow文件搬迁到另一个qcow文件中
    """

    def __init__(self, parent_storage_obj: m.SnapshotStorage, merge_storage_obj: m.SnapshotStorage,
                 children_snapshot_storage_objs: typing.List[m.SnapshotStorage],
                 storage_tree: tree.DiskSnapshotStorageTree, call_name: str):
        # 校验入参数据
        assert merge_storage_obj.parent_ident == parent_storage_obj.ident
        assert merge_storage_obj.image_path != parent_storage_obj.image_path
        for child in children_snapshot_storage_objs:
            assert child.parent_ident == merge_storage_obj.parent_ident
            assert child.image_path != merge_storage_obj.image_path
        # 构造
        self.merge_storage: storage.Storage = storage.Storage(merge_storage_obj)
        super(MergeQcowSnapshotTypeBWork, self).__init__(parent_storage_obj, children_snapshot_storage_objs)
        call_name += f' MergeQcowSnapshotTypeBWork {self.new_storage.ident}'
        self.write_chain: chain.StorageChainForWrite = self._create_write_chain(storage_tree, call_name)

    def _create_write_chain(self, storage_tree, call_name) -> chain.StorageChainForWrite:
        write_chain = chain.StorageChainForWrite(srm.get_srm(), call_name)
        depend_nodes = storage_tree.fetch_nodes_to_root(self.parent_storage.ident)
        for node in depend_nodes:
            write_chain.insert_tail(node.storage)
        write_chain.insert_tail(self.new_storage.storage_obj)
        return write_chain

    def _create_or_get_new_storage(self):
        assert self.parent_storage
        assert self.parent_storage.is_qcow
        assert self.merge_storage
        assert storage.count_exist_in_file(self.merge_storage.image_path) == 1
        assert len(self.children_snapshot_storage) > 0

        return storage.create_new_storage(
            self._generate_storage_ident(), self.parent_storage.ident, None, False, self.parent_storage.disk_bytes,
            self.parent_storage.image_path, self.parent_storage.tree_ident
        )

    def __str__(self):
        return f'merge_qcow_snapshot_type_b_work:<{self.merge_storage}>'

    def alloc_resource(self):
        self.write_chain.acquire()

    def free_resource(self):
        self.write_chain.release()

    def work(self):
        try:
            raw_flag = action.DiskSnapshotAction.generate_flag(f'{self}')
            hash_version = 0  # TODO hash version

            action.DiskSnapshotAction.move_data_from_qcow(self.merge_storage, self.write_chain, raw_flag, hash_version)

            self.work_successful = True
        except Exception as e:
            self.work_successful = False
            _logger.warning(self.msg_when_exception(f'{e}'))
            _logger.warning(lg.format_exception(e))

    def save_work_result(self):
        if self.work_successful:
            self.new_storage.update_status(m.SnapshotStorage.STATUS_STORAGE)
            self._update_children_storage_objs()
        else:
            self.new_storage.update_status(m.SnapshotStorage.STATUS_ABNORMAL)
        return self.work_successful

    def _fill_more_detail_info(self, info_list):
        info_list.append(f'  merge_storage_obj  : {self.merge_storage}')


class StorageCollection(object):
    """快照存储回收逻辑"""

    TYPE_CDP = 1
    TYPE_QCOW_MOVE_DATA = 2
    TYPE_QCOW_REMOVE = 3

    def __init__(self, tree_ident):
        """
        :param tree_ident:
            存储镜像依赖树的标识
        """
        self.name = f'storage_collection:[{tree_ident}]'
        self.tree_ident = tree_ident

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.__str__()

    def trace_msg(self):
        return self.name

    def collect(self):
        """执行一轮回收逻辑

        :remark:
            同步阻塞
            1. 分析当前 storage 状态，生成执行任务
            2. 执行回收作业，进行数据删除、改写、迁移
            3. 当执行作业成功，标记数据状态为已经删除
        """

        def _alloc_resource():
            if not works:
                return
            for w in works:
                w.alloc_resource()

        def _free_resource():
            if not works:
                return
            for w in works:
                w.free_resource()

        works = None
        try:
            with lm.get_storage_locker(self.trace_msg), s.readonly():
                works = self._analyze_storage_and_create_recycling_works()
                _alloc_resource()

            if works:
                for work in works:
                    work.work()
                return self._save_works_result(works)
            elif works is None:
                pass  # TODO dump and clean data record
            else:
                return False
        finally:
            _free_resource()

    def _save_works_result(self, works):
        work_successful = False
        with lm.get_storage_locker(self.trace_msg), s.transaction():
            for work in works:
                if work.save_work_result():
                    work_successful = True
        return work_successful

    def _analyze_storage_and_create_recycling_works(self) -> typing.Union[typing.List[RecyclingWorkBase], None]:
        """分析存储快照并获取回收存储快照的作业

        1. 从叶子开始查找可直接删除的节点（可删除状态）
            可删除状态： 在叶子且为 STATUS_RECYCLING 的节点意味着可直接删除
            如果节点对应的文件正在使用中，那么就忽略，下次再扫描
            需要向根节点查找尽可能多的 deleting 状态节点，优化删除

        2. 从根向叶子做广度优先遍历，查找可进行合并的节点（可回收状态）
            可回收状态： 为 STATUS_RECYCLING 的非叶子节点意味着可进行合并操作，但需要判断不属于以下情况
                a. 该节点为根节点、且有复数的子节点
                b. 父节点为（存储状态、可回收状态）以外的状态，也就是父节点仅能为这两种状态

                cdp节点：
                a. 该节点为根节点
                b. 父节点为qcow、且所在的文件正在写入中
                c. 有子节点依赖了该节点的某个时刻

                qcow节点：
                a. 该节点有文件级去重

                qcow节点有子节点在其他文件中：
                a. 该节点为根节点
                b. 父节点为cdp
                c. 该节点的磁盘大小与父节点的磁盘大小不同
                d. 有其它节点（父or子）在该节点的同一文件中
                e. 父节点（为qcow）所在的文件正在写入中

                qcow节点没有子节点在其他文件中：
                a. 该节点所在文件正在写入中

        :remark:
            为了优化性能，禁止使用ORM对象去查找父与子，改为使用Node对象查找
        """

        storage_tree = tree.generate(self.tree_ident)
        if storage_tree.is_empty:
            return None

        deleting_storage_objs = self._fetch_deleting_storage_objs(storage_tree)
        if deleting_storage_objs:
            return self._create_delete_works(deleting_storage_objs)  # 生成删除作业

        # 从根向叶子做广度优先遍历，找到可回收的快照存储
        for node in storage_tree.nodes_by_bfs:  # type: tree.StorageNode
            can_merge, merge_type = self._can_disk_snapshot_storage_merge(node)
            if not can_merge:
                continue

            if merge_type == self.TYPE_CDP:
                assert node.storage.is_cdp
                merge_cdp_snapshot_storage_objs = self._fetch_merge_cdp_snapshot_storage_objs(node)
                if merge_cdp_snapshot_storage_objs:
                    return [
                        MergeCdpWork(
                            node.parent.storage, merge_cdp_snapshot_storage_objs,
                            [n.storage_obj for n in node.children], storage_tree, self.name),
                    ]
            elif merge_type == self.TYPE_QCOW_MOVE_DATA:
                return [
                    MergeQcowSnapshotTypeBWork(
                        node.parent.storage_obj, node.storage, [n.storage for n in node.children],
                        storage_tree, self.name),
                ]
            else:
                assert merge_type == self.TYPE_QCOW_REMOVE
                return [
                    MergeQcowSnapshotTypeAWork(
                        self._get_parent_storage_obj_by_node(node),
                        node.storage,
                        [n.storage for n in node.children]),
                ]

        return list()

    def _fetch_deleting_storage_objs(
            self, storage_tree: tree.DiskSnapshotStorageTree) -> typing.List[m.SnapshotStorage]:
        delete_storage_objs = list()
        for leaf in storage_tree.leaves:  # type: tree.StorageNode
            # 从叶子向根深度优先遍历，找到可以直接删除的快照存储
            for node in leaf.fetch_nodes_to_root(False):  # type: tree.StorageNode
                if self._can_disk_snapshot_storage_delete(node):
                    delete_storage_objs.append(node.storage)
                else:
                    break
        return delete_storage_objs

    def _fetch_merge_cdp_snapshot_storage_objs(self, node: tree.StorageNode) -> typing.List[m.SnapshotStorage]:
        merge_cdp_snapshot_storage_objs = list()
        current_node = node

        while True:
            storage_obj = current_node.storage
            assert storage_obj.is_cdp
            merge_cdp_snapshot_storage_objs.append(storage_obj)

            current_node = self._get_child_node_with_cdp_disk_snapshot_storage(current_node)
            if current_node is None:
                break

            # remark： current_node 已经变更, 不可再使用 storage_item
            can_merge, merge_type = self._can_disk_snapshot_storage_merge(node)
            if not can_merge or merge_type != self.TYPE_CDP:
                break

        return merge_cdp_snapshot_storage_objs

    @staticmethod
    def _can_disk_snapshot_storage_delete(node: tree.StorageNode) -> bool:
        storage_obj: m.SnapshotStorage = node.storage

        if storage_obj.status != m.SnapshotStorage.STATUS_RECYCLING:
            return False

        ref_manager = srm.get_srm()

        if ref_manager.is_storage_using(storage_obj.ident):
            return False

        if rt.PathInMount.is_in_not_mount(storage_obj.image_path):
            return False

        if storage_obj.is_qcow and ref_manager.is_storage_writing(storage_obj.image_path):
            return False

        for child_node in node.children:
            if child_node.storage_obj.storage_status != m.SnapshotStorage.STATUS_RECYCLING:
                return False

        return True

    @staticmethod
    def _get_child_node_with_cdp_disk_snapshot_storage(node: tree.StorageNode) -> typing.Union[tree.StorageNode, None]:
        children = node.children
        child_node = None

        for child in children:  # type: tree.StorageNode

            if child.storage.status in (m.SnapshotStorage.STATUS_ABNORMAL, m.SnapshotStorage.STATUS_DELETED):
                continue
            if child.storage.is_qcow:
                continue

            assert child_node is None  # 设计上，CDP子快照仅有一个
            child_node = child

        return child_node

    def _create_delete_works(self, deleting_storage_objs: typing.List[m.SnapshotStorage]) -> typing.List[DeleteWork]:
        works = list()

        def insert_work(_work):
            if _work in works:
                _work.set_duplicated()
            works.append(_work)

        for storage_obj in deleting_storage_objs:
            if storage_obj.is_cdp:
                insert_work(DeleteFileWork(storage_obj, self.name))
            else:
                if storage.is_image_path_using(storage_obj.image_path):
                    insert_work(DeleteQcowSnapshotWork(storage_obj, self.name))
                else:
                    insert_work(DeleteFileWork(storage_obj, self.name))
        return works

    def _can_disk_snapshot_storage_merge(self, node: tree.StorageNode) -> (bool, int):
        if node.is_root and len(node.children) > 1:
            return False, 0  # 不支持：此时如果合并，那么快照树会分裂为两棵树

        if node.is_leaf:
            return False, 0  # 不支持：当前节点为叶子，应该走删除逻辑，而非回收逻辑

        storage_obj = node.storage
        if storage_obj.status != m.SnapshotStorage.STATUS_RECYCLING:
            return False, 0

        parent_storage_obj = self._get_parent_storage_obj_by_node(node)
        if parent_storage_obj and parent_storage_obj in (
                m.SnapshotStorage.STATUS_CREATING, m.SnapshotStorage.STATUS_WRITING,
                m.SnapshotStorage.STATUS_HASHING, m.SnapshotStorage.STATUS_ABNORMAL):
            return False, 0  # 不支持：父快照存储正在生成中

        if rt.PathInMount.is_in_not_mount(storage_obj.image_path):
            return False

        if storage_obj.is_cdp:
            if node.is_root:
                return False, 0  # 不支持：如果cdp在根节点中
            if self._is_child_depend_with_timestamp(node):
                return False, 0  # 不支持：cdp文件的中间有依赖的情况
            if srm.get_srm().is_storage_writing(parent_storage_obj.image_path):
                return False, 0  # 不支持：父快照存储正在写入中
            """可回收"""
            return True, self.TYPE_CDP

        # else is_qcow
        if node.storage.file_level_deduplication:
            return False, 0  # 不支持：带有文件级去重

        if self._is_children_in_other_file(node):
            if node.is_root:
                return False, 0  # 不支持：如果该节点为根节点
            if parent_storage_obj and parent_storage_obj.is_cdp:
                return False, 0  # 不支持：父快照是CDP文件
            if parent_storage_obj.disk_bytes != node.storage.disk_bytes:
                return False, 0  # 不支持：父快照是CDP文件
            if self._is_multi_snapshot_in_the_qcow(node):
                return False, 0  # 不支持：有其他快照点在该qcow文件中
            if srm.get_srm().is_storage_writing(parent_storage_obj.image_path):
                return False, 0  # 不支持：父快照存储正在写入中
            """可回收"""
            return True, self.TYPE_QCOW_MOVE_DATA

        # else not is_children_in_other_file
        if srm.get_srm().is_storage_writing(node.storage.image_path):
            return False, 0  # 不支持：快照存储所在文件正在写入中
        """可回收"""
        return True, self.TYPE_QCOW_REMOVE

    @staticmethod
    def _is_child_depend_with_timestamp(node: tree.StorageNode):
        for child in node.children:
            storage_obj = child.storage_obj
            if storage_obj.parent_timestamp is not None:
                return True
        else:
            return False

    @staticmethod
    def _is_children_in_other_file(node: tree.StorageNode):
        storage_obj = node.storage
        for child in node.children:
            if storage_obj.image_path != child.storage_obj.image_path:
                return True
        else:
            return False

    @staticmethod
    def _is_multi_snapshot_in_the_qcow(node: tree.StorageNode):
        assert not node.is_root
        if node.parent.storage_obj.image_path == node.storage.image_path:
            return True
        for child in node.children:
            if child.storage_obj.image_path == node.storage.image_path:
                return True
        else:
            return False

    @staticmethod
    def _get_parent_storage_obj_by_node(node: tree.StorageNode) -> typing.Union[m.SnapshotStorage, None]:
        return None if node.is_root else node.parent.storage
