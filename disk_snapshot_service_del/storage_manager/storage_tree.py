from anytree import LevelOrderIter
from anytree import Node
from anytree import find
from storage_manager import models as m


class DiskSnapshotStorageNode(Node):
    """快照存储树节点

    :remark:
        每节点对应磁盘快照存储的数据库对象
        数据库对象的数据库id作为节点的name
    """

    def __init__(self, disk_snapshot_storage_obj):
        super(DiskSnapshotStorageNode, self).__init__(name=str(disk_snapshot_storage_obj.id))
        self.storage_obj = disk_snapshot_storage_obj


class DiskSnapshotStorageTree:
    """磁盘快照存储树

    :remark:
        将关联的快照存储的数据库对象缓存到内存中，提高性能
    """

    def __init__(self, query_set):
        self.root_node = None
        self._init_root(query_set)

    def _init_root(self, query_set):
        """将数据库对象转换为树节点对象，并加入树中

        :param query_set:
            QuerySet 有关联的磁盘快照存储的数据库查询对象
        """
        node_dict = dict()
        for db_obj in query_set.all():
            node_dict[db_obj.id] = DiskSnapshotStorageNode(db_obj)

        for index, node in node_dict.items():
            parent_index = node.storage_obj.parent_snapshot_id
            if parent_index:
                node.parent = node_dict[parent_index]
            else:
                assert self.root_node is None
                self.root_node = node

    def is_empty(self) -> bool:
        return self.root_node is None

    @property
    def leaves(self):
        if self.root_node is None:
            return
        for leaf in self.root_node.leaves:
            yield leaf

    @property
    def nodes_by_bfs(self):
        if self.root_node is None:
            return
        for node in LevelOrderIter(self.root_node):  # 广度优先
            yield node

    def get_node_by_storage_obj(self, storage_obj) -> DiskSnapshotStorageNode:
        assert self.root_node is not None
        return find(
            self.root_node,
            lambda node: node.storage_obj.disk_snapshot_storage_ident == storage_obj.disk_snapshot_storage_ident)

    @staticmethod
    def create_instance_by_storage_root(storage_root_obj):
        return DiskSnapshotStorageTree(m.DiskSnapshotStorage.valid_storage_objs(storage_root_obj))


def dfs_to_root(node: DiskSnapshotStorageNode):
    """从节点向根做遍历

    :param node:
        快照存储树节点，支持 None
    """
    while True:
        if node is not None:
            yield node
        else:
            break
        node = node.parent
