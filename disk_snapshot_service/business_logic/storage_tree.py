import typing

from cpkt.core import exc
from cpkt.core import xlogging as lg

import anytree
from data_access import models as m
from data_access import storage as da_storage

_logger = lg.get_logger(__name__)


class StorageNode(anytree.Node):
    """真实存在的磁盘快照存储对象树节点"""

    def __init__(self, s: m.SnapshotStorage):
        super(StorageNode, self).__init__(name=s.ident)
        self._storage: m.SnapshotStorage = s

    @property
    def ident(self) -> str:
        return self.name

    @property
    def storage(self) -> m.SnapshotStorage:
        return self._storage

    def fetch_nodes_to_root(self, root_to_node: bool = True) -> typing.List['StorageNode']:
        result = list()
        node = self
        while node:
            result.append(node)
            node = node.parent
        if root_to_node:
            result.reverse()
        return result


class DiskSnapshotStorageTree(object):
    """磁盘快照存储对象树

    :remark:
        将关联的"磁盘快照存储对象"缓存到内存中，提高性能
    """

    def __init__(self, tree_ident: str):
        """必须使用create_tree_inst创建实例"""
        self.tree_ident: str = tree_ident
        self.root_node: StorageNode = None
        self.node_dict: typing.Dict[str, StorageNode] = dict()

    @staticmethod
    def create_tree(tree_ident: str) -> 'DiskSnapshotStorageTree':
        """tree_ident所关联的有效快照存储节点，生成树"""

        storage_tree = DiskSnapshotStorageTree(tree_ident)
        storage_objs = da_storage.query_valid_objs(tree_ident)
        return storage_tree.__init_root(storage_objs)

    def __init_root(self, storage_objs):
        """磁盘快照存储数据库对象转换为树节点对象，并加入树中"""

        for obj in storage_objs:  # type: m.SnapshotStorage
            self.node_dict[obj.ident] = StorageNode(obj)
        for ident, node in self.node_dict.items():
            parent_ident = node.storage.parent_ident
            if parent_ident:
                node.parent = self.node_dict[parent_ident]
            else:
                assert self.root_node is None, ('磁盘快照存储树分裂', f'not one root {self.root_node} and {node}', 0)
                self.root_node = node
        return self

    @property
    def is_empty(self) -> bool:
        return self.root_node is None

    @property
    def nodes_by_bfs(self):
        if self.is_empty:
            return
        for node in anytree.LevelOrderIter(self.root_node):  # 广度优先
            yield node

    @property
    def leaves(self):
        if self.is_empty:
            return
        for leaf in self.root_node.leaves:
            yield leaf

    def get_node_by_ident(self, ident: str) -> StorageNode:
        return self.node_dict[ident]

    def fetch_nodes_to_root(self, ident: str, root_to_node: bool = True) -> typing.List[StorageNode]:
        node = self.get_node_by_ident(ident)
        return node.fetch_nodes_to_root(root_to_node)


def generate(tree_ident) -> DiskSnapshotStorageTree:
    """生成快照存储树"""

    try:
        return DiskSnapshotStorageTree.create_tree(tree_ident)
    except AssertionError:
        raise
    except KeyError as e:
        _logger.error(lg.format_exception(e))
        raise exc.generate_exception_and_logger(
            '磁盘快照存储节点分裂', f'generate tree failed with KeyError {e}', 0)


def check(tree_ident):
    """检测快照存储树的基本依赖关系，事实上就是重新生成一次树"""

    generate(tree_ident)
