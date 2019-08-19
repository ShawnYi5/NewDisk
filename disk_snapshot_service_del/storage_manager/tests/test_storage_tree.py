from unittest.mock import MagicMock

from storage_manager import storage_tree as tree


def test_empty_tree():
    empty_query_set = MagicMock()
    empty_query_set.all = MagicMock(return_value=list())

    storage_tree = tree.DiskSnapshotStorageTree(empty_query_set)

    for _ in storage_tree.leaves:
        assert False, 'never run'

    for _ in storage_tree.nodes_by_bfs:
        assert False, 'never run'
