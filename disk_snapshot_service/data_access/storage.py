import typing

from cpkt.core import xlogging as lg

from data_access import models as m
from data_access import session as s

_logger = lg.get_logger(__name__)


def query_valid_objs(tree_ident):
    """获取有效的快照存储"""

    return (s.get_scoped_session().query(m.SnapshotStorage)
            .filter(m.SnapshotStorage.tree_ident == tree_ident)
            .filter(m.SnapshotStorage.status != m.SnapshotStorage.STATUS_DELETED)
            .all()
            )


def get_obj_by_ident(storage_ident) -> m.SnapshotStorage:
    """获取指定快照存储"""

    return s.get_scoped_session().query(m.SnapshotStorage).filter(m.SnapshotStorage.ident == storage_ident).first()


def create_obj(storage_ident, parent_ident, parent_timestamp, storage_type, disk_bytes, status, image_path, tree_ident):
    new_storage_obj = m.SnapshotStorage(
        ident=storage_ident,
        parent_ident=parent_ident,
        parent_timestamp=parent_timestamp,
        type=storage_type,
        disk_bytes=disk_bytes,
        status=status,
        image_path=image_path,
        tree_ident=tree_ident,
    )
    session = s.get_scoped_session()
    session.add(new_storage_obj)
    session.flush()
    return new_storage_obj


# 状态转移定义，{目标状态 : (源状态1，源状态2，……)}
_status_transition = {
    m.SnapshotStorage.STATUS_ABNORMAL: (m.SnapshotStorage.STATUS_CREATING, m.SnapshotStorage.STATUS_WRITING,
                                        m.SnapshotStorage.STATUS_HASHING, m.SnapshotStorage.STATUS_STORAGE,
                                        m.SnapshotStorage.STATUS_RECYCLING,),
    m.SnapshotStorage.STATUS_WRITING: (m.SnapshotStorage.STATUS_CREATING,),
    m.SnapshotStorage.STATUS_HASHING: (m.SnapshotStorage.STATUS_WRITING,),
    m.SnapshotStorage.STATUS_STORAGE: (m.SnapshotStorage.STATUS_HASHING,),
    m.SnapshotStorage.STATUS_RECYCLING: (m.SnapshotStorage.STATUS_STORAGE,),
    m.SnapshotStorage.STATUS_DELETED: (m.SnapshotStorage.STATUS_RECYCLING, m.SnapshotStorage.STATUS_ABNORMAL,),
}


def update_obj_status(storage_obj: m.SnapshotStorage, new_status) -> m.SnapshotStorage:
    if storage_obj.status == new_status:
        return storage_obj

    assert storage_obj.status in _status_transition[new_status], (
        '快照存储转移状态无效',
        f'update snapshot [{storage_obj}] status failed, want to <{m.SnapshotStorage.format_status(new_status)}>', 0)
    storage_obj.status = new_status
    s.get_scoped_session().flush()
    return storage_obj


def update_obj_parent(storage_obj: m.SnapshotStorage,
                      parent_storage_obj: typing.Union[m.SnapshotStorage, None]) -> m.SnapshotStorage:
    _logger.info(f'alter [{storage_obj}] parent to <{parent_storage_obj}>')
    storage_obj.parent_ident = parent_storage_obj.ident if parent_storage_obj else None
    s.get_scoped_session().flush()
    return storage_obj


def update_obj_values(storage_obj: m.SnapshotStorage, values: dict) -> m.SnapshotStorage:
    for k, v in values.items():
        setattr(storage_obj, k, v)
    s.get_scoped_session().flush()
    return storage_obj


def query_image_path_using_count(image_path: str) -> int:
    return (s.get_scoped_session().query(m.SnapshotStorage)
            .filter(m.SnapshotStorage.image_path == image_path)
            .filter(m.SnapshotStorage.status.notin_((m.SnapshotStorage.STATUS_DELETED,
                                                     m.SnapshotStorage.STATUS_RECYCLING,
                                                     )))
            .count()
            )


def query_image_path_exist_count(image_path: str) -> int:
    return (s.get_scoped_session().query(m.SnapshotStorage)
            .filter(m.SnapshotStorage.image_path == image_path)
            .filter(m.SnapshotStorage.status != m.SnapshotStorage.STATUS_DELETED)
            .count()
            )
