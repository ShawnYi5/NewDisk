import json
import typing

from cpkt.core import xlogging as lg
from cpkt.data import define as dd

from business_logic import locker_manager as lm
from business_logic import storage
from data_access import journal as da_journal
from data_access import models as m
from data_access import session as s
from data_access import storage as da_storage

_logger = lg.get_logger(__name__)


class Journal(object):

    def __init__(self, journal_obj):
        self.journal_obj: m.Journal = journal_obj

    @property
    def consumed(self) -> bool:
        return self.journal_obj.consumed_timestamp is not None

    @property
    def token(self) -> str:
        return self.journal_obj.token

    def consume(self):
        return da_journal.consume(self.journal_obj)


J = typing.TypeVar('J', bound=Journal)


class DestroyInJournal(Journal):

    def __init__(self, journal_obj):
        super(DestroyInJournal, self).__init__(journal_obj)
        assert journal_obj.operation_type == m.Journal.TYPE_DESTROY, (
            '磁盘快照日志类型错误', f'journal type NOT TYPE_DESTROY {journal_obj.token}', 0)
        self.destroy = json.loads(journal_obj.operation_str)

    @property
    def idents(self) -> typing.List[str]:
        return self.destroy['idents'].split(',')


class CreateInJournal(Journal):

    def __init__(self, journal_obj):
        super(CreateInJournal, self).__init__(journal_obj)
        assert journal_obj.operation_type == m.Journal.TYPE_CREATE, (
            '磁盘快照日志类型错误', f'journal type NOT TYPE_NORMAL_CREATE {journal_obj.token}', 0)
        self.normal_create = json.loads(journal_obj.operation_str)

    @property
    def new_ident(self):
        return self.normal_create['new_ident']

    @property
    def parent_ident(self):
        return self.normal_create.get('parent_ident', None)

    @property
    def parent_timestamp(self):
        return self.normal_create.get('parent_timestamp', None)

    @property
    def new_type(self):
        return self.normal_create['new_type']

    @property
    def new_storage_folder(self):
        return self.normal_create['new_storage_folder']

    @property
    def new_disk_bytes(self):
        return self.normal_create['new_disk_bytes']

    @property
    def new_hash_version(self):
        return self.normal_create.get('new_hash_version', None)  # 更新获取hash类型的代码

    @property
    def is_root(self):
        return self.parent_ident is None

    @property
    def is_cdp(self):
        return self.new_type == dd.DiskSnapshotService.STORAGE_TYPE_CDP

    @property
    def is_qcow(self):
        return self.new_type == dd.DiskSnapshotService.STORAGE_TYPE_QCOW

    @property
    def children_storages(self) -> typing.List[storage.Storage]:
        if not self.journal_obj.children_idents:
            return list()
        return [storage.Storage(da_storage.get_obj_by_ident(ident))
                for ident in self.journal_obj.children_idents.split(',')]

    def append_child_storage_ident(self, storage_ident: str):
        if self.journal_obj.children_idents:
            idents = self.journal_obj.children_idents.split(',')
        else:
            idents = list()
        idents.append(storage_ident)
        da_journal.alter_children(self.journal_obj, ','.join(idents))

    def query_parent_in_journals(
            self, unconsumed: typing.List['CreateInJournal']) -> typing.Union['CreateInJournal', None]:
        for jn in unconsumed:
            if jn.new_ident == self.parent_ident:
                return jn
        else:
            return None


_journal_sub_class = {
    m.Journal.TYPE_CREATE: CreateInJournal,
    m.Journal.TYPE_DESTROY: DestroyInJournal,
}


def _generate_journal_inst(journal_obj):
    return _journal_sub_class[journal_obj.operation_type](journal_obj)


def consume(token: str, trace_msg: str, return_class: typing.Type[J]) -> J:
    """消费日志

    :param token:
    :param trace_msg:
    :param return_class:
    :return: return_class类型的实例
    """

    with lm.get_journal_locker(trace_msg), s.transaction():
        journal_obj = s.get_scoped_session().query(m.Journal).filter(m.Journal.token == token).first()
        assert journal_obj, ('磁盘快照日志令牌不存在', f'journal token [{token}] not exist', 0)
        assert not journal_obj.consumed_timestamp, ('磁盘快照日志已被消费', f'journal has consumed {token}', 0)
        da_journal.consume(journal_obj)
        return return_class(journal_obj)


def query_unconsumed(before_journal_obj: m.Journal = None) -> typing.List[Journal]:
    journal_objs = da_journal.query_unconsumed_objs(before_journal_obj=before_journal_obj)
    return [_generate_journal_inst(o) for o in journal_objs]


def query_unconsumed_create(before_journal_obj: m.Journal = None) -> typing.List[CreateInJournal]:
    journal_objs = da_journal.query_unconsumed_objs(
        journal_type=m.Journal.TYPE_CREATE, before_journal_obj=before_journal_obj)
    return [_generate_journal_inst(o) for o in journal_objs]


create = da_journal.create_obj
