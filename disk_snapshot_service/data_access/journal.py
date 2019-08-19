from cpkt.core import xlogging as lg

from basic_library import xfunctions as xf
from data_access import models as m
from data_access import session as s

_logger = lg.get_logger(__name__)


def consume(journal_obj):
    assert not journal_obj.consumed_timestamp, ('磁盘快照日志已被消费', f'journal has consumed {journal_obj}', 0)
    journal_obj.consumed_timestamp = xf.current_timestamp()
    s.get_scoped_session().flush()
    _logger.info(f'journal consumed : {journal_obj}')
    return journal_obj


def query_unconsumed_objs(journal_type=None, before_journal_obj: m.Journal = None):
    q = s.get_scoped_session().query(m.Journal).filter(m.Journal.consumed_timestamp.is_(None))
    if journal_type:
        q = q.filter(m.Journal.operation_type == journal_type)
    if before_journal_obj:
        q = q.filter(m.Journal.id < before_journal_obj.id)
    return q.order_by(m.Journal.id).all()


def alter_children(journal_obj, new_value: str):
    old_value = journal_obj.children_idents
    journal_obj.children_idents = new_value
    s.get_scoped_session().flush()
    _logger.info(f'change <{journal_obj}> children from [{old_value}] to [{new_value}]')


def create_obj(token: str, operation_str: str, operation_type: str):
    new_journal_obj = m.Journal(
        token=token,
        operation_str=operation_str,
        operation_type=operation_type,
        produced_timestamp=xf.current_timestamp_float(),
    )
    session = s.get_scoped_session()
    session.add(new_journal_obj)
    session.flush()
    _logger.info(f'create <{new_journal_obj}>')
    return new_journal_obj
