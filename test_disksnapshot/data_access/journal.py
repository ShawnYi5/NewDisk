from cpkt.core import xlogging as lg

# from basic_library import xfunctions as xf
from data_access import models as m
from data_access import session as s

_logger = lg.get_logger(__name__)


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
