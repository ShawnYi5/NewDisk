from cpkt.core import xlogging as lg

import interface_data_define as idd
from business_logic import journal
from data_access import models as m
from data_access import session as s

_logger = lg.get_logger(__name__)


def for_create(params: idd.GenerateJournalForCreateParams):
    with s.transaction():
        journal.create(
            params.journal_token,
            idd.JournalForCreateSchema().dumps(params).data,
            m.Journal.TYPE_CREATE
        )


def for_destroy(params: idd.GenerateJournalForDestroyParams):
    with s.transaction():
        journal.create(
            params.journal_token,
            idd.JournalForDestroySchema().dumps(params).data,
            m.Journal.TYPE_DESTROY
        )
