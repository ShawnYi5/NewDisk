import decimal
import threading
import time
import typing

import arrow
from dateutil import tz


def convert_timestamp_float_to_decimal(timestamp: float) -> decimal.Decimal:
    return decimal.Decimal(round(timestamp, 6))


def current_timestamp() -> decimal.Decimal:
    return convert_timestamp_float_to_decimal(time.time())


def current_timestamp_float() -> float:
    return round(time.time(), 6)


def humanize_timestamp(timestamp: typing.Union[decimal.Decimal, None], empty_str='') -> str:
    """格式化时间戳为人可读的描述"""
    if not timestamp:
        return empty_str

    return arrow.Arrow.fromtimestamp(timestamp, tz.tzlocal()).format('YYYY-MM-DD HH:mm:ss.SSSSSS')


UNIQUE_NUMBER_STORAGE_CHAIN = 0
UNIQUE_NUMBER_DESTROY_JOURNAL = 1
UNIQUE_ICE_OP_INDEX = 2

_unique_number_cache = [
    [threading.Lock(), 0], [threading.Lock(), 0], [threading.Lock(), 0],
]


def generate_unique_number(index: int) -> int:
    with _unique_number_cache[index][0]:
        _unique_number_cache[index][1] += 1
        return _unique_number_cache[index][1]


class DataHolder(object):
    def __init__(self, value=None):
        self.value = value

    def set(self, value):
        self.value = value
        return value

    def get(self):
        return self.value
