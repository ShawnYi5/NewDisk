import threading
import typing

from cpkt.core import exc
from cpkt.core import xlogging as lg

from basic_library import xfunctions as xf
from business_logic import storage_action as action
from business_logic import storage_chain as chain

_logger = lg.get_logger(__name__)


class Handle(object):
    """句柄对象"""

    def __init__(self, handle, writing, raw_flag):
        self.handle: str = handle
        self.writing: bool = writing
        self.raw_flag: str = raw_flag
        self.storage_chain: chain.StorageChain = None
        self.raw_handle: int = 0
        self.ice_endpoint: str = ''
        self.created_time = xf.current_timestamp()
        self.locker = threading.Lock()

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return (f'{self.handle} | {self.raw_handle} | {self.ice_endpoint} | '
                f'{xf.humanize_timestamp(self.created_time)} | {self.writing} | '
                f'{self.storage_chain.name if self.storage_chain else None}')

    def _close_raw_handle(self):
        with self.locker:
            if self.raw_handle:
                action.DiskSnapshotAction.close_disk_snapshot(self.raw_handle, self.ice_endpoint)

    def _release_chain(self):
        try:
            if self.storage_chain:
                self.storage_chain.release()
        except Exception as e:
            _logger.warning(lg.format_exception(e))

    def destroy(self):
        HandlePool.get_handle_pool().remove(self.handle)
        try:
            self._close_raw_handle()
        finally:
            self._release_chain()


_handle_pool = None
_handle_pool_locker = threading.Lock()


class HandlePool(object):
    """handle 管理器"""

    def __init__(self):
        self.cache: typing.Dict[str, Handle] = dict()
        self.cache_locker = threading.Lock()

    @staticmethod
    def get_handle_pool():
        global _handle_pool

        if _handle_pool is None:
            with _handle_pool_locker:
                if _handle_pool is None:
                    _handle_pool = HandlePool()
        return _handle_pool

    def insert(self, handle_inst: Handle) -> Handle:
        with self.cache_locker:
            assert handle_inst.handle not in self.cache, (
                '添加句柄失败，重复的快照存储操作句柄', f'same handle in pool {self.cache[handle_inst.handle]}', 0)
            self.cache[handle_inst.handle] = handle_inst
            _logger.info(f'insert handle [{handle_inst.handle}] to pool')
            return handle_inst

    def remove(self, handle: str) -> Handle:
        with self.cache_locker:
            handle_inst = self.cache.pop(handle, None)
            if handle_inst:
                _logger.info(f'remove {handle_inst} from pool')
            else:
                _logger.warning(f'handle {handle} NOT in pool')
            return handle_inst

    def get(self, handle: str) -> Handle:
        with self.cache_locker:
            handle_inst = self.cache.get(handle, None)
            if not handle_inst:
                _logger.warning(f'handle {handle} NOT in pool')
            return handle_inst


def generate_handle(handle: str, writing: bool, raw_flag: str) -> Handle:
    """产生新的Handle对象，并将其加入到 pool 中"""
    return HandlePool.get_handle_pool().insert(Handle(handle, writing, raw_flag))


def get_handle(handle: str, raise_except: bool) -> Handle:
    """获取Handle对象"""
    result = HandlePool.get_handle_pool().get(handle)
    if not result and raise_except:
        raise exc.generate_exception_and_logger('快照存储操作句柄不存在', f'handle {handle} NOT in pool', 0)
    return result
