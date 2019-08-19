import threading

from cpkt.core import xlogging as lg

_logger = lg.get_logger(__name__)


class LockWithTrace(object):

    def __init__(self, name):
        self.name = name
        self._locker = threading.RLock()
        self._current_trace = list()

    def acquire(self, trace):
        self._locker.acquire()
        try:
            if not self._current_trace:
                _logger.debug(f'locker {self.name} acquire : {trace}')
            self._current_trace.append(trace)
        except Exception:
            self._locker.release()
            raise
        return self

    def release(self):
        try:
            trace = self._current_trace.pop(-1)
            if not self._current_trace:
                _logger.debug(f'locker {self.name} release : {trace}')
        finally:
            self._locker.release()

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

    @property
    def current_trace(self):
        return ' # '.join(self._current_trace)


_locker_manager = None
_locker_manager_locker = threading.Lock()


class LockerManager(object):
    def __init__(self):
        self._locker_dict = {
            'journal': LockWithTrace('journal'),
            'storage': LockWithTrace('storage'),
        }

    @staticmethod
    def get_locker_manager():
        global _locker_manager

        if _locker_manager is None:
            with _locker_manager_locker:
                if _locker_manager is None:
                    _locker_manager = LockerManager()
        return _locker_manager

    def get_locker(self, key, trace) -> LockWithTrace:
        """获取锁对象"""
        return self._locker_dict[key].acquire(trace)


def get_journal_locker(trace) -> LockWithTrace:
    """获取日志表锁对象"""
    return LockerManager.get_locker_manager().get_locker('journal', trace)


def get_storage_locker(trace) -> LockWithTrace:
    """获取快照存储表锁对象"""
    return LockerManager.get_locker_manager().get_locker('storage', trace)
