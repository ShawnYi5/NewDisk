import abc

from basic_library import xlogging

_logger = xlogging.getLogger(__name__)


class TaskItem(abc.ABC):
    """任务元素抽象类

    :remark:
        acquire方法与release方法配对使用
        acquire方法调用后，在调用release方法之前，不可重入
    """

    def __init__(self):
        self._valid = False

    @abc.abstractmethod
    @property
    def name(self):
        raise NotImplementedError()

    def __del__(self):
        if self._valid:
            _logger.warning(f'{self.name} NOT call release')
            self.release()

    @abc.abstractmethod
    def release(self):
        _ = self._valid
        self._valid = False
        return _

    @abc.abstractmethod
    def acquire(self):
        assert not self._valid
        self._valid = True
