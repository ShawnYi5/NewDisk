import threading

from basic_library import rwlock
from basic_library import xlogging

_logger = xlogging.getLogger(__name__)

_task_container = None
_task_container_locker = threading.Lock()


class TaskContainer(object):
    """任务容器

    :remark:
        存储所有对外接口的任务对象
    """

    @staticmethod
    def get_task_container():
        global _task_container

        if _task_container is None:
            with _task_container_locker:
                if _task_container is None:
                    _task_container = TaskContainer()
        return _task_container

    def __init__(self):
        """
        :var self.tasks
            存放所有的任务
                key为任务类型
                value为dict，其key为任务标识符
        :var self.locker
            锁对象，访问/修改self.tasks前必须进入该锁的临界区
        """
        self.tasks = dict()
        self.locker = rwlock.RWLockWrite()

    def add_task(self, task_type: str, task_ident: str, task_item, item_can_dup=False):
        with self.locker.gen_wlock():
            if task_type not in self.tasks:
                self.tasks[task_type] = dict()

            assert task_ident not in self.tasks[task_type]

            if not item_can_dup:
                for _item in self.tasks[task_type].values():
                    if _item == task_item:
                        return False

            self.tasks[task_type][task_ident] = task_item
            return True

    def remove_task(self, task_type: str, task_ident: str, release_item=False):
        with self.locker.gen_wlock():
            if task_type not in self.tasks:
                _logger.warning(f'not exist task_type : {task_type}')
                return None

            task_item = self.tasks[task_type].pop(task_ident, None)
            if not task_item:
                _logger.warning(f'not exist task_type : {task_type}')
                return None

        if release_item:
            task_item.release()

        return task_item

    def remove_task_with_prefix_ident(self, task_type: str, task_prefix_ident: str, release_item=False):
        with self.locker.gen_wlock():
            if task_type not in self.tasks:
                _logger.warning(f'not exist task_type : {task_type}')
                return None

            need_remove_task = [
                (task_ident, task_item) for task_ident, task_item in self.tasks[task_type].items()
                if task_ident.startswith(task_prefix_ident)]

            for task_ident, _ in need_remove_task:
                self.tasks.pop(task_ident)

        if release_item:
            for _, task_item in need_remove_task:
                task_item.release()

        return [v for _, v in need_remove_task]
