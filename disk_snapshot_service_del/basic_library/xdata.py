ERROR_HTTP_STATUS_DEFAULT = 555
ERROR_HTTP_STATUS_VALIDATION_ERROR = 556

ERROR_FAILED = 5001
ERROR_DELETE_DISK_SNAPSHOT_FAILED = 5002
ERROR_QUERY_CDP_FILE_TIMESTAMP_RANGE_FAILED = 5003


class DSSException(Exception):
    """
    在Dashboard模块中产生的异常

    :remark:
        所有自有代码产生的异常都必须从该类派生
    """

    def __init__(self, function_name, msg, debug, file_line, http_status, is_log=False):
        super(DSSException, self).__init__(msg)
        self.function_name = function_name  # 产生异常的方法名，建议使用__qualname__获取
        self.msg = msg  # 异常描述，供用户浏览
        self.debug = debug  # 异常调试，供开发人员排错
        self.file_line = file_line  # 发生异常的行号
        self.http_status = http_status  # web api 返回的 http status
        self.is_log = is_log


class UserCancelException(DSSException):
    pass


class StorageLockerNotExist(DSSException):
    pass


class StorageLockerRepeatGet(DSSException):
    pass


class HostSnapshotInvalid(DSSException):
    pass


class DiskSnapshotStorageInvalid(DSSException):
    pass


class StorageReferenceRepeated(DSSException):
    pass


class StorageDirectoryInvalid(DSSException):
    pass


class TaskIndentDuplicate(DSSException):
    pass


class StorageImageFileNotExist(DSSException):
    pass
