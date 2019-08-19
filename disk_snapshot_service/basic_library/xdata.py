from cpkt.core import exc

ERROR_HTTP_STATUS_DEFAULT = 555
ERROR_HTTP_STATUS_VALIDATION_ERROR = 556

ERROR_FAILED = 5001
ERROR_DELETE_DISK_SNAPSHOT_FAILED = 5002
ERROR_QUERY_CDP_FILE_TIMESTAMP_RANGE_FAILED = 5003


class UserCancelException(exc.CpktException):
    pass


class StorageLockerNotExist(exc.CpktException):
    pass


class StorageLockerRepeatGet(exc.CpktException):
    pass


class HostSnapshotInvalid(exc.CpktException):
    pass


class DiskSnapshotStorageInvalid(exc.CpktException):
    pass


class StorageReferenceRepeated(exc.CpktException):
    pass


class StorageDirectoryInvalid(exc.CpktException):
    pass


class TaskIndentDuplicate(exc.CpktException):
    pass


class StorageImageFileNotExist(exc.CpktException):
    pass
