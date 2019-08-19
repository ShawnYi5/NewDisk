from cpkt.data import define as dd
from marshmallow import Schema, fields, post_load, validate

Length = validate.Length


class EmptySchema(object):

    def loads(self, *args, **kwargs):
        _ = args
        _ = kwargs
        return None, None

    def dumps(self, *args, **kwargs):
        _ = args
        _ = kwargs
        return '{}', None


class CreateSnapshotParams(object):
    def __init__(self, handle, journal_token, caller_trace, caller_pid, caller_pid_created):
        self.handle = handle
        self.journal_token = journal_token
        if caller_trace:
            self.caller_trace = caller_trace
        else:
            self.caller_trace = f'create snapshot trace {journal_token}'
        self.caller_pid = caller_pid
        self.caller_pid_created = caller_pid_created


class CreateSnapshotParamsSchema(Schema):
    handle = fields.String(required=True, validate=Length(max=32))
    journal_token = fields.String(required=True, validate=Length(max=32))
    caller_trace = fields.String(missing=None)
    caller_pid = fields.Integer(required=True)
    caller_pid_created = fields.Integer(required=True)

    @post_load
    def make_params(self, data):
        return CreateSnapshotParams(**data)


class CreateSnapshotResultSchema(Schema):
    raw_handle = fields.Integer()
    ice_endpoint = fields.String()


class CloseSnapshotParams(object):
    def __init__(self, handle):
        self.handle = handle


class CloseSnapshotParamsSchema(Schema):
    handle = fields.String(required=True, validate=Length(max=32))

    @post_load
    def make_params(self, data):
        return CloseSnapshotParams(**data)


class OpenSnapshotParams(object):
    def __init__(self, handle, caller_trace, caller_pid, caller_pid_created, storage_ident, timestamp, open_raw_handle):
        self.handle = handle
        if caller_trace:
            self.caller_trace = caller_trace
        else:
            self.caller_trace = f'open snapshot trace {handle}'
        self.caller_pid = caller_pid
        self.caller_pid_created = caller_pid_created
        self.storage_ident = storage_ident
        self.timestamp = timestamp
        self.open_raw_handle = open_raw_handle


class OpenSnapshotParamsSchema(Schema):
    handle = fields.String(required=True, validate=Length(max=32))
    caller_trace = fields.String(missing=None)
    caller_pid = fields.Integer(required=True)
    caller_pid_created = fields.Integer(required=True)
    storage_ident = fields.String(required=True, validate=Length(max=32))
    timestamp = fields.Decimal(missing=None)
    open_raw_handle = fields.Boolean(missing=False)

    @post_load
    def make_params(self, data):
        return OpenSnapshotParams(**data)


class OpenSnapshotResultSchema(Schema):
    raw_handle = fields.Integer()
    ice_endpoint = fields.String()


class GenerateJournalForCreateParams(object):
    def __init__(self, journal_token, new_ident, parent_ident, parent_timestamp, new_type,
                 new_storage_folder, new_disk_bytes, new_hash_version):
        self.journal_token = journal_token
        self.new_ident = new_ident
        self.parent_ident = parent_ident
        self.parent_timestamp = parent_timestamp
        self.new_type = new_type
        self.new_storage_folder = new_storage_folder
        self.new_disk_bytes = new_disk_bytes
        self.new_hash_version = new_hash_version


VALID_STORAGE_TYPE_ = (
    # new_type 的枚举：
    # from cpkt.data import define as dd
    # dd.DiskSnapshotService.STORAGE_TYPE_xxx
    dd.DiskSnapshotService.STORAGE_TYPE_QCOW,
    dd.DiskSnapshotService.STORAGE_TYPE_CDP,
)


class GenerateJournalForCreateParamsSchema(Schema):
    journal_token = fields.String(required=True, validate=Length(max=32))
    new_ident = fields.String(required=True, validate=Length(max=32))
    parent_ident = fields.String(missing=None, validate=Length(max=32))
    parent_timestamp = fields.Decimal(missing=None)
    new_type = fields.String(required=True, validate=lambda _: _ in VALID_STORAGE_TYPE_)
    new_storage_folder = fields.String(required=True)
    new_disk_bytes = fields.Integer(required=True)
    new_hash_version = fields.Integer(missing=1)

    @post_load
    def make_params(self, data):
        return GenerateJournalForCreateParams(**data)


class GenerateJournalForDestroyParams(object):
    def __init__(self, journal_token, idents):
        self.journal_token = journal_token
        self.idents = ','.join(idents)


class GenerateJournalForDestroyParamsSchema(Schema):
    journal_token = fields.String(required=True, validate=Length(max=32))
    idents = fields.List(fields.String(validate=Length(max=32)), required=True, validate=Length(min=1))

    @post_load
    def make_params(self, data):
        return GenerateJournalForDestroyParams(**data)


class JournalForCreateSchema(Schema):
    """供写入数据库字段时使用"""

    new_ident = fields.String()
    parent_ident = fields.String()
    parent_timestamp = fields.Decimal()
    new_type = fields.String()
    new_storage_folder = fields.String()
    new_disk_bytes = fields.Integer()
    new_hash_version = fields.Integer()


class JournalForDestroySchema(Schema):
    """供写入数据库字段时使用"""

    idents = fields.String()


class GetRawHandleParams(object):
    def __init__(self, handle):
        self.handle = handle


class GetRawHandleParamsSchema(Schema):
    handle = fields.String(required=True, validate=Length(max=32))

    @post_load
    def make_params(self, data):
        return GetRawHandleParams(**data)


class GetRawHandleResultSchema(Schema):
    raw_handle = fields.Integer()
    ice_endpoint = fields.String()


class SetHashModeParams(object):
    def __init__(self, handle, hash_mode):
        self.handle = handle
        self.hash_mode = hash_mode


class SetHashModeParamsSchema(Schema):
    handle = fields.String(required=True, validate=Length(max=32))
    hash_mode = fields.String(required=True)

    @post_load
    def make_params(self, data):
        return SetHashModeParams(**data)
