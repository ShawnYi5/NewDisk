import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.ext import declarative

Base = declarative.declarative_base()


class Journal(Base):
    __tablename__ = 'journal'

    # operation_type:
    TYPE_CREATE = 'c'
    TYPE_DESTROY = 'd'

    TYPE_DISPLAY = {
        TYPE_CREATE: 'create',
        TYPE_DESTROY: 'destroy',
    }

    id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, autoincrement=True, nullable=False)
    produced_timestamp = sqlalchemy.Column(sqlalchemy.Numeric(16, 6, 6, True), nullable=False)
    consumed_timestamp = sqlalchemy.Column(sqlalchemy.Numeric(16, 6, 6, True), nullable=True)
    token = sqlalchemy.Column(sqlalchemy.String(32), unique=True, nullable=False)
    operation_str = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    operation_type = sqlalchemy.Column(sqlalchemy.String(1), nullable=False)  # operation_type 为枚举类型
    children_idents = sqlalchemy.Column(sqlalchemy.String, nullable=True)

    @property
    def op_type_display(self) -> str:
        return self.TYPE_DISPLAY[self.operation_type]

    @property
    def consumed(self) -> bool:
        """是否被消费"""
        return self.consumed_timestamp is not None

    def __str__(self):
        return 'journal: {}-{}-{}'.format(
            self.token, self.op_type_display, 'consumed' if self.consumed else 'valid')

    def __repr__(self):
        return self.__str__()


class SnapshotStorage(Base):
    __tablename__ = 'snapshot_storage'

    # type:
    TYPE_QCOW = 'q'
    TYPE_CDP = 'c'

    # status:
    STATUS_CREATING = 'c'
    STATUS_WRITING = 'w'
    STATUS_HASHING = 'h'
    STATUS_STORAGE = 's'
    STATUS_ABNORMAL = 'a'
    STATUS_RECYCLING = 'r'
    STATUS_DELETED = 'd'

    STATUS_DISPLAY = {
        STATUS_CREATING: 'creating',
        STATUS_WRITING: 'writing',
        STATUS_HASHING: 'hashing',
        STATUS_STORAGE: 'storage',
        STATUS_ABNORMAL: 'abnormal',
        STATUS_RECYCLING: 'recycling',
        STATUS_DELETED: 'deleted',
    }

    INVALID_STORAGE_STATUS = (
        STATUS_ABNORMAL,
        STATUS_DELETED,
    )

    ident = sqlalchemy.Column(sqlalchemy.String(32), primary_key=True, unique=True, nullable=False)
    parent_ident = sqlalchemy.Column(sqlalchemy.String(32), sqlalchemy.ForeignKey("snapshot_storage.ident"),
                                     nullable=True)
    parent_timestamp = sqlalchemy.Column(sqlalchemy.Numeric(16, 6, 6, True), nullable=True)

    type = sqlalchemy.Column(sqlalchemy.String(1), nullable=False)  # type 为枚举类型
    disk_bytes = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
    status = sqlalchemy.Column(sqlalchemy.String(1), nullable=False)  # status 为枚举类型
    image_path = sqlalchemy.Column(sqlalchemy.String(250), nullable=False)
    new_storage_size = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=True)
    start_timestamp = sqlalchemy.Column(sqlalchemy.Numeric(16, 6, 6, True), nullable=True)
    finish_timestamp = sqlalchemy.Column(sqlalchemy.Numeric(16, 6, 6, True), nullable=True)
    tree_ident = sqlalchemy.Column(sqlalchemy.String(40), index=True, nullable=False)
    file_level_deduplication = sqlalchemy.Column(sqlalchemy.Boolean, nullable=True)
    hash = orm.relationship("Hash", lazy='dynamic')

    @staticmethod
    def format_status(status):
        return SnapshotStorage.STATUS_DISPLAY[status]

    @property
    def status_display(self) -> str:
        return self.STATUS_DISPLAY[self.status]

    def __str__(self):
        return 'snapshot: {}-{}-{}'.format(self.ident, self.status_display, self.parent_ident)

    def __repr__(self):
        return self.__str__()

    @property
    def is_cdp(self):
        return self.type == SnapshotStorage.TYPE_CDP

    @property
    def is_qcow(self):
        return self.type == SnapshotStorage.TYPE_QCOW


class Hash(Base):
    __tablename__ = 'hash'

    # version:
    VERSION_MD4_CRC32 = '1'

    # type:
    TYPE_INCREMENT = 'i'
    TYPE_FULL = 'f'

    id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, autoincrement=True, nullable=False)
    storage_ident = sqlalchemy.Column(sqlalchemy.String(32), sqlalchemy.ForeignKey("snapshot_storage.ident"),
                                      nullable=False)
    timestamp = sqlalchemy.Column(sqlalchemy.Numeric(16, 6, 6, True), nullable=False)
    # Hash算法
    version = sqlalchemy.Column(sqlalchemy.String(1), nullable=False)  # version 为枚举类型
    # 全量、增量
    type = sqlalchemy.Column(sqlalchemy.String(1), nullable=False)  # type 为枚举类型
    path = sqlalchemy.Column(sqlalchemy.String(250), nullable=False)
