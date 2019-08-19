import uuid

from django.db import models

from basic_library import xfield
from basic_library import xfunctions
from basic_library import xlogging

_logger = xlogging.getLogger(__name__)


class Host(models.Model):
    """
    主机
    """
    id = models.BigAutoField(primary_key=True)
    host_ident = models.CharField(max_length=40, unique=True)

    def __str__(self):
        return f'host:<{self.host_ident}>'

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def get_or_create(host_ident):
        o, created = Host.objects.get_or_create(host_ident=host_ident)
        if created:
            _logger.debug(f'{o} created')
        return o, created


class HostSnapshot(models.Model):
    """
    主机快照
    """
    NORMAL = 1
    CDP = 2
    HOST_SNAPSHOT_TYPE_CHOICES = (
        (NORMAL, 'normal'),
        (CDP, 'cdp'),
    )

    _HOST_SNAPSHOT_TYPE_MAP = {
        'normal': NORMAL,
        'cdp': CDP
    }

    id = models.BigAutoField(primary_key=True)
    host = models.ForeignKey(Host, related_name='host_snapshots', on_delete=models.PROTECT)
    host_snapshot_ident = models.CharField(max_length=40, unique=True)
    host_snapshot_valid = models.BooleanField(default=True)
    host_snapshot_type = models.PositiveSmallIntegerField(choices=HOST_SNAPSHOT_TYPE_CHOICES)
    host_snapshot_begin_timestamp = xfield.TimestampField()
    host_snapshot_end_timestamp = xfield.TimestampField()
    host_snapshot_task_info = models.TextField(default='{}')

    class Meta:
        indexes = [
            models.Index(fields=['host_snapshot_begin_timestamp']),
            models.Index(fields=['host_snapshot_end_timestamp']),
        ]

    def __str__(self):
        return f'host_snapshot:<{self.get_host_snapshot_type_display()}:{self.host_snapshot_ident}>'

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def get_obj_by_ident(ident):
        try:
            return HostSnapshot.objects.get(host_snapshot_ident=ident)
        except HostSnapshot.DoesNotExist:
            xlogging.raise_and_logging_error(
                r'内部异常，无效的HostSnapshot标识符', f'invalid HostSnapshot {ident}', print_args=False, exc_info=True)

    @property
    def is_cdp_host_snapshot(self):
        return self.host_snapshot_type == HostSnapshot.CDP

    @staticmethod
    def create(host_obj: Host, host_snapshot_ident: str, host_snapshot_type):
        if isinstance(host_snapshot_type, str):
            host_snapshot_type = HostSnapshot._HOST_SNAPSHOT_TYPE_MAP[host_snapshot_type]
        now_timestamp = xfunctions.current_timestamp()
        o = HostSnapshot.objects.create(
            host=host_obj,
            host_snapshot_ident=host_snapshot_ident,
            host_snapshot_type=host_snapshot_type,
            host_snapshot_begin_timestamp=now_timestamp,
            host_snapshot_end_timestamp=now_timestamp
        )
        _logger.debug(f'{o} created')
        return o


class SourceDisk(models.Model):
    """
    源磁盘
    """
    MBR = 1
    GPT = 2
    RAW = 3
    SOURCE_DISK_PARTITION_TYPE_CHOICES = (
        (MBR, 'MBR'),
        (GPT, 'GPT'),
        (RAW, 'RAW'),
    )

    _SOURCE_DISK_PARTITION_TYPE_MAP = {
        'MBR': MBR,
        'GPT': GPT,
        'RAW': RAW,
    }

    id = models.BigAutoField(primary_key=True)
    host = models.ForeignKey(Host, related_name='source_disks', on_delete=models.PROTECT)
    disk_native_guid = models.CharField(max_length=40)
    agent_disk_ident = models.CharField(max_length=40)
    disk_display_name = models.CharField(max_length=128)
    disk_bytes = models.BigIntegerField()
    boot_device = models.BooleanField()
    os_device = models.BooleanField()
    bmf_device = models.BooleanField()
    partition_type = models.PositiveSmallIntegerField(choices=SOURCE_DISK_PARTITION_TYPE_CHOICES)

    class Meta:
        indexes = [
            models.Index(fields=['disk_native_guid']),
            models.Index(fields=['agent_disk_ident'])
        ]

    def __str__(self):
        return f'source_disk:<{self.agent_disk_ident}-{self.disk_native_guid}>'

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def get_or_create(host_obj: Host, disk_native_guid: str, agent_disk_ident: str, disk_display_name: str,
                      disk_bytes: int, boot_device: bool, os_device: bool, bmf_device: bool, partition_type):
        if isinstance(partition_type, str):
            partition_type = SourceDisk._SOURCE_DISK_PARTITION_TYPE_MAP[partition_type]
        o, created = SourceDisk.objects.get_or_create(
            host=host_obj,
            disk_native_guid=disk_native_guid,
            agent_disk_ident=agent_disk_ident,
            disk_display_name=disk_display_name,
            disk_bytes=disk_bytes,
            boot_device=boot_device, os_device=os_device, bmf_device=bmf_device,
            partition_type=partition_type
        )
        if created:
            _logger.debug(f'{o} created')
        return o


class DiskSnapshotLocator(models.Model):
    """
    存储镜像定位标记
        对于qcow类型，如果为CDP流的一部分，那么就是CDP Token；否则就是disk_snapshot_ident
        对于cdp类型，就是CDP Token
        对于共享磁盘的备份，该值为业务逻辑生成
    """
    id = models.BigAutoField(primary_key=True)
    locator_ident = models.CharField(max_length=40, unique=True)

    def __str__(self):
        return f'locator:<{self.id}:{self.locator_ident}>'

    def __repr__(self):
        return self.__str__()


class DiskSnapshotStorageRoot(models.Model):
    """
    存储镜像依赖树标识
        具有连同关系的快照存储都关联到同一标识
    """
    ROOT_HASH_TYPE_UNKNOWN = 0
    ROOT_HASH_TYPE_NONE = 1
    ROOT_HASH_TYPE_MD4_CRC32 = 2
    ROOT_HASH_TYPE_CHOICES = (
        (ROOT_HASH_TYPE_NONE, 'NO_HASH'),
        (ROOT_HASH_TYPE_MD4_CRC32, 'MD4CRC32'),
    )

    RECYCLE_ROOT_UUID = '00000000000000000000000000000001'

    id = models.BigAutoField(primary_key=True)
    root_uuid = models.UUIDField(default=uuid.uuid4, unique=True, editable=False)
    hash_type = models.PositiveSmallIntegerField(choices=ROOT_HASH_TYPE_CHOICES)
    root_valid = models.BooleanField(default=True)

    @property
    def root_ident(self):
        return self.root_uuid.hex

    def __str__(self):
        return f'root:<{self.id}>'

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def get_recycle_root_obj():
        try:
            return DiskSnapshotStorageRoot.objects.get(root_uuid=DiskSnapshotStorageRoot.RECYCLE_ROOT_UUID)
        except DiskSnapshotStorageRoot.DoesNotExist:
            return DiskSnapshotStorageRoot.objects.create(
                root_uuid=DiskSnapshotStorageRoot.RECYCLE_ROOT_UUID,
                hash_type=DiskSnapshotStorageRoot.ROOT_HASH_TYPE_UNKNOWN,
            )

    @staticmethod
    def get_obj_by_ident(ident):
        try:
            return DiskSnapshotStorageRoot.objects.get(root_uuid=ident)
        except DiskSnapshotStorageRoot.DoesNotExist:
            xlogging.raise_and_logging_error(
                r'内部异常，无效的Root标识符', f'invalid DiskSnapshotStorageRoot {ident}',
                print_args=False, exc_info=True)

    def set_invalid(self):
        assert not self.is_recycle_root
        self.root_valid = False
        self.save(update_fields=['root_valid', ])

    @property
    def is_recycle_root(self):
        return self.root_ident == DiskSnapshotStorageRoot.RECYCLE_ROOT_UUID

    @staticmethod
    def get_valid_objs():
        return DiskSnapshotStorageRoot.objects.filter(root_valid=True)


class DiskSnapshot(models.Model):
    """
    逻辑磁盘快照
    """
    id = models.BigAutoField(primary_key=True)
    source_disk = models.ForeignKey(SourceDisk, related_name='disk_snapshots', on_delete=models.PROTECT)
    host_snapshot = models.ForeignKey(HostSnapshot, related_name='disk_snapshots', on_delete=models.PROTECT)
    locator = models.ForeignKey(DiskSnapshotLocator, related_name='disk_snapshots', on_delete=models.PROTECT)
    disk_index = models.IntegerField()

    def __str__(self):
        return (
            f'disk_snapshot:<{self.disk_index}:{self.locator_id}:'
            f'{self.host_snapshot.get_host_snapshot_type_display()}:{self.host_snapshot.host_snapshot_ident}>'
        )

    def __repr__(self):
        return self.__str__()


class DiskSnapshotStorage(models.Model):
    """
    磁盘快照数据存储
    """
    CDP = 1
    QCOW = 2

    DISK_SNAPSHOT_STORAGE_TYPE_CHOICES = (
        (CDP, 'CDP'),
        (QCOW, 'QCOW'),
    )

    CREATING = 1
    DATA_WRITING = 2
    HASHING = 3
    STORAGE = 4
    EXCEPTION = 5
    RECYCLING = 6
    RECYCLED = 7

    DISK_SNAPSHOT_STORAGE_STATUS_CHOICES = (
        (CREATING, 'creating'),
        (DATA_WRITING, 'data_writing'),
        (HASHING, 'hashing'),
        (STORAGE, 'storage'),
        (EXCEPTION, 'exception'),
        (RECYCLING, 'recycling'),
        (RECYCLED, 'recycled'),
    )

    STATUS_CAN_DELETE = (HASHING, STORAGE, EXCEPTION, RECYCLING,)
    STATUS_CAN_MERGE = (STORAGE, EXCEPTION, RECYCLING,)
    STATUS_RECYCLE = (RECYCLING, RECYCLED,)
    STATUS_NOT_READABLE = (CREATING, RECYCLED,)
    STATUS_WRITING = (CREATING, DATA_WRITING, HASHING)

    id = models.BigAutoField(primary_key=True)
    storage_root = models.ForeignKey(DiskSnapshotStorageRoot,
                                     related_name='disk_snapshot_storages', on_delete=models.PROTECT)
    source_disk = models.ForeignKey(SourceDisk,
                                    related_name='disk_snapshot_storages', on_delete=models.PROTECT)
    locator = models.ForeignKey(DiskSnapshotLocator,
                                related_name='disk_snapshot_storages', on_delete=models.PROTECT, null=True)
    storage_type = models.PositiveSmallIntegerField(choices=DISK_SNAPSHOT_STORAGE_TYPE_CHOICES)
    storage_status = models.PositiveSmallIntegerField(choices=DISK_SNAPSHOT_STORAGE_STATUS_CHOICES)
    disk_snapshot_storage_ident = models.CharField(max_length=40, unique=True)
    disk_bytes = models.BigIntegerField()
    image_path = models.CharField(max_length=128)
    full_hash_path = models.CharField(max_length=128 + 8, null=True)
    inc_hash_path = models.CharField(max_length=128 + 16, null=True)
    storage_begin_timestamp = xfield.TimestampField()
    storage_end_timestamp = xfield.TimestampField()
    parent_snapshot = models.ForeignKey('self',
                                        related_name='children_snapshots', on_delete=models.PROTECT, null=True)
    parent_timestamp = xfield.TimestampField(null=True)
    inc_raw_data_bytes = models.BigIntegerField(default=-1)
    file_level_deduplication = models.BooleanField()

    class Meta:
        indexes = [
            models.Index(fields=['image_path']),
        ]

    def __str__(self):
        return (
            f'storage:<{self.disk_snapshot_storage_ident}:{self.locator_id}'
            f':{self.image_path}:{self.storage_root_id}>'
        )

    def __repr__(self):
        return self.__str__()

    @property
    def is_cdp_file(self):
        return self.storage_type == DiskSnapshotStorage.CDP

    @staticmethod
    def valid_storage_objs(storage_root_obj):
        return (DiskSnapshotStorage.objects
                .filter(storage_root=storage_root_obj)
                .exclude(storage_status=DiskSnapshotStorage.RECYCLED)
                )

    def set_storage_status(self, storage_status):
        if self.storage_status != storage_status:
            if self.storage_status in (self.STORAGE, self.EXCEPTION,):
                assert storage_status in (self.RECYCLING, self.RECYCLED,)
            elif self.storage_status == self.RECYCLING:
                assert storage_status == self.RECYCLED
            else:
                assert self.storage_status != self.RECYCLED

        _logger.debug(f'{self} status {self.storage_status} to {storage_status}')
        self.storage_status = storage_status
        self.save(update_fields=['storage_status', ])
