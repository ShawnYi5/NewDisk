from storage_manager import models as m


def _get_valid_storage_query_set(storage_root_obj):
    return (m.DiskSnapshotStorage.objects.filter(storage_root=storage_root_obj).exclude(
        storage_status=m.DiskSnapshotStorage.RECYCLED))


def valid_storage_query_set(uuid_storage_root):
    storage_root_obj = m.DiskSnapshotStorageRoot.objects.get(root_uuid=uuid_storage_root)
    return _get_valid_storage_query_set(storage_root_obj)


def get_list_locator_obj(storage_query_set):
    list_locator_obj = list()
    list_locator_id = list()
    list_locator_id_set = storage_query_set.all().values_list("locator_id")
    for i in list_locator_id_set:
        list_locator_id.append(i[0])
    list_locator_id = list(set(list_locator_id))
    if None in list_locator_id:
        list_locator_id.remove(None)
    for locator_id in list_locator_id:
        list_locator_obj.append(m.DiskSnapshotLocator.objects.get(id=locator_id))
    return list_locator_obj


def get_list_host_snapshot_obj(list_locator_obj):
    list_host_snapshot_obj = list()
    list_host_snapshot_id = list()
    for locator_obj in list_locator_obj:
        host_snapshot_ids = m.DiskSnapshot.objects.filter(locator_id=locator_obj.id).values_list('host_snapshot_id')
        for host_snapshot_id in host_snapshot_ids:
            list_host_snapshot_id.append(host_snapshot_id[0])
    list_host_snapshot_id = list(set(list_host_snapshot_id))
    for host_snapshot_id in list_host_snapshot_id:
        list_host_snapshot_obj.append(m.HostSnapshot.objects.get(id=host_snapshot_id))
    return list_host_snapshot_obj


def get_list_host_id(list_host_snapshot_obj):
    list_host_id = list()
    for host_snapshot_obj in list_host_snapshot_obj:
        list_host_id.append(host_snapshot_obj.host_id)
    return list(set(list_host_id))


def get_storage_root_uuid_list():
    uuid_list = list()
    root_query = m.DiskSnapshotStorageRoot.objects.all()
    for i in root_query:
        uuid_list.append(i.root_uuid)
    return uuid_list
