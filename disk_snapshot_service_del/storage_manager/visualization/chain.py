from storage_manager import models as m


def locator_chain(locator_objs, storage_objs):
    chain_host_snapshot = ""
    for locator_obj in locator_objs:
        locator_host_snapshot_ids = m.DiskSnapshot.objects.filter(locator_id=locator_obj.id).values_list('locator_id',
                                                                                                         'host_snapshot_id')
        for lh_id in list(set(locator_host_snapshot_ids)):
            if lh_id:
                chain_host_snapshot = chain_host_snapshot + "Locator_%s -> Host_snapshot_%s;\n" % (
                    str(lh_id[0]), str(lh_id[1]))

    chain_storage = ""
    for storage_obj in storage_objs:
        storage_id = storage_obj.id
        locator_id = storage_obj.locator_id
        if locator_id:
            chain_storage = chain_storage + "Locator_%s -> Storage_%s;\n" % (str(locator_id), str(storage_id))

    return chain_host_snapshot + chain_storage


def storage_chain(storage_objs):
    chain_str = ""
    for storage_obj in storage_objs:
        storage_id = storage_obj.id
        parent_id = storage_obj.parent_snapshot_id
        if parent_id:
            chain_str = chain_str + "Storage_%s -> Storage_%s;" % (str(parent_id), str(storage_id))
    return chain_str


def host_snapshot_chain_by_host_id(host_id):
    chain_str = "->"
    list_str = list()
    dict_id_begin_time = dict()
    begin_time_query_set = m.HostSnapshot.objects.filter(host_id=host_id).values('id', 'host_snapshot_begin_timestamp')
    for i in begin_time_query_set:
        dict_id_begin_time.update({i['id']: i["host_snapshot_begin_timestamp"]})
    dict_id_begin_time = sorted(dict_id_begin_time.items(), key=lambda item: item[1])
    for i in dict_id_begin_time:
        list_str.append('Host_snapshot_%s' % str(i[0]))
    chain_str = chain_str.join(list_str) + ';\n'
    return chain_str
