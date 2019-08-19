from storage_manager.visualization import label, struct, chain, db_set
from storage_manager import models as m


def storage_dot(storage_objs, storage_chain):
    sub_graph_storage = """subgraph cluster_storage{
            label = "disk_snapshot_storage"
            node[shape=record]
            %s%s}"""
    labels_storage = ""
    for storage_obj in storage_objs:
        labels_storage = labels_storage + label.storage_label(storage_obj)
    sub_graph_storage = sub_graph_storage % (labels_storage, storage_chain)
    return sub_graph_storage


def locator_dot(locator_objs, locator_chain):
    sub_graph_locator = """%s%s}"""
    labels_locator = ""
    for locator_obj in locator_objs:
        labels_locator = labels_locator + label.locator_label(locator_obj.id)
    sub_graph_locator = sub_graph_locator % (labels_locator, locator_chain)
    return sub_graph_locator


def host_snapshot_dot(host_ids):
    host_dot = ""
    for host_id in host_ids:
        host_dot = host_dot + get_host_snapshot_labels(host_id)
    return host_dot


def get_host_snapshot_labels(host_id):
    sub_graph_host = """subgraph cluster_host%s{
        label = "host%s"
        node[shape=record]
        %s%s}"""
    host_snapshot_label = ""
    host_snapshot_objs = m.HostSnapshot.objects.filter(host_id=host_id)
    for host_snapshot_obj in host_snapshot_objs:
        host_snapshot_label = host_snapshot_label + label.host_snapshot_label(host_snapshot_obj)
    host_snapshot_chain_by_host_id = chain.host_snapshot_chain_by_host_id(host_id)
    return sub_graph_host % (host_id, host_id, host_snapshot_label, host_snapshot_chain_by_host_id)


def digraph(uuid):
    valid_storage_objs = db_set.valid_storage_query_set(uuid)
    locator_objs = db_set.get_list_locator_obj(valid_storage_objs)
    host_snapshot_objs = db_set.get_list_host_snapshot_obj(locator_objs)
    host_ids = db_set.get_list_host_id(host_snapshot_objs)

    storage_chain = chain.storage_chain(valid_storage_objs)
    locator_chain = chain.locator_chain(locator_objs, valid_storage_objs)

    sub_graph_storage = storage_dot(valid_storage_objs, storage_chain)
    sub_graph_locator = locator_dot(locator_objs, locator_chain)
    sub_graph_host = host_snapshot_dot(host_ids)

    return struct.struct % (sub_graph_host, sub_graph_storage, sub_graph_locator)
