import time


def _convert_timestamp_to_datetime_display(timestamp):
    decimal_timestamp = round(timestamp - int(timestamp), 6)
    t = str(time.strftime("%Y-%m-%d %H:%M:%S.", time.localtime(timestamp))) + str(decimal_timestamp)[2:8]
    return t


def storage_label(storage_obj):
    convert_labels = list()
    disk_snapshot_storage_dot_str_list = list()
    if storage_obj.get_storage_type_display() == 'CDP':
        convert_labels.append(f'<f0> storage_{storage_obj.id}')
        convert_labels.append(f'<f1> {storage_obj.disk_snapshot_storage_ident}')
        convert_labels.append(f'<f2> {storage_obj.get_storage_status_display()}')
        convert_labels.append(f'<f3> {storage_obj.image_path}')
        convert_labels.append(
            f'<f4> {_convert_timestamp_to_datetime_display(storage_obj.storage_begin_timestamp)}')
        convert_labels.append(f'<f5> {_convert_timestamp_to_datetime_display(storage_obj.storage_end_timestamp)}')
    else:
        convert_labels.append(f'<f0> storage_{storage_obj.id}')
        convert_labels.append(f'<f1> {storage_obj.disk_snapshot_storage_ident}')
        convert_labels.append(f'<f2> {storage_obj.get_storage_status_display()}')
        convert_labels.append(f'<f3> {storage_obj.image_path}')
        convert_labels.append(
            f'<f4> {_convert_timestamp_to_datetime_display(storage_obj.storage_begin_timestamp)}')
    labels_str = '|'.join(convert_labels)
    disk_snapshot_storage_dot_str_list.append('Storage_%s [label="{%s}" shape=Mrecord];\n' %
                                              (storage_obj.id, labels_str))
    return disk_snapshot_storage_dot_str_list[0]


def host_snapshot_label(host_snapshot_obj):
    host_snapshot_dot_str_list = list()
    convert_labels = list()
    if host_snapshot_obj.host_snapshot_valid:
        if host_snapshot_obj.get_host_snapshot_type_display() == 'cdp':
            convert_labels.append(f'<f0> host_snapshot_{host_snapshot_obj.id}')
            convert_labels.append(f'<f1> {host_snapshot_obj.host_snapshot_ident}')
            convert_labels.append(
                f'<f2> {_convert_timestamp_to_datetime_display(host_snapshot_obj.host_snapshot_begin_timestamp)}')
            convert_labels.append(
                f'<f3> {_convert_timestamp_to_datetime_display(host_snapshot_obj.host_snapshot_end_timestamp)}')
        else:
            convert_labels.append(f'<f0> host_snapshot_{host_snapshot_obj.id}')
            convert_labels.append(f'<f1> {host_snapshot_obj.host_snapshot_ident}')
            convert_labels.append(
                f'<f2> {_convert_timestamp_to_datetime_display(host_snapshot_obj.host_snapshot_begin_timestamp)}')
    else:
        convert_labels.append(f'<f0> host_snapshot_{host_snapshot_obj.id}')
        convert_labels.append(f'<f1> invalid')
    labels_str = '|'.join(convert_labels)
    host_snapshot_dot_str_list.append('Host_snapshot_%s [label="{%s}" shape=Mrecord];\n' %
                                      (host_snapshot_obj.id, labels_str))
    return host_snapshot_dot_str_list[0]


def locator_label(locator_id):
    disk_snapshot_locator_dot_str_list = list()
    labels_str = f'<f0> L_{locator_id}'
    disk_snapshot_locator_dot_str_list.append(f'Locator_{locator_id} [label="{labels_str}" shape=Mrecord];\n')
    return disk_snapshot_locator_dot_str_list[0]
