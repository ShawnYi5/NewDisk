import threading
import time
import random

import create_storage_task as cst
import destroy_storage_task as dst
import open_storage_task as ost

normal_guid_pool = ['tree1_1', 'tree2_1', 'tree3_1']
security_guid_pool = ['tree1_1', 'tree2_1', 'tree3_1']
handle_pool = list()


class CreateStorageThread(threading.Thread):
    def run(self):
        while True:
            print('a')
            time.sleep(random.randint(1, 3))
            cst.CreateStorage().execute()


class DestroyStorageThread(threading.Thread):
    def run(self):
        while True:
            print('b')
            time.sleep(random.randint(1, 3))
            dst.DestroyStorage().execute()


class OpenStorageThread(threading.Thread):
    def run(self):
        while True:
            print('c')
            time.sleep(random.randint(1, 3))
            ost.OpenStorage().execute()


def test_disksnapshot_task():
    print("Start test disk snapshot!!!!!!!!!!!!!!!!!!!!!!!")
    threads = [CreateStorageThread(), DestroyStorageThread(), OpenStorageThread(),
               CreateStorageThread(), DestroyStorageThread(), OpenStorageThread()]
    for t in threads:
        t.start()


if __name__ == "__main__":
    test_disksnapshot_task()
