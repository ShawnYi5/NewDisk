from storage_manager.visualization import merge
from graphviz import render
import os
from storage_manager.visualization import db_set

os.environ["PATH"] += os.pathsep + "C:/Program Files (x86)/Graphviz2.38/bin"


def run():
    root_uuid_list = db_set.get_storage_root_uuid_list()
    for uuid in root_uuid_list:
        result = merge.digraph(uuid)
        test_data_directory = os.path.abspath(os.path.dirname(os.path.dirname(__file__))) + '\\tests\\test_data\\'
        with open(test_data_directory + f'{uuid}.gv', 'w') as f:
            f.write(result)
        render('dot', 'png', test_data_directory + f'{uuid}.gv')
