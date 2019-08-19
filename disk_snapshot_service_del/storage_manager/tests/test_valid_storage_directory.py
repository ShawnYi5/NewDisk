import os
from unittest.mock import MagicMock, patch

import pytest

from basic_library import xdata
from storage_manager import valid_storage_directory as vsd


@patch.object(target=os.path, attribute='isdir', new=MagicMock(return_value=True))
def test_normal_one():
    """普通使用"""

    file_path = r'/a/b/c/d'
    dir_path = r'/a/b/c'

    with pytest.raises(xdata.StorageDirectoryInvalid):
        vsd.check_path(file_path)
    assert not vsd.check_path(file_path, False)

    vsd.add_directory(dir_path)
    vsd.check_path(file_path)

    vsd.add_directory(dir_path)
    vsd.check_path(file_path)

    vsd.remove_directory(dir_path)

    with pytest.raises(xdata.StorageDirectoryInvalid):
        vsd.check_path(file_path)
    assert not vsd.check_path(file_path, False)
