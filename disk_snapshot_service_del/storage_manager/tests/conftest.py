import os

import pytest
from django.core.management import call_command


@pytest.fixture(scope='session')
def django_db_setup(django_db_setup, django_db_blocker):
    with django_db_blocker.unblock():
        call_command('loaddata', os.path.join('.', 'storage_manager', 'tests', 'test_data.json'))
