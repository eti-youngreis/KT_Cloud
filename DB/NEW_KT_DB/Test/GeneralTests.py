import pytest
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager 


def storage_manager():
    """Fixture to create an instance of OptionGroup."""
    return StorageManager()

def test_file_exists(storage_manager:StorageManager, file_name):
    assert storage_manager.is_file_exist(file_name)
   

    