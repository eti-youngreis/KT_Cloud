import pytest
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager 

@pytest.fixture
def storage_manager():
    """Fixture to create an instance of OptionGroup."""
    return StorageManager()

def check_file_exists(storage_manager:StorageManager, file_name:str):
    return storage_manager.is_file_exist(file_name)
   

    