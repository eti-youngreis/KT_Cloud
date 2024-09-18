import pytest
from KT_STORAGE import StorageManager 


def storage_manager():
    """Fixture to create an instance of OptionGroup."""
    return StorageManager()

def test_file_exists():
    assert storage_manager.is_file_exist(file_name)