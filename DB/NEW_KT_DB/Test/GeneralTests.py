import pytest
from KT_STORAGE import StorageManager 


def storage_manager():
    """Fixture to create an instance of OptionGroup."""
    return StorageManager()

def is_file_exist(storage_manager: StorageManager, file_path: str):
    return storage_manager.is_file_exist(file_path)

def is_object_equal(obj1, obj2):
    return all([obj1.__getattribute__(attr) == obj2.__getattribute__(attr) for attr, _ in obj1.__dict__.items()]) and len(obj1.__dict__) == len(obj2.__dict__)