
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager


def storage_manager():
    """Fixture to create an instance of OptionGroup."""
    return StorageManager()


def is_file_exist(storage_manager: StorageManager, file_path: str):
    return storage_manager.is_file_exist(file_path)
