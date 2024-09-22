<<<<<<< HEAD
import json
import os
import sys
import pytest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
=======

from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager

>>>>>>> 4d54c64e35d96b15639167fc7ab4458f0d4701df

@pytest.fixture
def storage_manager():
    """Fixture to create an instance of StorageManager."""
    return StorageManager('test')

<<<<<<< HEAD
def assert_file_exists(storage_manager, file_name):
    assert storage_manager.is_file_exist(file_name), f"Expected file {file_name} was not created."

# Generic function to delete a file
def delete_file_if_exists(storage_manager, file_name):
    storage_manager.delete_file(file_name)

# Generic function to load JSON file and assert its content
def assert_json_content(storage_manager, file_name, expected_data):
    full_path = os.path.join(storage_manager.base_directory, file_name)
    with open(full_path, 'r') as json_file:
        data = json.load(json_file)
        for key, value in expected_data.items():
            print(value)
            assert data[key] == value, f"Expected {key} to be {value}, but got {data[key]}"
=======

def is_file_exist(storage_manager: StorageManager, file_path: str):
    return storage_manager.is_file_exist(file_path)
>>>>>>> 4d54c64e35d96b15639167fc7ab4458f0d4701df
