import json
import os
import sys
import pytest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager

@pytest.fixture
def storage_manager():
    """Fixture to create an instance of StorageManager."""
    return StorageManager('test')

def assert_file_exist(storage_manager, file_name):
    assert storage_manager.is_file_exist(file_name), f"Expected file {file_name} was not created."

def is_file_exist(storage_manager, file_name):
    return storage_manager.is_file_exist(file_name)

# Generic function to load JSON file and assert its content
def assert_json_content(storage_manager, file_name, expected_data):
    '''Validates the content of a JSON file stored in the storage manager's base directory.

    Parameters:
    storage_manager : StorageManager
        An instance of a storage manager that handles file operations. It should contain a base directory 
        where the JSON file is stored.
        
    file_name : str
        The name of the JSON file (including the extension) to be validated.
        
    expected_data : dict
        A dictionary containing the expected key-value pairs. Each key in this dictionary is compared 
        to the corresponding key in the JSON file, and an assertion is made to ensure the values match.
        
    Raises:
    AssertionError
        If any of the values in the JSON file do not match the corresponding values in the `expected_data`, 
        an AssertionError is raised with a message indicating the mismatch.'''
    full_path = os.path.join(storage_manager.base_directory, file_name)
    with open(full_path, 'r') as json_file:
        data = json.load(json_file)
        for key, value in expected_data.items():
            print(value)
            assert data[key] == value, f"Expected {key} to be {value}, but got {data[key]}"
