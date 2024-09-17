import json
import os
import pytest
from Storage.NEW_KT_Storage.DataAccess import StorageManager 


def storage_manager():
    """Fixture to create an instance of OptionGroup."""
    return StorageManager('test')

def test_file_exists(file_name):
    assert storage_manager.is_file_exist(file_name)

def assert_file_exists(file_name):
    assert storage_manager.is_file_exist(file_name), f"Expected file {file_name} was not created."

# Generic function to delete a file
def delete_file_if_exists(storage_manager,file_name):
    storage_manager.delete_file(file_name)

# Generic function to load JSON file and assert its content
def assert_json_content(file_name, expected_data):
    with open(file_name, 'r') as json_file:
        data = json.load(json_file)
        for key, value in expected_data.items():
            assert data[key] == value, f"Expected {key} to be {value}, but got {data[key]}"

# @pytest.fixture
# def storage_manager():
#     """Fixture to create an instance of StorageManager."""
#     return StorageManager('test')


# def test_file_exists(storage_manager, file_name):  # Pass storage_manager as a parameter
    
#     # Use storage_manager to check if the file exists
#     assert storage_manager.is_file_exist(file_name), f"File {file_name} does not exist."