import pytest
import os
from GeneralTests import test_file_exists
from Storage.NEW_KT_Storage.Models.ObjectVersioningModel import VersionObject
from Storage.NEW_KT_Storage.DataAccess.VersionObjectManager import VersionManager

def test_create_in_memory_version_object(self):
    '''
    This test ensures that a version object is correctly created and stored.
    '''
    version_obj = VersionObject(...)  # initialize the VersionObject with appropriate data
    vm = VersionManager()
    vm.create_in_memory_version_object(version_obj, 'bucket1', 'object1')

    expected_path = os.path.join(vm.base_directory, 'bucket1', 'object1', f"bucket1object120240918025520.json")
    self.assertTrue(os.path.exists(expected_path))
    self.assertTrue(vm.object_manager.get_from_memory(vm.object_name, {"pk": "bucket1object120240918025520"}))

def test_get_version_object_exists(self):
    '''
    This test checks if an existing version object can be retrieved successfully.
    '''
    version_id = "20240918025520"
    vm = VersionManager()
    version_obj, version_obj_db = vm.get_version_object('bucket1', 'object1', version_id)

    self.assertIsNotNone(version_obj)
    self.assertIsNotNone(version_obj_db)

def test_get_version_object_not_exist(self):
    '''
    This test ensures that the method raises FileNotFoundError if the version doesn't exist.
    '''
    vm = VersionManager()
    with self.assertRaises(FileNotFoundError):
        vm.get_version_object('bucket1', 'nonexistent_object', '20240918025520')

def test_delete_version_object(self):
    '''
    This test verifies that the version object is correctly deleted from the directory and the database.
    '''
    version_id = "20240918025520"
    vm = VersionManager()
    vm.delete_version_object('bucket1', 'object1', version_id)

    expected_path = os.path.join(vm.base_directory, 'bucket1', 'object1', f"{version_id}.json")
    self.assertFalse(os.path.exists(expected_path))
    self.assertIsNone(vm.object_manager.get_from_memory(vm.object_name, {"pk": f'bucket1object1{version_id}'}))

def test_delete_version_object_not_exist(self):
    '''
    This ensures that the correct exception is raised when attempting to delete a nonexistent version.
    '''
    vm = VersionManager()
    with self.assertRaises(FileNotFoundError):
        vm.delete_version_object('bucket1', 'nonexistent_object', '20240918025520')

def test_persistence_after_create_version(self):
    '''
    This test verifies that version metadata is correctly saved to the database.
    '''
    version_obj = VersionObject(...)  # Initialize with sample data
    vm = VersionManager()
    vm.create_in_memory_version_object(version_obj, 'bucket1', 'object1')

    version_metadata = vm.object_manager.get_from_memory(vm.object_name, {"pk": "bucket1object120240918025520"})
    self.assertIsNotNone(version_metadata)
