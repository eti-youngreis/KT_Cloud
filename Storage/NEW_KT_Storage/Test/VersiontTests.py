import pytest
import datetime
import os
from Storage.NEW_KT_Storage.DataAccess.VersionManager import VersionManager
from Storage.NEW_KT_Storage.Service.Classes.VersionService import VersionService
from Storage.NEW_KT_Storage.Models.VersionModel import Version
@pytest.fixture
def version_service():
    manager = VersionManager()
    return VersionService(manager)

@pytest.fixture
def valid_version_params():
    return {
        "bucket_name": "test-bucket",
        "key": "test-object",
        "is_latest": True,
        "content": "Test content",
        "last_modified": datetime.datetime.now(),
        "etag": "123456",
        "size": 1024
    }

@pytest.fixture
def version_id():
    return "v20230919120000"


# Tests for the create method
def test_create_valid(version_service, valid_version_params):
    '''
    Test valid creation of a version object.
    '''
    result = version_service.create(**valid_version_params)
    assert result["status"] == "success"
    assert result["message"] == "Version object created successfully"


def test_create_missing_required_params(version_service):
    '''
    Test that missing required parameters raises a ValueError.
    '''
    with pytest.raises(ValueError):
        version_service.create(
            bucket_name=None,
            key="test-object",
            is_latest=True,
            content="Test content",
            last_modified=datetime.datetime.now(),
            etag="123456",
            size=1024
        )


def test_create_invalid_size(version_service):
    '''
    Test that creating an object with a negative size raises a ValueError.
    '''
    with pytest.raises(ValueError):
        version_service.create(
            bucket_name="test-bucket",
            key="test-object",
            is_latest=True,
            content="Test content",
            last_modified=datetime.datetime.now(),
            etag="123456",
            size=-10
        )


# Additional tests for the create method
def test_create_with_default_values(version_service):
    '''
    Test that creating a version with some default values works.
    '''
    result = version_service.create(
        bucket_name="test-bucket",
        key="test-object",
        is_latest=True,
        content="Test content",
        last_modified=datetime.datetime.now(),
        etag="123456",
        size=1024,
        storage_class=None,
        owner=None
    )
    assert result["status"] == "success"


# def test_create_duplicate_version_id(version_service, valid_version_params):
#     '''
#     Test that creating a version with an existing version ID raises a ValueError.
#     '''
#     version_id = "v1"  # Define a specific version ID for testing
#     valid_version_params["version_id"] = version_id  # Use the defined version ID
#     version_service.create(**valid_version_params)  # Create first version

# Tests for the get method



def test_get_version_not_found(version_service, version_id):
    '''
    Test retrieving a version that does not exist.
    '''
    with pytest.raises(ValueError):
        version_service.get("non-existent-bucket", "non-existent-object", version_id)



    # Now try to create another version with the same version ID
    with pytest.raises(ValueError):
        version_service.create(**valid_version_params)  # Attempt to create duplicate


def test_get_invalid_version_id_format(version_service):
    '''
    Test that providing an invalid version ID format raises a ValueError.
    '''
    with pytest.raises(ValueError):
        version_service.get("test-bucket", "test-object", "invalid_version_id")

def test_get_missing_required_params(version_service):
    '''
    Test that missing required parameters raises a ValueError.
    '''
    with pytest.raises(ValueError):
        version_service.get(bucket_name=None, key="test-object", version_id="v20230919120000")

def test_delete_existing_version(version_service, valid_version_params):
    '''
    Test deleting an existing version.
    '''
    version_service.create(**valid_version_params)  # Create a version first
    version_id = "test-buckettest-objectv1dcf0e65-e8ad-4778-8a1d-bc95f5201545"  # Use the correct version_id here
    result = version_service.delete(valid_version_params["bucket_name"], valid_version_params["key"], version_id)
    assert result["status"] == "success"

def test_delete_non_existent_version(version_service):
    '''
    Test that attempting to delete a non-existent version raises a ValueError.
    '''
    with pytest.raises(ValueError):
        version_service.delete("test-bucket", "test-object", "non-existent-version-id")


def test_delete_missing_required_params(version_service):
    '''
    Test that missing required parameters raises a ValueError.
    '''
    with pytest.raises(ValueError):
        version_service.delete(bucket_name="test-bucket", key=None, version_id="v20230919120000")



#
def test_persistence_after_create_version(version_service, valid_version_params):
    '''
    This test verifies that version metadata is correctly saved to the database.
    '''
    # Create version using valid parameters
    version_service.create(**valid_version_params)

    # Access VersionManager instance
    version_service = version_service.dal

    # Retrieve the version_id used for the test
    version_id = valid_version_params.get("version_id", None)
    if not version_id:
        version_id = "v1"  # Default version ID if not provided

    # Check if version metadata exists in memory using the correct get_version call
    version_metadata = version_manager.get_version(valid_version_params["bucket_name"], valid_version_params["key"], version_id)

    assert version_metadata is not None, "Version metadata should be saved to the database."

