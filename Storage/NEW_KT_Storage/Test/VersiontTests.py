import pytest
import datetime
#from Storage.NEW_KT_Storage.DataAccess.VersionManager import VersionManager
#from Storage.NEW_KT_Storage.Service.Classes.VersionService import VersionService

@pytest.fixture
def version_service():
    pass
     #manager = VersionManager()
     #return VersionService(manager)

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