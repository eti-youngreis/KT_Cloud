import pytest
import os
import asyncio
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from PyStorage import S3ClientSimulator

@pytest.fixture
def s3_client_simulator():
    return S3ClientSimulator()


@pytest.mark.asyncio
async def test_copy_object_file_not_found(s3_client_simulator):
    bucket_name = "bucket2"
    copy_source = {'Bucket': 'bucket1', 'Key': 'non_existing.txt'}
    key = 'object1_copy.txt'

    with pytest.raises(RuntimeError, match="File not found error"):
        await s3_client_simulator.copy_object(bucket_name, copy_source, key)

# @pytest.mark.asyncio
# async def test_copy_object_metadata_key_error(s3_client_simulator):
#     bucket_name = "bucket2"
#     copy_source = {'Bucket': 'bucket1', 'Key': 'object1.txt'}
#     key = 'object1_copy.txt'

#     s3_client_simulator.metadata_manager.get_bucket_metadata = lambda bucket, key: {}

#     with pytest.raises(RuntimeError, match="Metadata key error"):
#         await s3_client_simulator.copy_object(bucket_name, copy_source, key)
@pytest.mark.asyncio
async def test_copy_object_invalid_file_size(s3_client_simulator):
    bucket_name = "bucket2"
    copy_source = {'Bucket': 'bucket1', 'Key': 'large_file.txt'}
    key = 'object1_copy.txt'

    # Mocking the metadata copy method to simulate an invalid file size error
    s3_client_simulator.metadata_manager.copy_metadata = lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("Invalid file size"))

    with pytest.raises(RuntimeError, match="Invalid file size"):
        await s3_client_simulator.copy_object(bucket_name, copy_source, key)

@pytest.mark.asyncio
async def test_copy_object_invalid_bucket_configuration(s3_client_simulator):
    bucket_name = "bucket2"
    copy_source = {'Bucket': 'invalid_bucket', 'Key': 'object1.txt'}
    key = 'object1_copy.txt'

    # Mocking the metadata manager to simulate an invalid bucket configuration error
    s3_client_simulator.metadata_manager.get_bucket_metadata = lambda bucket, key: (_ for _ in ()).throw(ValueError("Invalid bucket configuration"))

    with pytest.raises(RuntimeError, match="Invalid bucket configuration"):
        await s3_client_simulator.copy_object(bucket_name, copy_source, key)


@pytest.mark.asyncio
async def test_copy_object_unexpected_error(s3_client_simulator):
    bucket_name = "bucket2"
    copy_source = {'Bucket': 'bucket1', 'Key': 'object1.txt'}
    key = 'object1_copy.txt'

    s3_client_simulator.metadata_manager.copy_metadata = lambda *args, **kwargs: (_ for _ in ()).throw(Exception)

    with pytest.raises(RuntimeError, match="Unexpected error"):
        await s3_client_simulator.copy_object(bucket_name, copy_source, key)

@pytest.mark.asyncio
async def test_copy_object_empty_source(s3_client_simulator):
    bucket_name = "bucket2"
    copy_source = {'Bucket': 'bucket1', 'Key': 'object1.txt'}
    key = 'object1_copy.txt'

    s3_client_simulator.metadata_manager.get_bucket_metadata = lambda bucket, key: None

    with pytest.raises(RuntimeError, match="File not found error"):
        await s3_client_simulator.copy_object(bucket_name, copy_source, key)

@pytest.mark.asyncio
async def test_copy_object_invalid_key_type(s3_client_simulator):
    bucket_name = "bucket2"
    copy_source = {'Bucket': 'bucket1', 'Key': 'object1.txt'}
    key = 12345  # Invalid key type

    with pytest.raises(TypeError):
        await s3_client_simulator.copy_object(bucket_name, copy_source, key)

@pytest.mark.asyncio
async def test_get_object_acl_success(s3_client_simulator):
    # Arrange
    bucket_name = "bucket1"
    key = "object1.txt"
    
    # Mock the metadata manager to return valid ACL metadata
    s3_client_simulator.metadata_manager.get_latest_version = lambda bucket, key: '1'
    s3_client_simulator.metadata_manager.get_bucket_metadata = lambda bucket, key: {
        'versions': {
            '1': {
                'acl': {
                    'owner': 'user1',
                    'permissions': ['READ', 'WRITE']
                }
            }
        }
    }

    # Act
    acl_result = await s3_client_simulator.get_object_acl(bucket_name, key)

    # Assert
    assert acl_result['Owner']['DisplayName'] == 'user1'
    assert len(acl_result['Grants']) == 2
    assert acl_result['Grants'][0]['Permission'] == 'READ'
    assert acl_result['Grants'][1]['Permission'] == 'WRITE'

@pytest.mark.asyncio
async def test_get_object_acl_no_acl(s3_client_simulator):
    # Arrange
    bucket_name = "bucket1"
    key = "object1.txt"
    
    # Mock the metadata manager to return metadata without ACL
    s3_client_simulator.metadata_manager.get_latest_version = lambda bucket, key: '1'
    s3_client_simulator.metadata_manager.get_bucket_metadata = lambda bucket, key: {
        'versions': {
            '1': {}
        }
    }

    # Act
    acl_result = await s3_client_simulator.get_object_acl(bucket_name, key)

    # Assert
    assert acl_result['Owner']['DisplayName'] == 'unknown'
    assert len(acl_result['Grants']) == 0

@pytest.mark.asyncio
async def test_get_object_acl_file_not_found(s3_client_simulator):
    # Arrange
    bucket_name = "bucket1"
    key = "non_existing.txt"
    
    # Mock the latest version to raise a FileNotFoundError
    s3_client_simulator.metadata_manager.get_latest_version = lambda bucket, key: (_ for _ in ()).throw(FileNotFoundError)

    # Act & Assert
    with pytest.raises(RuntimeError, match="Metadata not found"):
        await s3_client_simulator.get_object_acl(bucket_name, key)

@pytest.mark.asyncio
async def test_get_object_acl_unexpected_error(s3_client_simulator):
    # Arrange
    bucket_name = "bucket1"
    key = "object1.txt"
    
    # Mock the metadata manager to raise an unexpected error
    s3_client_simulator.metadata_manager.get_latest_version = lambda bucket, key: '1'
    s3_client_simulator.metadata_manager.get_bucket_metadata = lambda bucket, key: {
        'versions': {
            '1': {}
        }
    }
    s3_client_simulator.metadata_manager.get_bucket_metadata = lambda bucket, key: (_ for _ in ()).throw(Exception)

    # Act & Assert
    with pytest.raises(RuntimeError, match="Unexpected error"):
        await s3_client_simulator.get_object_acl(bucket_name, key)

@pytest.mark.asyncio
async def test_get_object_acl_missing_owner(s3_client_simulator):
    # Arrange
    bucket_name = "bucket1"
    key = "object1.txt"
    
    # Mock the metadata without an owner
    s3_client_simulator.metadata_manager.get_latest_version = lambda bucket, key: '1'
    s3_client_simulator.metadata_manager.get_bucket_metadata = lambda bucket, key: {
        'versions': {
            '1': {
                'acl': {
                    'permissions': ['READ']
                }
            }
        }
    }

    # Act
    acl_result = await s3_client_simulator.get_object_acl(bucket_name, key)

    # Assert
    assert acl_result['Owner']['DisplayName'] == 'unknown'
    assert len(acl_result['Grants']) == 1
    assert acl_result['Grants'][0]['Permission'] == 'READ'

@pytest.mark.asyncio
async def test_get_object_acl_multiple_permissions(s3_client_simulator):
    # Arrange
    bucket_name = "bucket1"
    key = "object1.txt"
    
    # Mock the metadata with multiple permissions
    s3_client_simulator.metadata_manager.get_latest_version = lambda bucket, key: '1'
    s3_client_simulator.metadata_manager.get_bucket_metadata = lambda bucket, key: {
        'versions': {
            '1': {
                'acl': {
                    'owner': 'user2',
                    'permissions': ['READ', 'WRITE', 'DELETE']
                }
            }
        }
    }

    # Act
    acl_result = await s3_client_simulator.get_object_acl(bucket_name, key)

    # Assert
    assert acl_result['Owner']['DisplayName'] == 'user2'
    assert len(acl_result['Grants']) == 3
    assert acl_result['Grants'][0]['Permission'] == 'READ'
    assert acl_result['Grants'][1]['Permission'] == 'WRITE'
    assert acl_result['Grants'][2]['Permission'] == 'DELETE'

@pytest.mark.asyncio
async def test_get_object_acl_empty_permissions(s3_client_simulator):
    # Arrange
    bucket_name = "bucket1"
    key = "object1.txt"
    
    # Mock the metadata with empty permissions
    s3_client_simulator.metadata_manager.get_latest_version = lambda bucket, key: '1'
    s3_client_simulator.metadata_manager.get_bucket_metadata = lambda bucket, key: {
        'versions': {
            '1': {
                'acl': {
                    'owner': 'user3',
                    'permissions': []
                }
            }
        }
    }

    # Act
    acl_result = await s3_client_simulator.get_object_acl(bucket_name, key)

    # Assert
    assert acl_result['Owner']['DisplayName'] == 'user3'
    assert len(acl_result['Grants']) == 0

@pytest.mark.asyncio
async def test_get_object_acl_owner_only(s3_client_simulator):
    # Arrange
    bucket_name = "bucket1"
    key = "object1.txt"
    
    # Mock the metadata with only an owner and no permissions
    s3_client_simulator.metadata_manager.get_latest_version = lambda bucket, key: '1'
    s3_client_simulator.metadata_manager.get_bucket_metadata = lambda bucket, key: {
        'versions': {
            '1': {
                'acl': {
                    'owner': 'user4'
                }
            }
        }
    }

    # Act
    acl_result = await s3_client_simulator.get_object_acl(bucket_name, key)

    # Assert
    assert acl_result['Owner']['DisplayName'] == 'user4'
    assert len(acl_result['Grants']) == 0


@pytest.mark.asyncio
async def test_delete_object_error_handling(s3_client_simulator):
    bucket_name = "bucket1"
    key = "object_to_delete.txt"
    
    # Mock the versioning status to not enabled
    s3_client_simulator.metadata_manager.get_versioning_status = lambda bucket: 'not enabled'
    
    # Mock the delete object method to succeed
    s3_client_simulator.metadata_manager.delete_object = lambda bucket, key, is_sync: True
    
    # Mock the file removal to raise an exception
    with pytest.raises(Exception):
        await s3_client_simulator.delete_object(bucket_name, key)


@pytest.mark.asyncio
async def test_delete_object_invalid_version_id(s3_client_simulator):
    bucket_name = "bucket1"
    key = "object_with_invalid_version.txt"
    
    # Mock the versioning status to enabled
    s3_client_simulator.metadata_manager.get_versioning_status = lambda bucket: 'enabled'
    
    # Act
    result = await s3_client_simulator.delete_object(bucket_name, key, versionId=None)

    # Assert
    assert result == {}

@pytest.mark.asyncio
async def test_delete_object_invalid_file_path(s3_client_simulator):
    # Arrange
    bucket_name = "bucket1"
    key = "object_to_delete_invalid.txt"
    
    # Mock the versioning status to not enabled
    s3_client_simulator.metadata_manager.get_versioning_status = lambda bucket: 'not enabled'
    s3_client_simulator.metadata_manager.delete_object = lambda bucket, key, is_sync: True

    # Mocking the file removal to raise an exception
    with pytest.raises(Exception):
        await s3_client_simulator.delete_object(bucket_name, key)

@pytest.mark.asyncio
async def test_delete_object_with_delete_marker(s3_client_simulator):
    # Arrange
    bucket_name = "bucket1"
    key = "object_with_delete_marker.txt"
    
    # Mock the versioning status to suspended
    s3_client_simulator.metadata_manager.get_versioning_status = lambda bucket: 'suspended'
    s3_client_simulator.metadata_manager.get_latest_version = lambda bucket, key: '1'
    s3_client_simulator.metadata_manager.get_bucket_metadata = lambda bucket, key: {
        'deleteMarker': True
    }

    # Act & Assert
    with pytest.raises(FileNotFoundError, match="object object_with_delete_marker.txt not found"):
        await s3_client_simulator.delete_object(bucket_name, key)


@pytest.mark.asyncio
async def test_delete_object_with_invalid_bucket(s3_client_simulator):
    bucket_name = "invalid_bucket"
    key = "file_to_delete.txt"

    # Mock the versioning status to not enabled
    s3_client_simulator.metadata_manager.get_versioning_status = lambda bucket: 'not enabled'

    with pytest.raises(FileNotFoundError, match=f"object {key} not found"):
        await s3_client_simulator.delete_object(bucket_name, key)

@pytest.mark.asyncio
async def test_delete_object_when_no_version_id_provided(s3_client_simulator):
    bucket_name = "bucket1"
    key = "file_without_version.txt"

    # Mock the versioning status to enabled
    s3_client_simulator.metadata_manager.get_versioning_status = lambda bucket: 'enabled'

    # Act
    result = await s3_client_simulator.delete_object(bucket_name, key, versionId=None)

    # Assert
    assert result == {}

@pytest.mark.asyncio
async def test_delete_objects_with_errors(s3_client_simulator):
    bucket_name = "bucket1"
    delete = {
        'Objects': [
            {'Key': 'object1.txt'},
            {'Key': 'object2.txt'}
        ]
    }

    # Mock the delete_object method to raise an exception for one object
    async def delete_object_mock(bucket, key, version_id, is_sync):
        if key == 'object1.txt':
            raise RuntimeError("Deletion failed")
        return {'DeleteMarker': True}

    s3_client_simulator.delete_object = delete_object_mock

    # Act
    result = await s3_client_simulator.delete_objects(bucket_name, delete)

    # Assert
    assert len(result['Deleted']) == 1
    assert len(result['Errors']) == 1
    assert result['Errors'][0]['Key'] == 'object1.txt'
    assert result['Errors'][0]['Message'] == "Deletion failed"

@pytest.mark.asyncio
async def test_delete_objects_empty_list(s3_client_simulator):
    bucket_name = "bucket1"
    delete = {
        'Objects': []
    }

    # Act
    result = await s3_client_simulator.delete_objects(bucket_name, delete)

    # Assert
    assert len(result['Deleted']) == 0
    assert len(result['Errors']) == 0


@pytest.mark.asyncio
async def test_delete_objects_some_failures(s3_client_simulator):
    bucket_name = "bucket1"
    delete = {
        'Objects': [
            {'Key': 'object1.txt'},
            {'Key': 'object2.txt'},
            {'Key': 'object3.txt'}
        ]
    }

    # Mock the delete_object method to succeed for the first and fail for the second
    async def delete_object_mock(bucket, key, version_id, is_sync):
        if key == 'object2.txt':
            raise RuntimeError("Deletion failed")
        return {'DeleteMarker': True}

    s3_client_simulator.delete_object = delete_object_mock

    # Act
    result = await s3_client_simulator.delete_objects(bucket_name, delete)

    # Assert
    assert len(result['Deleted']) == 2
    assert len(result['Errors']) == 1
    assert result['Errors'][0]['Key'] == 'object2.txt'
    assert result['Errors'][0]['Message'] == "Deletion failed"

@pytest.mark.asyncio
async def test_delete_objects_internal_error(s3_client_simulator):
    bucket_name = "bucket1"
    delete = {
        'Objects': [
            {'Key': 'object1.txt'}
        ]
    }

    # Mock the delete_object method to raise an internal error
    async def delete_object_mock(bucket, key, version_id, is_sync):
        raise Exception("Internal error occurred")

    s3_client_simulator.delete_object = delete_object_mock

    # Act
    result = await s3_client_simulator.delete_objects(bucket_name, delete)

    # Assert
    assert len(result['Deleted']) == 0
    assert len(result['Errors']) == 1
    assert result['Errors'][0]['Key'] == 'object1.txt'
    assert result['Errors'][0]['Message'] == "Internal error occurred"

@pytest.mark.asyncio
async def test_delete_objects_non_existent_keys(s3_client_simulator):
    bucket_name = "bucket1"
    delete = {
        'Objects': [
            {'Key': 'non_existent_object1.txt'},
            {'Key': 'non_existent_object2.txt'}
        ]
    }

    # Mock the delete_object method to raise an exception for non-existent keys
    async def delete_object_mock(bucket, key, version_id, is_sync):
        raise FileNotFoundError(f"Object {key} not found")

    s3_client_simulator.delete_object = delete_object_mock

    # Act
    result = await s3_client_simulator.delete_objects(bucket_name, delete)

    # Assert
    assert len(result['Deleted']) == 0
    assert len(result['Errors']) == 2
    assert result['Errors'][0]['Key'] == 'non_existent_object1.txt'
    assert result['Errors'][0]['Message'] == "Object non_existent_object1.txt not found"
    assert result['Errors'][1]['Key'] == 'non_existent_object2.txt'
    assert result['Errors'][1]['Message'] == "Object non_existent_object2.txt not found"

@pytest.mark.asyncio
async def test_delete_objects_with_partial_success(s3_client_simulator):
    bucket_name = "bucket1"
    delete = {
        'Objects': [
            {'Key': 'object1.txt'},
            {'Key': 'object2.txt'},
            {'Key': 'object3.txt'}
        ]
    }

    # Mock the delete_object method to succeed for the first and fail for the second
    async def delete_object_mock(bucket, key, version_id, is_sync):
        if key == 'object2.txt':
            raise RuntimeError("Failed to delete")
        return {'DeleteMarker': True}

    s3_client_simulator.delete_object = delete_object_mock

    # Act
    result = await s3_client_simulator.delete_objects(bucket_name, delete)

    # Assert
    assert len(result['Deleted']) == 2
    assert len(result['Errors']) == 1
    assert result['Errors'][0]['Key'] == 'object2.txt'
    assert result['Errors'][0]['Message'] == "Failed to delete"

