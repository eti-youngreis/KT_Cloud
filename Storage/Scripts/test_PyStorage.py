import os
import pytest
import json
from PyStorage import S3ClientSimulator  # Adjust the import based on your module
import asyncio
from unittest import mock
from unittest.mock import AsyncMock, MagicMock
from pathlib import Path
import shutil


@pytest.fixture
def create_test_metadata(tmp_path):
    # Paths for the metadata file and server path
    metadata_file = tmp_path / "metadata.json"
    server_path = tmp_path / "server"

    # Create an instance of S3ClientSimulator
    s3_client = S3ClientSimulator(metadata_file=str(metadata_file), server_path=str(server_path))

    # Set up initial metadata
    metadata = {
        "server": {
            "buckets": {
                "bucket1": {
                    "objects": {
                        "object1.txt": {
                            "versions": {
                                "1": {
                                    "etag": "etag1",
                                    "size": 1024,
                                    "lastModified": "2023-07-01T12:00:00Z",
                                    "isLatest": False,
                                    "acl": {
                                        "owner": "user1",
                                        "permissions": ["READ", "WRITE"]
                                    },
                                    "legalHold": {
                                        "status": "OFF"
                                    },
                                    "retention": {
                                        "mode": "COMPLIANCE",
                                        "retainUntilDate": "2024-07-01T12:00:00Z"
                                    },
                                    "ContentLength": 1024,
                                    "ETag": "1234567890abcdef",
                                    "ContentType": "text/plain",
                                    "metadata": {"custom-metadata": "value"},
                                    "checksum": "abc124",
                                    "ObjectParts": "None",
                                    "ObjectSize": 67890,
                                    "StorageClass": "STANDARD",
                                    "TagSet": [
                                        {"key": "1", "value": "2"},
                                        {"key": "3", "value": "4"}
                                    ]
                                },
                                "2": {
                                    "etag": "etag2",
                                    "size": 2048,
                                    "lastModified": "2023-08-01T12:00:00Z",
                                    "isLatest": True,
                                    "acl": {
                                        "owner": "user1",
                                        "permissions": ["READ"]
                                    },
                                    "legalHold": {
                                        "status": "OFF"
                                    },
                                    "retention": {
                                        "mode": "GOVERNANCE",
                                        "retainUntilDate": "2024-08-01T12:00:00Z"
                                    },
                                    "tagSet": [
                                        {"key": "Key1", "value": "Value1"},
                                        {"key": "Key2", "value": "Value2"}
                                    ],
                                    "LastModified": "2024-07-31T12:00:00Z",
                                    "ContentLength": 1024,
                                    "ETag": "1234567890abcdef",
                                    "ContentType": "text/plain",
                                    "metadata": {"custom-metadata": "value"},
                                    "checksum": "abc124",
                                    "ObjectParts": "None",
                                    "ObjectSize": 67890,
                                    "StorageClass": "STANDARD"
                                }
                            }
                        }
                    },
                    "object_lock": {
                        "ObjectLockEnabled": "Enabled",
                        "Rule": {"DefaultRetention": {"Days": 365}}
                    }
                }
            }
        }
    }

    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=4)

    # Load the metadata into s3_client
    s3_client.metadata_manager.metadata = metadata
    s3_client.metadata_manager.metadata_file = str(metadata_file)

    yield s3_client

    # Cleanup
    if os.path.exists(metadata_file):
        os.remove(metadata_file)
    if os.path.exists(server_path):
        shutil.rmtree(server_path)

# Test cases for head_object
@pytest.mark.asyncio
async def test_head_object_existing_object(create_test_metadata):
    s3_client = create_test_metadata
    response = await s3_client.head_object('bucket1', 'object1.txt')
    assert response['ETag'] == 'etag2'

def test_head_object_non_existing_object(create_test_metadata):
    s3_client = create_test_metadata
    # Use pytest.raises to check for the expected exception
    with pytest.raises(FileNotFoundError, match="Object nonexistent.txt not found in bucket bucket1"):
        asyncio.run(s3_client.head_object('bucket1', 'nonexistent.txt'))

def test_head_object_existing_object_version(create_test_metadata):
    s3_client = create_test_metadata
    response =asyncio.run( s3_client.head_object('bucket1', 'object1.txt',version_id='1')) 
    assert response['ETag'] == 'etag1'

@pytest.mark.asyncio
async def test_head_object_no_version_id(create_test_metadata):
    s3_client = create_test_metadata
    response = await s3_client.head_object('bucket1', 'object1.txt')
    assert response['ETag'] == 'etag2'


# # Test cases for get_object_torrent
def test_get_object_torrent_existing_object(create_test_metadata):
    s3_client = create_test_metadata
    response =asyncio.run(s3_client.get_object_torrent('bucket1', 'object1.txt')) 
    assert response is not None  # Adjust assertion based on expected response

@pytest.mark.asyncio
async def test_get_object_torrent_non_existing_object(create_test_metadata):
    s3_client = create_test_metadata
    with pytest.raises(FileNotFoundError, match="Object Object key  not found in bucket bucket1"):
        await s3_client.get_object_torrent('bucket1', 'Object key ')
    

def test_get_object_torrent_existing_object_version(create_test_metadata):
    s3_client = create_test_metadata
    response =asyncio.run(s3_client.get_object_torrent('bucket1', 'object1.txt', version_id='1')) 
    assert response is not None  # Adjust assertion based on expected response

def test_get_object_torrent_no_version_id(create_test_metadata):
    s3_client = create_test_metadata
    response =asyncio.run( s3_client.get_object_torrent('bucket1', 'object1.txt'))
    assert response is not None  # Adjust assertion based on expected response


# Test cases for get_object_lock_configuration
def test_get_object_lock_configuration_existing_bucket(create_test_metadata):
    s3_client = create_test_metadata
    response =asyncio.run( s3_client.get_object_lock_configuration('bucket1'))
    assert response['ObjectLockConfiguration'] == {'LockConfiguration': {}, 'ObjectLockEnabled': 'DISABLED'}

@pytest.mark.asyncio
async def test_get_object_lock_configuration_non_existing_bucket(create_test_metadata):
    s3_client = create_test_metadata
    with pytest.raises(FileNotFoundError, match="Bucket 'invalid_bucket' not found."):
        await s3_client.get_object_lock_configuration('invalid_bucket')


@pytest.mark.asyncio
async def test_get_object_lock_configuration_bucket_not_found(create_test_metadata):
    s3_client = create_test_metadata
    with pytest.raises(FileNotFoundError, match="Bucket 'bucket2' not found."):
        await s3_client.get_object_lock_configuration('bucket2')



@pytest.mark.asyncio
async def test_put_object_acl_valid(create_test_metadata):
    s3_client = create_test_metadata
    response = await s3_client.put_object_acl('bucket1', 'object1.txt', acl={'owner': 'user2', 'permissions': ['READ', 'WRITE']})
    # Verify ACL change
    acl = s3_client.metadata_manager.metadata['server']['buckets']['bucket1']['objects']['object1.txt']['versions']
    assert 'acl' in acl['2'] and acl['2']['acl'] == {'owner': 'user2', 'permissions': ['READ', 'WRITE']}

@pytest.mark.asyncio
async def test_put_object_acl_invalid_bucket(create_test_metadata):
    s3_client = create_test_metadata
    with pytest.raises(FileNotFoundError, match="Bucket 'invalid_bucket' does not exist"):
        await s3_client.put_object_acl('invalid_bucket', 'object1.txt', acl={'owner': 'user2', 'permissions': ['READ']})

@pytest.mark.asyncio
async def test_put_object_acl_invalid_key(create_test_metadata):
    s3_client = create_test_metadata
    with pytest.raises(FileNotFoundError, match="Object 'nonexistent.txt' does not exist in bucket 'bucket1'"):
        await s3_client.put_object_acl('bucket1', 'nonexistent.txt', acl={'owner': 'user2', 'permissions': ['READ']})

@pytest.mark.asyncio
async def test_put_object_acl_empty_acl(create_test_metadata):
    s3_client = create_test_metadata
    response = await s3_client.put_object_acl('bucket1', 'object1.txt', acl={})
    # Verify ACL is updated to empty
    acl = s3_client.metadata_manager.metadata['server']['buckets']['bucket1']['objects']['object1.txt']['versions']
    assert 'acl' in acl['2'] and acl['2']['acl'] == {}


class CustomAsyncMock(MagicMock):
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass
@pytest.fixture(autouse=True)
def mock_aiofiles():
    with mock.patch('aiofiles.open', new_callable=CustomAsyncMock) as mock_open:
        yield mock_open

@pytest.mark.asyncio
async def test_put_object_valid(create_test_metadata):
    s3_client = create_test_metadata
    body = b"Test content"
    # Create the required directories if they don't exist
    bucket_path = Path(s3_client.URL_SERVER +"/"+ 'bucket1')
    if not bucket_path.exists():
        bucket_path.mkdir(parents=True)
    response = await s3_client.put_object('bucket1', 'object1.txt', body)
    assert response['ETag'] is not None
    assert response['VersionId'] == '3'

@pytest.mark.asyncio
async def test_put_object_no_bucket(create_test_metadata):
    s3_client = create_test_metadata
    body = b"Test content no bucket"
    response = await s3_client.put_object('bucket_not_exist', 'object1.txt', body)
    assert response['ETag'] is not None
    assert response['VersionId'] == '1'
    assert 'bucket_not_exist' in s3_client.metadata_manager.metadata["server"]["buckets"]

@pytest.mark.asyncio
async def test_put_object_empty_body(create_test_metadata):
    s3_client = create_test_metadata
    body = b""
    bucket_path = Path(s3_client.URL_SERVER) / 'bucket1'
    if not bucket_path.exists():
        bucket_path.mkdir(parents=True)
    response = await s3_client.put_object('bucket1', 'object1.txt', body)
    assert response['ETag'] is not None
    assert response['VersionId'] == '3'



