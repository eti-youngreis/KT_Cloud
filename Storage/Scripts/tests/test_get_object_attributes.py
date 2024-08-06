import os
import pytest
import asyncio
import json
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from PyStorage import PyStorage

functions = PyStorage()


@pytest.fixture
def create_test_metadata(tmp_path):
    # Create a temporary metadata file
    metadata_file_path = tmp_path / "metadata.json"
    data = {
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
                                    "legalHold": False,
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
                                    "legalHold": True,
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
                    }
                },
                "bucket2": {
                    "objects": {
                        "object3.pdf": {
                            "versions": {
                                "1": {
                                    "etag": "etag4",
                                    "size": 3072,
                                    "lastModified": "2023-05-01T12:00:00Z",
                                    "isLatest": True,
                                    "acl": {
                                        "owner": "user3",
                                        "permissions": ["READ", "WRITE"]
                                    },
                                    "legalHold": False,
                                    "retention": {
                                        "mode": "GOVERNANCE",
                                        "retainUntilDate": "2024-05-01T12:00:00Z"
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
                    }
                }
            }
        }
    }

    with open(metadata_file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    # Load the metadata into functions.metadata_manager.metadata
    with open(metadata_file_path, 'r', encoding='utf-8') as f:
        metadata = json.load(f)
    functions.metadata_manager.metadata = metadata
    functions.metadata_manager.metadata_file=str(metadata_file_path)
    yield metadata_file_path
    
    if os.path.exists(metadata_file_path):
        os.remove(metadata_file_path)


@pytest.mark.asyncio
async def test_get_object_attributes_async_specific_version(create_test_metadata):
    # Test to get attributes for a specific version asynchronously
    bucket = "bucket1"
    key = "object1.txt"
    version_id = "2"
    attributes = await functions.get_object_attributes(bucket, key, version_id=version_id, async_flag=True)
    assert attributes["checksum"] == "abc124"
    assert attributes["ETag"] == "1234567890abcdef"
    assert attributes["ObjectParts"] == "None"
    assert attributes["ObjectSize"] == 67890
    assert attributes["StorageClass"] == "STANDARD"

def test_get_object_attributes_invalid_key(create_test_metadata):
    # Test to ensure FileNotFoundError is raised for an invalid key
    key = "non_existent_file.txt"
    with pytest.raises(FileNotFoundError):
        asyncio.run(functions.get_object_attributes("bucket1", key))

@pytest.mark.asyncio
async def test_get_object_attributes_async_invalid_key(create_test_metadata):
    # Test to ensure FileNotFoundError is raised for an invalid key asynchronously
    key = "non_existent_file.txt"
    with pytest.raises(FileNotFoundError):
        await functions.get_object_attributes("bucket1", key, async_flag=True)

def test_get_object_attributes_no_version_specified( create_test_metadata):
    # Test to get the latest version attributes synchronously without specifying version
    bucket = "bucket1"
    key = "object1.txt"
    attributes = asyncio.run(functions.get_object_attributes(bucket, key))
    assert attributes["checksum"] == "abc124"
    assert attributes["ETag"] == "1234567890abcdef"
    assert attributes["ObjectParts"] == "None"
    assert attributes["ObjectSize"] == 67890
    assert attributes["StorageClass"] == "STANDARD"

@pytest.mark.asyncio
async def test_get_object_attributes_async_no_version_specified(create_test_metadata):
    # Test to get the latest version attributes asynchronously without specifying version
    bucket = "bucket1"
    key = "object1.txt"
    attributes = await functions.get_object_attributes(bucket, key, async_flag=True)
    assert attributes["checksum"] == "abc124"
    assert attributes["ETag"] == "1234567890abcdef"
    assert attributes["ObjectParts"] == "None"
    assert attributes["ObjectSize"] == 67890
    assert attributes["StorageClass"] == "STANDARD"


@pytest.mark.asyncio
async def test_get_object_attributes_async_version_not_found(create_test_metadata):
    # Test to ensure FileNotFoundError is raised when version is not found asynchronously
    bucket = "bucket1"
    key = "object1.txt"
    version_id = "nonexistent_version"
    with pytest.raises(FileNotFoundError):
        await functions.get_object_attributes(bucket, key, version_id=version_id, async_flag=True)

def test_get_object_attributes_sync_version_not_found(create_test_metadata):
    # Test to ensure FileNotFoundError is raised when version is not found synchronously
    bucket = "bucket1"
    key = "object1.txt"
    version_id = "nonexistent_version"
    with pytest.raises(FileNotFoundError):
        asyncio.run(functions.get_object_attributes(bucket, key, version_id=version_id))

@pytest.mark.asyncio
async def test_get_object_attributes_async_bucket_not_found(create_test_metadata):
    # Test to ensure FileNotFoundError is raised when bucket is not found asynchronously
    bucket = "non_existent_bucket"
    key = "object1.txt"
    with pytest.raises(FileNotFoundError):
        await functions.get_object_attributes(bucket, key, async_flag=True)
