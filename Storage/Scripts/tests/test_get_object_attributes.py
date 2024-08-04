import os
import pytest
import asyncio
import json
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from PyStorage import PyStorage

functions = PyStorage()

@pytest.fixture
def create_test_file(tmp_path):
    # Create a temporary test file
    file_path = tmp_path / "test_file.txt"
    with open(file_path, 'w') as f:
        f.write("This is a test file.")
    return file_path

@pytest.fixture
def create_test_metadata(create_test_file, tmp_path):
    # Create a temporary metadata file
    metadata_file_path = tmp_path / "metadata.json"

    file_path = str(create_test_file)
    data = {
        file_path: {
            "versions": {
                "123": {
                    "LegalHold": {"Status": "ON"},
                    "acl": {
                        "owner": "user1",
                        "permissions": {
                            "read": ["user1", "user2"],
                            "write": ["user1"]
                        }
                    },
                    "TagSet": [
                        {"Key": "Key1", "Value": "Value1"},
                        {"Key": "Key2", "Value": "Value2"}
                    ],
                    "ObjectLockConfiguration": {
                        "ObjectLockEnabled": "Enabled",
                        "Rule": {
                            "DefaultRetention": {
                                "Mode": "GOVERNANCE",
                                "Days": 30
                            }
                        }
                    },
                    "LastModified": "2024-07-31T12:00:00Z",
                    "ContentLength": 1024,
                    "ETag": "1234567890abcdef",
                    "ContentType": "text/plain",
                    "Metadata": {},
                    "checksum": "abc124",
                    "ObjectParts": "None",
                    "ObjectSize": 67890,
                    "StorageClass": "STANDARD"
                },
                "12554": {
                    "LegalHold": {"Status": "ON"},
                    "acl": {
                        "owner": "user1",
                        "permissions": {
                            "read": ["user1", "user2"],
                            "write": ["user1"]
                        }
                    },
                    "TagSet": [
                        {"Key": "Key1", "Value": "Value1"},
                        {"Key": "Key2", "Value": "Value2"}
                    ],
                    "ObjectLockConfiguration": {
                        "ObjectLockEnabled": "Disabled",
                        "Rule": {
                            "DefaultRetention": {
                                "Mode": "COMPLIANCE",
                                "Days": 60
                            }
                        }
                    },
                    "LastModified": "2024-07-25T09:00:00Z",
                    "ContentLength": 2048,
                    "ETag": "abcdef1234567890",
                    "ContentType": "application/octet-stream",
                    "Metadata": {},
                    "checksum": "abc124",
                    "ObjectParts": "None",
                    "ObjectSize": 67890,
                    "StorageClass": "STANDARD"
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
    
    yield metadata_file_path
    
    if os.path.exists(metadata_file_path):
        os.remove(metadata_file_path)


def test_get_object_attributes_specific_version(create_test_file, create_test_metadata):
    # Test to get attributes for a specific version synchronously
    key = str(create_test_file)
    version_id = "123"
    attributes = asyncio.run(functions.get_object_attributes(key, version_id))
    assert attributes["checksum"] == "abc124"
    assert attributes["ETag"] == "1234567890abcdef"
    assert attributes["ObjectParts"] == "None"
    assert attributes["ObjectSize"] == 67890
    assert attributes["StorageClass"] == "STANDARD"

@pytest.mark.asyncio
async def test_get_object_attributes_async_specific_version(create_test_file, create_test_metadata):
    # Test to get attributes for a specific version asynchronously
    key = str(create_test_file)
    version_id = "12554"
    attributes = await functions.get_object_attributes(key, version_id=version_id, async_flag=True)
    assert attributes["checksum"] == "abc124"
    assert attributes["ETag"] == "abcdef1234567890"
    assert attributes["ObjectParts"] == "None"
    assert attributes["ObjectSize"] == 67890
    assert attributes["StorageClass"] == "STANDARD"

def test_get_object_attributes_invalid_key( create_test_metadata):
    # Test to ensure FileNotFoundError is raised for an invalid key
    key = "C:/Users/shana/Desktop/ניסיון חדש/non_existent_file.txt"
    with pytest.raises(FileNotFoundError):
        asyncio.run(functions.get_object_attributes(key))

@pytest.mark.asyncio
async def test_get_object_attributes_async_invalid_key( create_test_metadata):
    # Test to ensure FileNotFoundError is raised for an invalid key asynchronously
    key = "C:/Users/shana/Desktop/ניסיון חדש/non_existent_file.txt"
    with pytest.raises(FileNotFoundError):
        await functions.get_object_attributes(key, async_flag=True)

def test_get_object_attributes_no_version_specified(create_test_file, create_test_metadata):
    # Test to get the latest version attributes synchronously without specifying version
    key = str(create_test_file)
    attributes = asyncio.run(functions.get_object_attributes(key))
    assert attributes["checksum"] == "abc124"
    assert attributes["ETag"] == "abcdef1234567890"
    assert attributes["ObjectParts"] == "None"
    assert attributes["ObjectSize"] == 67890
    assert attributes["StorageClass"] == "STANDARD"

@pytest.mark.asyncio
async def test_get_object_attributes_async_no_version_specified(create_test_file, create_test_metadata):
    # Test to get the latest version attributes asynchronously without specifying version
    key = str(create_test_file)
    attributes = await functions.get_object_attributes(key, async_flag=True)
    assert attributes["checksum"] == "abc124"
    assert attributes["ETag"] == "abcdef1234567890"
    assert attributes["ObjectParts"] == "None"
    assert attributes["ObjectSize"] == 67890
    assert attributes["StorageClass"] == "STANDARD"
