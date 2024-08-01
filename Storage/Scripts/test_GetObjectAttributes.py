import pytest
import asyncio
import json
import os

from PyStorage import PyStorage

functions = PyStorage()
TEST_METADATA_FILE = "test_metadata.json"

@pytest.fixture
def create_test_file(tmp_path):
    file_path = tmp_path / "test_file.txt"
    with open(file_path, 'w') as f:
        f.write("This is a test file.")
    return file_path

@pytest.fixture
def create_test_metadata(create_test_file):
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
    with open(TEST_METADATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    yield data
    
    if os.path.exists(TEST_METADATA_FILE):
        os.remove(TEST_METADATA_FILE)

def load_test_metadata():
    # Helper function to load the test metadata
    with open(TEST_METADATA_FILE, 'r', encoding='utf-8') as f:
        metadata = json.load(f)
    functions.metadata_manager.metadata = metadata

def test_get_object_attributes_sync_latest_version(create_test_file, create_test_metadata):
    load_test_metadata()
    
    key = str(create_test_file)
    print(f"Test key: {key}")
    
    attributes = asyncio.run(functions.get_object_attributes(key))
    
    print(f"Attributes: {attributes}")
    
    assert attributes["checksum"] == "abc124"
    assert attributes["ETag"] == "abcdef1234567890"
    assert attributes["ObjectParts"] == "None"
    assert attributes["ObjectSize"] == 67890
    assert attributes["StorageClass"] == "STANDARD"


def test_get_object_attributes_specific_version(create_test_file, create_test_metadata):
    # Test to get attributes for a specific version synchronously
    load_test_metadata()
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
    load_test_metadata()
    key = str(create_test_file)
    version_id = "12554"
    attributes = await functions.get_object_attributes(key, version_id=version_id, async_mode=True)
    assert attributes["checksum"] == "abc124"
    assert attributes["ETag"] == "abcdef1234567890"
    assert attributes["ObjectParts"] == "None"
    assert attributes["ObjectSize"] == 67890
    assert attributes["StorageClass"] == "STANDARD"

def test_get_object_attributes_invalid_key(create_test_file, create_test_metadata):
    # Test to ensure FileNotFoundError is raised for an invalid key
    load_test_metadata()
    key = "C:/Users/shana/Desktop/ניסיון חדש/non_existent_file.txt"
    with pytest.raises(FileNotFoundError):
        asyncio.run(functions.get_object_attributes(key))

@pytest.mark.asyncio
async def test_get_object_attributes_async_invalid_key(create_test_file, create_test_metadata):
    # Test to ensure FileNotFoundError is raised for an invalid key asynchronously
    load_test_metadata()
    key = "C:/Users/shana/Desktop/ניסיון חדש/non_existent_file.txt"
    with pytest.raises(FileNotFoundError):
        await functions.get_object_attributes(key, async_mode=True)

def test_get_object_attributes_no_version_specified(create_test_file, create_test_metadata):
    # Test to get the latest version attributes synchronously without specifying version
    load_test_metadata()
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
    load_test_metadata()
    key = str(create_test_file)
    attributes = await functions.get_object_attributes(key, async_mode=True)
    assert attributes["checksum"] == "abc124"
    assert attributes["ETag"] == "abcdef1234567890"
    assert attributes["ObjectParts"] == "None"
    assert attributes["ObjectSize"] == 67890
    assert attributes["StorageClass"] == "STANDARD"

