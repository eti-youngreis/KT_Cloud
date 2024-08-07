import os
import pytest
import asyncio
import json
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from PyStorage import S3ClientSimulator

functions = S3ClientSimulator()

@pytest.fixture
def create_test_metadata(tmp_path):
    metadata_file_path = tmp_path / "metadata.json"
    print(metadata_file_path,"הדפסה תחילתית")
    print(metadata_file_path,"ddddddddd")
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
                                    "TagSet": [
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
                                    "TagSet": [
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
    print(metadata_file_path,"lllllll")
    with open(metadata_file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    with open(metadata_file_path, 'r', encoding='utf-8') as f:
        metadata = json.load(f)
    functions.metadata_manager.metadata = metadata
    functions.metadata_manager.metadata_file=str(metadata_file_path)
    yield metadata_file_path
    
    if os.path.exists(metadata_file_path):
        os.remove(metadata_file_path)

def test_put_object_tagging(create_test_metadata):
    # Test putting object tagging with valid tags and version ID
    bucket = 'bucket1'
    key = 'object1.txt'
    tags = {'TagSet':[{'key': '111', 'value': 'Te22st'}]}
    print(f"Metadata file path: {functions.metadata_manager.metadata }")

    asyncio.run(functions.put_object_tagging(bucket, key, tags, version_id='2', sync_flag=False))
    asyncio.run(functions.put_object_tagging(bucket, key, tags, version_id='2', sync_flag=True))

    metadata = functions.metadata_manager.metadata
    assert metadata['server']['buckets'][bucket]['objects'][key]['versions']['2']['TagSet'] == tags["TagSet"]

def test_put_object_tagging_file_without_tags(create_test_metadata):
    # Test putting object tagging with empty tags list and version ID
    bucket = 'bucket1'
    key = 'object1.txt'
    tags = {'TagSet':[]}

    asyncio.run(functions.put_object_tagging(bucket, key, tags, version_id='2', sync_flag=False))
    asyncio.run(functions.put_object_tagging(bucket, key, tags, version_id='2', sync_flag=True))

    metadata = functions.metadata_manager.metadata
    assert metadata['server']['buckets'][bucket]['objects'][key]['versions']['2']['TagSet'] == tags["TagSet"]

def test_put_object_tagging_empty_values_sync(create_test_metadata):
    # Test putting object tagging with empty key and value in tags list
    bucket = 'bucket1'
    key = 'object1.txt'
    tags = {'TagSet':[{'key': '', 'value': ''}]}

    asyncio.run(functions.put_object_tagging(bucket, key, tags, version_id='2', sync_flag=False))
    asyncio.run(functions.put_object_tagging(bucket, key, tags, version_id='2', sync_flag=True))

    metadata = functions.metadata_manager.metadata
    assert metadata['server']['buckets'][bucket]['objects'][key]['versions']['2']['TagSet'] == tags["TagSet"]

def test_put_object_tagging_single_key_empty_value(create_test_metadata):
    # Test putting object tagging with a single key and empty value
    bucket = 'bucket1'
    key = 'object1.txt'
    tags = {'TagSet':[{'key': 'SingleKey', 'value': ''}]}

    asyncio.run(functions.put_object_tagging(bucket, key, tags, version_id='2', sync_flag=True))
    asyncio.run(functions.put_object_tagging(bucket, key, tags, version_id='2', sync_flag=False))

    metadata = functions.metadata_manager.metadata
    assert metadata['server']['buckets'][bucket]['objects'][key]['versions']['2']['TagSet'] == tags["TagSet"]

def test_put_object_tagging_multiple_keys(create_test_metadata):
    # Test putting object tagging with multiple keys and values
    bucket = 'bucket1'
    key = 'object1.txt'
    tags = {'TagSet':[{'key': 'Key1', 'value': 'Value1'}, {'key': 'Key2', 'value': 'Value2'}]}

    asyncio.run(functions.put_object_tagging(bucket, key, tags, version_id='2', sync_flag=False))
    asyncio.run(functions.put_object_tagging(bucket, key, tags, version_id='2', sync_flag=True))

    metadata = functions.metadata_manager.metadata
    assert metadata['server']['buckets'][bucket]['objects'][key]['versions']['2']['TagSet'] == tags["TagSet"]

def test_put_object_tagging_invalid_arguments(create_test_metadata):
    # Test putting object tagging with invalid async_flag argument
    bucket = 'bucket1'
    key = 'object1.txt'
    tags ={'TagSet': [{'key': 'Invalid', 'value': 'Arguments'}]}

    with pytest.raises(TypeError):
        asyncio.run(functions.put_object_tagging(bucket, key, tags, version_id='2', sync_flag='a'))
        asyncio.run(functions.put_object_tagging(bucket, key, tags, version_id='2', sync_flag='m'))

def test_put_object_tagging_empty_file_path(create_test_metadata):
    # Test putting object tagging with an empty file path and valid version ID
    bucket = 'bucket1'
    key = 'object1.txt'
    tags = {'TagSet':[]}

    asyncio.run(functions.put_object_tagging(bucket, key, tags, version_id='2', sync_flag=False))
    asyncio.run(functions.put_object_tagging(bucket, key, tags, version_id='2', sync_flag=True))

    metadata = functions.metadata_manager.metadata
    assert metadata['server']['buckets'][bucket]['objects'][key]['versions']['2']['TagSet'] == tags["TagSet"]

def test_put_object_tagging_large_metadata(create_test_metadata):
    # Test putting object tagging with a large number of tags
    bucket = 'bucket1'
    key = 'object1.txt'
    large_tags =[{'key': f'Key{i}', 'value': f'Value{i}'} for i in range(1000)]
    tags={'TagSet':large_tags}
    print(tags)

    asyncio.run(functions.put_object_tagging(bucket, key, tags, version_id='2', sync_flag=False))
    asyncio.run(functions.put_object_tagging(bucket, key, tags, version_id='2', sync_flag=True))

    metadata = functions.metadata_manager.metadata
    assert metadata['server']['buckets'][bucket]['objects'][key]['versions']['2']['TagSet'] == large_tags

def test_put_object_tagging_special_characters(create_test_metadata):
    # Test putting object tagging with special characters in tags
    bucket = 'bucket1'
    key = 'object1.txt'
    tags = {'TagSet':[{'key': 'Key#1', 'value': 'Value@1'}, {'key': 'Key&2', 'value': 'Value*2'}]}

    asyncio.run(functions.put_object_tagging(bucket, key, tags, version_id='2', sync_flag=False))
    asyncio.run(functions.put_object_tagging(bucket, key, tags, version_id='2', sync_flag=True))

    metadata = functions.metadata_manager.metadata
    assert metadata['server']['buckets'][bucket]['objects'][key]['versions']['2']['TagSet'] == tags["TagSet"]
