import tempfile
import os
from pathlib import Path
import json
from unittest.mock import AsyncMock, patch

import aiofiles
import pytest

from PyStorage import S3ClientSimulator

@pytest.fixture(scope='module')
def setup_temp_files():
    temp_metadata_file = tempfile.NamedTemporaryFile(delete=False)
    temp_metadata_file.close()
    temp_server_dir = tempfile.TemporaryDirectory()

    metadata_content = {
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
                                    "isLatest": True
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    with open(temp_metadata_file.name, 'w', encoding='utf-8') as f:
        json.dump(metadata_content, f)

    yield temp_metadata_file.name, temp_server_dir.name

    os.remove(temp_metadata_file.name)
    temp_server_dir.cleanup()


async def test_delete_object_success(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
        mock_delete_object.return_value = True
        client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
        bucket_name = "bucket1"
        key = "object1.txt"

        # Create the file to be deleted
        file_path = Path(temp_server_dir) / bucket_name / key
        os.makedirs(file_path.parent, exist_ok=True)
        with open(file_path, 'w') as f:
            f.write("Test data")

        result = await client.delete_object(bucket_name, key)

        assert result['DeleteMarker']
        assert not file_path.exists()


async def test_delete_object_no_metadata_entry(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
        mock_delete_object.return_value = False
        client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
        bucket_name = "bucket1"
        key = "non_existent.txt"

        result = await client.delete_object(bucket_name, key)

        assert result == {}


async def test_delete_object_no_file(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
        mock_delete_object.return_value = True
        client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
        bucket_name = "bucket1"
        key = "non_existent.txt"

        result = await client.delete_object(bucket_name, key)

        assert result['DeleteMarker']


async def test_delete_object_metadata_error(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
        mock_delete_object.side_effect = Exception("Metadata error")
        client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
        bucket_name = "bucket1"
        key = "object1.txt"

        with pytest.raises(Exception):
            await client.delete_object(bucket_name, key)


async def test_delete_object_is_sync_false(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
        mock_delete_object.return_value = True
        client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
        bucket_name = "bucket1"
        key = "object1.txt"

        # Create the file to be deleted
        file_path = Path(temp_server_dir) / bucket_name / key
        os.makedirs(file_path.parent, exist_ok=True)
        with open(file_path, 'w') as f:
            f.write("Test data")

        result = await client.delete_object(bucket_name, key, is_sync=False)

        assert result['DeleteMarker']
        assert not file_path.exists()


# async def test_delete_object_without_mock(setup_temp_files):
#     temp_metadata_file, temp_server_dir = setup_temp_files
#     client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
#     bucket_name = "bucket1"
#     key = "object1.txt"
#
#     # Create the file to be deleted
#     file_path = Path(temp_server_dir) / bucket_name / key
#     os.makedirs(file_path.parent, exist_ok=True)
#     with open(file_path, 'w') as f:
#         f.write("Test data")
#
#     result = await client.delete_object(bucket_name, key)
#
#     assert result['DeleteMarker']
#     assert not file_path.exists()
#

async def test_delete_object_no_buckets(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
        mock_delete_object.return_value = True
        client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
        bucket_name = "bucket2"
        key = "object2.txt"

        result = await client.delete_object(bucket_name, key)

        assert result['DeleteMarker']


async def test_delete_object_empty_bucket(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
        mock_delete_object.return_value = True
        client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
        bucket_name = "bucket1"
        key = "empty.txt"

        result = await client.delete_object(bucket_name, key)

        assert result['DeleteMarker']


async def test_delete_object_with_nonexistent_version(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
        mock_delete_object.return_value = False
        client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
        bucket_name = "bucket1"
        key = "object1.txt"

        result = await client.delete_object(bucket_name, key)

        assert result == {}


async def test_delete_object_empty_file(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
        mock_delete_object.return_value = True
        client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
        bucket_name = "bucket1"
        key = "empty.txt"

        file_path = Path(temp_server_dir) / bucket_name / key
        os.makedirs(file_path.parent, exist_ok=True)
        with open(file_path, 'w') as f:
            f.write("")

        result = await client.delete_object(bucket_name, key)

        assert result['DeleteMarker']
        assert not file_path.exists()


async def test_delete_objects_success(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object, \
         patch('MetaData.MetadataManager.delete_version', new_callable=AsyncMock) as mock_delete_version:
        mock_delete_object.return_value = True
        mock_delete_version.return_value = True
        client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
        bucket_name = "bucket1"
        delete = {'Objects': [{'Key': 'object1.txt'}]}

        # Create the file to be deleted
        file_path = Path(temp_server_dir) / bucket_name / 'object1.txt'
        os.makedirs(file_path.parent, exist_ok=True)
        with open(file_path, 'w') as f:
            f.write("Test data")

        result = await client.delete_objects(bucket_name, delete)

        assert result['Deleted']
        assert not file_path.exists()
        assert not result['Errors']


async def test_delete_objects_no_metadata_entry(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
        mock_delete_object.return_value = False
        client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
        bucket_name = "bucket1"
        delete = {'Objects': [{'Key': 'non_existent.txt'}]}

        result = await client.delete_objects(bucket_name, delete)

        assert not result['Deleted']
        assert result['Errors']


async def test_delete_objects_partial_success(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
        mock_delete_object.side_effect = [True, False]
        client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
        bucket_name = "bucket1"
        delete = {'Objects': [{'Key': 'object1.txt'}, {'Key': 'non_existent.txt'}]}

        # Create the file to be deleted
        file_path = Path(temp_server_dir) / bucket_name / 'object1.txt'
        os.makedirs(file_path.parent, exist_ok=True)
        with open(file_path, 'w') as f:
            f.write("Test data")

        result = await client.delete_objects(bucket_name, delete)

        assert len(result['Deleted']) == 1
        assert len(result['Errors']) == 1
        assert not file_path.exists()


async def test_delete_objects_with_version_id(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    with patch('MetaData.MetadataManager.delete_version', new_callable=AsyncMock) as mock_delete_version:
        mock_delete_version.return_value = True
        client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
        bucket_name = "bucket1"
        delete = {'Objects': [{'Key': 'object1.txt', 'VersionId': '1'}]}

        # Create the file to be deleted
        file_path = Path(temp_server_dir) / bucket_name / 'object1.txt'
        os.makedirs(file_path.parent, exist_ok=True)
        with open(file_path, 'w') as f:
            f.write("Test data")

        result = await client.delete_objects(bucket_name, delete)

        assert result['Deleted']
        assert not file_path.exists()
        assert not result['Errors']


async def test_delete_objects_metadata_error(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
        mock_delete_object.side_effect = Exception("Metadata error")
        client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
        bucket_name = "bucket1"
        delete = {'Objects': [{'Key': 'object1.txt'}]}

        result = await client.delete_objects(bucket_name, delete)

        assert not result['Deleted']
        assert len(result['Errors']) == 1


async def test_delete_objects_is_sync_false(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
        mock_delete_object.return_value = True
        client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
        bucket_name = "bucket1"
        delete = {'Objects': [{'Key': 'object1.txt'}]}

        # Create the file to be deleted
        file_path = Path(temp_server_dir) / bucket_name / 'object1.txt'
        os.makedirs(file_path.parent, exist_ok=True)
        with open(file_path, 'w') as f:
            f.write("Test data")

        result = await client.delete_objects(bucket_name, delete, is_sync=False)

        assert result['Deleted']
        assert not file_path.exists()
        assert not result['Errors']


async def test_delete_objects_without_mock(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
    bucket_name = "bucket1"
    delete = {'Objects': [{'Key': 'object1.txt'}]}

    # Create the file to be deleted
    file_path = Path(temp_server_dir) / bucket_name / 'object1.txt'
    os.makedirs(file_path.parent, exist_ok=True)
    with open(file_path, 'w') as f:
        f.write("Test data")

    result = await client.delete_objects(bucket_name, delete)

    assert result['Deleted']
    assert not file_path.exists()
    assert not result['Errors']


async def test_delete_objects_no_buckets(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
        mock_delete_object.return_value = True
        client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
        bucket_name = "bucket2"
        delete = {'Objects': [{'Key': 'object2.txt'}]}

        result = await client.delete_objects(bucket_name, delete)

        assert result['Deleted']
        assert not result['Errors']


async def test_delete_objects_empty_bucket(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
        mock_delete_object.return_value = True
        client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
        bucket_name = "bucket1"
        delete = {'Objects': [{'Key': 'empty.txt'}]}

        result = await client.delete_objects(bucket_name, delete)

        assert result['Deleted']
        assert not result['Errors']


async def test_delete_objects_with_nonexistent_version(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    with patch('MetaData.MetadataManager.delete_version', new_callable=AsyncMock) as mock_delete_version:
        mock_delete_version.return_value = False
        client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
        bucket_name = "bucket1"
        delete = {'Objects': [{'Key': 'object1.txt', 'VersionId': 'nonexistent'}]}

        result = await client.delete_objects(bucket_name, delete)

        assert not result['Deleted']
        assert result['Errors']


async def test_get_object_unexpected_error(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
    bucket_name = "bucket1"
    key = "object1.txt"

    with patch('PyStorage.MetadataManager.get_latest_version', side_effect=Exception("Unexpected error")):
        with pytest.raises(RuntimeError):
            await client.get_object(bucket_name, key)


# async def test_get_object_success(setup_temp_files):
#     temp_metadata_file, temp_server_dir = setup_temp_files
#     client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
#     bucket_name = "bucket1"
#     key = "object1.txt"
#
#     # Create the file to be retrieved
#     file_path = Path(temp_server_dir) / bucket_name / key
#     os.makedirs(file_path.parent, exist_ok=True)
#     with open(file_path, 'w') as f:
#         f.write("Test data")
#
#     result = await client.get_object(bucket_name, key)
#
#     assert result['Body'] == b"Test data"
#     assert result['ContentLength'] == 9
#     assert result['ContentType'] == 'application/octet-stream'
#     assert result['ETag'] == 'etag1'
#     assert result['Metadata'] == {}
#     assert result['LastModified'] == "2023-07-01T12:00:00Z"


async def test_get_object_no_metadata(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
    bucket_name = "bucket1"
    key = "object2.txt"

    # Create the file to be retrieved
    file_path = Path(temp_server_dir) / bucket_name / key
    os.makedirs(file_path.parent, exist_ok=True)
    with open(file_path, 'w') as f:
        f.write("Another test data")

    # Modify metadata to not include key
    metadata_content = {
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
                                    "isLatest": True
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    with open(temp_metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata_content, f)

    with pytest.raises(RuntimeError):
        await client.get_object(bucket_name, key)


# async def test_get_object_invalid_metadata(setup_temp_files):
#     temp_metadata_file, temp_server_dir = setup_temp_files
#     client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
#     bucket_name = "bucket1"
#     key = "object1.txt"
#
#     # Corrupt the metadata
#     metadata_content = {
#         "server": {
#             "buckets": {
#                 "bucket1": {
#                     "objects": {
#                         "object1.txt": {
#                             "versions": {
#                                 "1": {
#                                     "size": 1024,
#                                     "lastModified": "2023-07-01T12:00:00Z",
#                                     "isLatest": True
#                                 }
#                             }
#                         }
#                     }
#                 }
#             }
#         }
#     }
#     with open(temp_metadata_file, 'w', encoding='utf-8') as f:
#         json.dump(metadata_content, f)
#
#     with pytest.raises(RuntimeError):
#         await client.get_object(bucket_name, key)


async def test_get_object_no_bucket(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
    bucket_name = "non_existent_bucket"
    key = "object1.txt"

    with pytest.raises(RuntimeError):
        await client.get_object(bucket_name, key)


async def test_get_object_invalid_json(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
    bucket_name = "bucket1"
    key = "object1.txt"

    # Corrupt the metadata JSON
    with open(temp_metadata_file, 'w', encoding='utf-8') as f:
        f.write("invalid json")

    with pytest.raises(RuntimeError):
        await client.get_object(bucket_name, key)


# async def test_get_object_acl_success(setup_temp_files):
#     temp_metadata_file, temp_server_dir = setup_temp_files
#     client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
#     bucket_name = "bucket1"
#     key = "object1.txt"
#
#     result = await client.get_object_acl(bucket_name, key)
#
#     metadata_content = {
#         "server": {
#             "buckets": {
#                 "bucket1": {
#                     "objects": {
#                         "object1.txt": {
#                             "versions": {
#                                 "1": {
#                                     "acl": {
#                                         'owner': 'owner1',
#                                         'permissions': ['READ', 'WRITE']
#                                     }
#                                 }
#                             }
#                         }
#                     }
#                 }
#             }
#         }
#     }
#     assert result['Owner'] == {'DisplayName': 'owner1', 'ID': 'owner1'}
#     assert result['Grants'] == ['READ', 'WRITE']
#

async def test_get_object_acl_missing_acl(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    # Remove 'acl' from metadata
    metadata_content = {
        "server": {
            "buckets": {
                "bucket1": {
                    "objects": {
                        "object1.txt": {
                            "versions": {
                                "1": {}
                            }
                        }
                    }
                }
            }
        }
    }
    with open(temp_metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata_content, f)

    client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
    bucket_name = "bucket1"
    key = "object1.txt"

    result = await client.get_object_acl(bucket_name, key)

    assert result == {
        'Owner': {'DisplayName': 'unknown', 'ID': 'unknown'},
        'Grants': []
    }


async def test_get_object_acl_no_bucket(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
    bucket_name = "non_existent_bucket"
    key = "object1.txt"

    with pytest.raises(RuntimeError):
        await client.get_object_acl(bucket_name, key)


async def test_get_object_acl_no_key(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
    bucket_name = "bucket1"
    key = "non_existent_key.txt"

    with pytest.raises(RuntimeError):
        await client.get_object_acl(bucket_name, key)


async def test_get_object_acl_partial_metadata(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    # Remove some metadata fields
    metadata_content = {
        "server": {
            "buckets": {
                "bucket1": {
                    "objects": {
                        "object1.txt": {
                            "versions": {
                                "1": {}
                            }
                        }
                    }
                }
            }
        }
    }
    with open(temp_metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata_content, f)

    client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
    bucket_name = "bucket1"
    key = "object1.txt"

    result = await client.get_object_acl(bucket_name, key)

    assert result == {
        'Owner': {'DisplayName': 'unknown', 'ID': 'unknown'},
        'Grants': []
    }


async def test_get_object_acl_metadata_error(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    with patch('PyStorage.MetadataManager.get_latest_version', new_callable=AsyncMock) as mock_get_latest_version:
        mock_get_latest_version.side_effect = Exception("Metadata error")
        client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
        bucket_name = "bucket1"
        key = "object1.txt"
        with pytest.raises(RuntimeError):
            await client.get_object_acl(bucket_name, key)


async def test_copy_object_key_error(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    with patch('MetaData.MetadataManager.copy_metadata', new_callable=AsyncMock) as mock_copy_metadata:
        mock_copy_metadata.side_effect = KeyError("Some key error")
        client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
        bucket_name = "bucket2"
        key = "copied_object.txt"
        copy_source = {'Bucket': 'bucket1', 'Key': 'object1.txt'}

        # Create the source file
        source_file_path = Path(temp_server_dir) / copy_source['Bucket'] / copy_source['Key']
        os.makedirs(source_file_path.parent, exist_ok=True)
        with open(source_file_path, 'w') as f:
            f.write("Test data")

        with pytest.raises(RuntimeError) as cm:
            await client.copy_object(bucket_name, copy_source, key)

        assert str(cm.value) == "Metadata key error: 'Some key error'"


async def test_copy_object_without_mock(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
    bucket_name = "bucket2"
    key = "copied_object.txt"
    copy_source = {'Bucket': 'bucket1', 'Key': 'object1.txt'}

    # Create the source file
    source_file_path = Path(temp_server_dir) / copy_source['Bucket'] / copy_source['Key']
    os.makedirs(source_file_path.parent, exist_ok=True)
    with open(source_file_path, 'w') as f:
        f.write("Test data")

    result = await client.copy_object(bucket_name, copy_source, key)

    assert result['CopyObjectResult']['ETag']
    assert result['CopyObjectResult']['LastModified']


async def test_copy_object_non_existent_source_key(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
    bucket_name = "bucket2"
    key = "copied_object.txt"
    copy_source = {'Bucket': 'bucket1', 'Key': 'non_existent_key.txt'}

    with pytest.raises(RuntimeError):
        await client.copy_object(bucket_name, copy_source, key)


async def test_copy_object_invalid_destination_bucket(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    with patch('MetaData.MetadataManager.copy_metadata', new_callable=AsyncMock) as mock_copy_metadata:
        client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
        bucket_name = "invalid_bucket"
        key = "copied_object.txt"
        copy_source = {'Bucket': 'bucket1', 'Key': 'object1.txt'}

        with pytest.raises(RuntimeError):
            await client.copy_object(bucket_name, copy_source, key)


async def test_copy_object_metadata_corruption(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    with patch('MetaData.MetadataManager.copy_metadata', new_callable=AsyncMock) as mock_copy_metadata:
        client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
        bucket_name = "bucket2"
        key = "corrupted_copied_object.txt"
        copy_source = {'Bucket': 'bucket1', 'Key': 'object1.txt'}

        # Corrupt metadata
        metadata_content = {
            "server": {
                "buckets": {
                    "bucket1": {
                        "objects": {
                            "object1.txt": {
                                "versions": {
                                    "1": {
                                        "size": 1024,
                                        "lastModified": "2023-07-01T12:00:00Z",
                                        "isLatest": True
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        with open(temp_metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata_content, f)

        with pytest.raises(RuntimeError):
            await client.copy_object(bucket_name, copy_source, key)


async def test_copy_object_no_source_bucket(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
    bucket_name = "bucket2"
    key = "copied_object.txt"
    copy_source = {'Bucket': 'no_source_bucket', 'Key': 'object1.txt'}

    with pytest.raises(RuntimeError):
        await client.copy_object(bucket_name, copy_source, key)


async def test_copy_object_no_source_key(setup_temp_files):
    temp_metadata_file, temp_server_dir = setup_temp_files
    client = S3ClientSimulator(temp_metadata_file, temp_server_dir)
    bucket_name = "bucket2"
    key = "copied_object.txt"
    copy_source = {'Bucket': 'bucket1', 'Key': 'no_source_key.txt'}

    with pytest.raises(RuntimeError):
        await client.copy_object(bucket_name, copy_source, key)
