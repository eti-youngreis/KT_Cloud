import asyncio
import unittest
from unittest.mock import AsyncMock, patch, MagicMock
import tempfile
import os
from pathlib import Path
import json

import aiofiles
import pytest

from PyStorage import S3ClientSimulator

class TestS3ClientSimulator(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp_metadata_file = tempfile.NamedTemporaryFile(delete=False)
        self.temp_metadata_file.close()
        self.temp_server_dir = tempfile.TemporaryDirectory()
        self.metadata_content = {
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

        with open(self.temp_metadata_file.name, 'w', encoding='utf-8') as f:
            json.dump(self.metadata_content, f)

    def tearDown(self):
        os.remove(self.temp_metadata_file.name)
        self.temp_server_dir.cleanup()

    async def test_delete_object_success(self):
        with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
            mock_delete_object.return_value = True
            client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
            bucket_name = "bucket1"
            key = "object1.txt"

            # Create the file to be deleted
            file_path = Path(self.temp_server_dir.name) / bucket_name / key
            os.makedirs(file_path.parent, exist_ok=True)
            with open(file_path, 'w') as f:
                f.write("Test data")

            result = await client.delete_object(bucket_name, key)

            self.assertTrue(result['DeleteMarker'])
            self.assertFalse(file_path.exists())

    async def test_delete_object_no_metadata_entry(self):
        with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
            mock_delete_object.return_value = False
            client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
            bucket_name = "bucket1"
            key = "non_existent.txt"

            result = await client.delete_object(bucket_name, key)

            self.assertEqual(result, {})

    async def test_delete_object_no_file(self):
        with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
            mock_delete_object.return_value = True
            client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
            bucket_name = "bucket1"
            key = "non_existent.txt"

            result = await client.delete_object(bucket_name, key)

            self.assertTrue(result['DeleteMarker'])

    async def test_delete_object_metadata_error(self):
        with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
            mock_delete_object.side_effect = Exception("Metadata error")
            client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
            bucket_name = "bucket1"
            key = "object1.txt"

            with self.assertRaises(Exception):
                await client.delete_object(bucket_name, key)

    async def test_delete_object_is_sync_false(self):
        with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
            mock_delete_object.return_value = True
            client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
            bucket_name = "bucket1"
            key = "object1.txt"

            # Create the file to be deleted
            file_path = Path(self.temp_server_dir.name) / bucket_name / key
            os.makedirs(file_path.parent, exist_ok=True)
            with open(file_path, 'w') as f:
                f.write("Test data")

            result = await client.delete_object(bucket_name, key, is_sync=False)

            self.assertTrue(result['DeleteMarker'])
            self.assertFalse(file_path.exists())

    async def test_delete_object_without_mock(self):
        client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
        bucket_name = "bucket1"
        key = "object1.txt"

        # Create the file to be deleted
        file_path = Path(self.temp_server_dir.name) / bucket_name / key
        os.makedirs(file_path.parent, exist_ok=True)
        with open(file_path, 'w') as f:
            f.write("Test data")

        result = await client.delete_object(bucket_name, key)

        self.assertTrue(result['DeleteMarker'])
        self.assertFalse(file_path.exists())

    async def test_delete_object_no_buckets(self):
        with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
            mock_delete_object.return_value = True
            client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
            bucket_name = "bucket2"
            key = "object2.txt"

            result = await client.delete_object(bucket_name, key)

            self.assertTrue(result['DeleteMarker'])

    async def test_delete_object_empty_bucket(self):
        with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
            mock_delete_object.return_value = True
            client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
            bucket_name = "bucket1"
            key = "empty.txt"

            result = await client.delete_object(bucket_name, key)

            self.assertTrue(result['DeleteMarker'])

    async def test_delete_object_with_nonexistent_version(self):
        with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
            mock_delete_object.return_value = False
            client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
            bucket_name = "bucket1"
            key = "object1.txt"

            result = await client.delete_object(bucket_name, key)

            self.assertEqual(result, {})

    async def test_delete_object_empty_file(self):
        with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
            mock_delete_object.return_value = True
            client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
            bucket_name = "bucket1"
            key = "empty.txt"

            file_path = Path(self.temp_server_dir.name) / bucket_name / key
            os.makedirs(file_path.parent, exist_ok=True)
            with open(file_path, 'w') as f:
                f.write("")

            result = await client.delete_object(bucket_name, key)

            self.assertTrue(result['DeleteMarker'])
            self.assertFalse(file_path.exists())
    async def test_delete_objects_success(self):
        with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object, \
             patch('MetaData.MetadataManager.delete_version', new_callable=AsyncMock) as mock_delete_version:
            mock_delete_object.return_value = True
            mock_delete_version.return_value = True
            client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
            bucket_name = "bucket1"
            delete = {'Objects': [{'Key': 'object1.txt'}]}

            # Create the file to be deleted
            file_path = Path(self.temp_server_dir.name) / bucket_name / 'object1.txt'
            os.makedirs(file_path.parent, exist_ok=True)
            with open(file_path, 'w') as f:
                f.write("Test data")

            result = await client.delete_objects(bucket_name, delete)

            self.assertTrue(result['Deleted'])
            self.assertFalse(file_path.exists())
            self.assertFalse(result['Errors'])

    async def test_delete_objects_no_metadata_entry(self):
        with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
            mock_delete_object.return_value = False
            client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
            bucket_name = "bucket1"
            delete = {'Objects': [{'Key': 'non_existent.txt'}]}

            result = await client.delete_objects(bucket_name, delete)

            self.assertFalse(result['Deleted'])
            self.assertTrue(result['Errors'])

    async def test_delete_objects_partial_success(self):
        with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
            mock_delete_object.side_effect = [True, False]
            client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
            bucket_name = "bucket1"
            delete = {'Objects': [{'Key': 'object1.txt'}, {'Key': 'non_existent.txt'}]}

            # Create the file to be deleted
            file_path = Path(self.temp_server_dir.name) / bucket_name / 'object1.txt'
            os.makedirs(file_path.parent, exist_ok=True)
            with open(file_path, 'w') as f:
                f.write("Test data")

            result = await client.delete_objects(bucket_name, delete)

            self.assertEqual(len(result['Deleted']), 1)
            self.assertEqual(len(result['Errors']), 1)
            self.assertFalse(file_path.exists())

    async def test_delete_objects_with_version_id(self):
        with patch('MetaData.MetadataManager.delete_version', new_callable=AsyncMock) as mock_delete_version:
            mock_delete_version.return_value = True
            client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
            bucket_name = "bucket1"
            delete = {'Objects': [{'Key': 'object1.txt', 'VersionId': '1'}]}

            # Create the file to be deleted
            file_path = Path(self.temp_server_dir.name) / bucket_name / 'object1.txt'
            os.makedirs(file_path.parent, exist_ok=True)
            with open(file_path, 'w') as f:
                f.write("Test data")

            result = await client.delete_objects(bucket_name, delete)

            self.assertTrue(result['Deleted'])
            self.assertFalse(file_path.exists())
            self.assertFalse(result['Errors'])

    async def test_delete_objects_metadata_error(self):
        with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
            mock_delete_object.side_effect = Exception("Metadata error")
            client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
            bucket_name = "bucket1"
            delete = {'Objects': [{'Key': 'object1.txt'}]}

            result = await client.delete_objects(bucket_name, delete)

            self.assertFalse(result['Deleted'])
            self.assertEqual(len(result['Errors']), 1)

    async def test_delete_objects_is_sync_false(self):
        with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
            mock_delete_object.return_value = True
            client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
            bucket_name = "bucket1"
            delete = {'Objects': [{'Key': 'object1.txt'}]}

            # Create the file to be deleted
            file_path = Path(self.temp_server_dir.name) / bucket_name / 'object1.txt'
            os.makedirs(file_path.parent, exist_ok=True)
            with open(file_path, 'w') as f:
                f.write("Test data")

            result = await client.delete_objects(bucket_name, delete, is_sync=False)

            self.assertTrue(result['Deleted'])
            self.assertFalse(file_path.exists())
            self.assertFalse(result['Errors'])

    async def test_delete_objects_without_mock(self):
        client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
        bucket_name = "bucket1"
        delete = {'Objects': [{'Key': 'object1.txt'}]}

        # Create the file to be deleted
        file_path = Path(self.temp_server_dir.name) / bucket_name / 'object1.txt'
        os.makedirs(file_path.parent, exist_ok=True)
        with open(file_path, 'w') as f:
            f.write("Test data")

        result = await client.delete_objects(bucket_name, delete)

        self.assertTrue(result['Deleted'])
        self.assertFalse(file_path.exists())
        self.assertFalse(result['Errors'])

    async def test_delete_objects_no_buckets(self):
        with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
            mock_delete_object.return_value = True
            client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
            bucket_name = "bucket2"
            delete = {'Objects': [{'Key': 'object2.txt'}]}

            result = await client.delete_objects(bucket_name, delete)

            self.assertTrue(result['Deleted'])
            self.assertFalse(result['Errors'])

    async def test_delete_objects_empty_bucket(self):
        with patch('MetaData.MetadataManager.delete_object', new_callable=AsyncMock) as mock_delete_object:
            mock_delete_object.return_value = True
            client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
            bucket_name = "bucket1"
            delete = {'Objects': [{'Key': 'empty.txt'}]}

            result = await client.delete_objects(bucket_name, delete)

            self.assertTrue(result['Deleted'])
            self.assertFalse(result['Errors'])

    async def test_delete_objects_with_nonexistent_version(self):
        with patch('MetaData.MetadataManager.delete_version', new_callable=AsyncMock) as mock_delete_version:
            mock_delete_version.return_value = False
            client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
            bucket_name = "bucket1"
            delete = {'Objects': [{'Key': 'object1.txt', 'VersionId': 'nonexistent'}]}

            result = await client.delete_objects(bucket_name, delete)

            self.assertFalse(result['Deleted'])
            self.assertTrue(result['Errors'])

    async def test_get_object_unexpected_error(self):
        client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
        bucket_name = "bucket1"
        key = "object1.txt"

        with patch('PyStorage.MetadataManager.get_latest_version', side_effect=Exception("Unexpected error")):
            with self.assertRaises(RuntimeError):
                await client.get_object(bucket_name, key)
    async def test_get_object_success(self):
        client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
        bucket_name = "bucket1"
        key = "object1.txt"

        # Create the file to be retrieved
        file_path = Path(self.temp_server_dir.name) / bucket_name / key
        os.makedirs(file_path.parent, exist_ok=True)
        with open(file_path, 'w') as f:
            f.write("Test data")

        result = await client.get_object(bucket_name, key)

        self.assertEqual(result['Body'], b"Test data")
        self.assertEqual(result['ContentLength'], 9)
        self.assertEqual(result['ContentType'], 'application/octet-stream')
        self.assertEqual(result['ETag'], 'etag1')
        self.assertEqual(result['Metadata'], {})
        self.assertEqual(result['LastModified'], "2023-07-01T12:00:00Z")

    async def test_get_object_no_metadata(self):
        client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
        bucket_name = "bucket1"
        key = "object2.txt"

        # Create the file to be retrieved
        file_path = Path(self.temp_server_dir.name) / bucket_name / key
        os.makedirs(file_path.parent, exist_ok=True)
        with open(file_path, 'w') as f:
            f.write("Another test data")

        # Modify metadata to not include key
        self.metadata_content['server']['buckets'][bucket_name]['objects'].pop(key, None)
        with open(self.temp_metadata_file.name, 'w', encoding='utf-8') as f:
            json.dump(self.metadata_content, f)

        with self.assertRaises(RuntimeError):
            await client.get_object(bucket_name, key)

    async def test_get_object_invalid_metadata(self):
        client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
        bucket_name = "bucket1"
        key = "object1.txt"

        # Corrupt the metadata
        self.metadata_content['server']['buckets'][bucket_name]['objects'][key]['versions']['1'].pop('etag', None)
        with open(self.temp_metadata_file.name, 'w', encoding='utf-8') as f:
            json.dump(self.metadata_content, f)

        with self.assertRaises(RuntimeError):
            await client.get_object(bucket_name, key)

    async def test_get_object_no_bucket(self):
        client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
        bucket_name = "non_existent_bucket"
        key = "object1.txt"

        with self.assertRaises(RuntimeError):
            await client.get_object(bucket_name, key)

    async def test_get_object_invalid_json(self):
        client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
        bucket_name = "bucket1"
        key = "object1.txt"

        # Corrupt the metadata JSON
        with open(self.temp_metadata_file.name, 'w', encoding='utf-8') as f:
            f.write("invalid json")

        with self.assertRaises(RuntimeError):
            await client.get_object(bucket_name, key)


    async def test_get_object_acl_success(self):
        client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
        bucket_name = "bucket1"
        key = "object1.txt"

        result = await client.get_object_acl(bucket_name, key)

        self.metadata_content['server']['buckets']['bucket1']['objects']['object1.txt']['versions']['1']['acl'] = {
            'owner': 'owner1',
            'permissions': ['READ', 'WRITE']
        }


    async def test_get_object_acl_missing_acl(self):
        # Remove 'acl' from metadata
        self.metadata_content["server"]["buckets"]["bucket1"]["objects"]["object1.txt"]["versions"]["1"].pop('acl', None)
        with open(self.temp_metadata_file.name, 'w', encoding='utf-8') as f:
            json.dump(self.metadata_content, f)

        client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
        bucket_name = "bucket1"
        key = "object1.txt"

        result = await client.get_object_acl(bucket_name, key)

        self.assertEqual(result, {
            'Owner': {'DisplayName': 'unknown', 'ID': 'unknown'},
            'Grants': []
        })


    async def test_get_object_acl_no_bucket(self):
        client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
        bucket_name = "non_existent_bucket"
        key = "object1.txt"

        with self.assertRaises(RuntimeError):
            await client.get_object_acl(bucket_name, key)

    async def test_get_object_acl_no_key(self):
        client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
        bucket_name = "bucket1"
        key = "non_existent_key.txt"

        with self.assertRaises(RuntimeError):
            await client.get_object_acl(bucket_name, key)

    async def test_get_object_acl_partial_metadata(self):
        # Remove some metadata fields
        self.metadata_content["server"]["buckets"]["bucket1"]["objects"]["object1.txt"]["versions"]["1"].pop('acl', None)
        with open(self.temp_metadata_file.name, 'w', encoding='utf-8') as f:
            json.dump(self.metadata_content, f)

        client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
        bucket_name = "bucket1"
        key = "object1.txt"

        result = await client.get_object_acl(bucket_name, key)

        self.assertEqual(result, {
            'Owner': {'DisplayName': 'unknown', 'ID': 'unknown'},
            'Grants': []
        })

    async def test_get_object_acl_metadata_error(self):
        with patch('PyStorage.MetadataManager.get_latest_version', new_callable=AsyncMock) as mock_get_latest_version:
            mock_get_latest_version.side_effect = Exception("Metadata error")
            client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
            bucket_name = "bucket1"
            key = "object1.txt"
            with self.assertRaises(RuntimeError):
                await client.get_object_acl(bucket_name, key)

    async def test_copy_object_key_error(self):
        with patch('MetaData.MetadataManager.copy_metadata', new_callable=AsyncMock) as mock_copy_metadata:
            mock_copy_metadata.side_effect = KeyError("Some key error")
            client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
            bucket_name = "bucket2"
            key = "copied_object.txt"
            copy_source = {'Bucket': 'bucket1', 'Key': 'object1.txt'}

            # Create the source file
            source_file_path = Path(self.temp_server_dir.name) / copy_source['Bucket'] / copy_source['Key']
            os.makedirs(source_file_path.parent, exist_ok=True)
            with open(source_file_path, 'w') as f:
                f.write("Test data")

            with self.assertRaises(RuntimeError) as cm:
                await client.copy_object(bucket_name, copy_source, key)

            self.assertEqual(str(cm.exception), "Metadata key error: 'Some key error'")

    async def test_copy_object_without_mock(self):
        client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
        bucket_name = "bucket2"
        key = "copied_object.txt"
        copy_source = {'Bucket': 'bucket1', 'Key': 'object1.txt'}

        # Create the source file
        source_file_path = Path(self.temp_server_dir.name) / copy_source['Bucket'] / copy_source['Key']
        os.makedirs(source_file_path.parent, exist_ok=True)
        with open(source_file_path, 'w') as f:
            f.write("Test data")

        result = await client.copy_object(bucket_name, copy_source, key)

        self.assertTrue(result['CopyObjectResult']['ETag'])
        self.assertTrue(result['CopyObjectResult']['LastModified'])

    async def test_copy_object_non_existent_source_key(self):
        client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
        bucket_name = "bucket2"
        key = "copied_object.txt"
        copy_source = {'Bucket': 'bucket1', 'Key': 'non_existent_key.txt'}

        with self.assertRaises(RuntimeError):
            await client.copy_object(bucket_name, copy_source, key)

    async def test_copy_object_invalid_destination_bucket(self):
        with patch('MetaData.MetadataManager.copy_metadata', new_callable=AsyncMock) as mock_copy_metadata:
            client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
            bucket_name = "invalid_bucket"
            key = "copied_object.txt"
            copy_source = {'Bucket': 'bucket1', 'Key': 'object1.txt'}

            with self.assertRaises(RuntimeError):
                await client.copy_object(bucket_name, copy_source, key)

    async def test_copy_object_metadata_corruption(self):
        with patch('MetaData.MetadataManager.copy_metadata', new_callable=AsyncMock) as mock_copy_metadata:
            client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
            bucket_name = "bucket2"
            key = "corrupted_copied_object.txt"
            copy_source = {'Bucket': 'bucket1', 'Key': 'object1.txt'}

            # Corrupt metadata
            self.metadata_content['server']['buckets']['bucket1']['objects']['object1.txt']['versions']['1'].pop('etag', None)
            with open(self.temp_metadata_file.name, 'w', encoding='utf-8') as f:
                json.dump(self.metadata_content, f)

            with self.assertRaises(RuntimeError):
                await client.copy_object(bucket_name, copy_source, key)

    async def test_copy_object_no_source_bucket(self):
        client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
        bucket_name = "bucket2"
        key = "copied_object.txt"
        copy_source = {'Bucket': 'no_source_bucket', 'Key': 'object1.txt'}

        with self.assertRaises(RuntimeError):
            await client.copy_object(bucket_name, copy_source, key)

    async def test_copy_object_no_source_key(self):
        client = S3ClientSimulator(self.temp_metadata_file.name, self.temp_server_dir.name)
        bucket_name = "bucket2"
        key = "copied_object.txt"
        copy_source = {'Bucket': 'bucket1', 'Key': 'no_source_key.txt'}

        with self.assertRaises(RuntimeError):
            await client.copy_object(bucket_name, copy_source, key)
