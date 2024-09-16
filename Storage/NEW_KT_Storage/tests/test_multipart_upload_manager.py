import os
import pytest
from unittest.mock import Mock, patch
from DataAccess.MultipartUploadManager import MultipartUploadManager

@pytest.fixture
def multipart_upload_manager():
    db_manager_mock = Mock()
    storage_path = '/tmp/test_storage'
    return MultipartUploadManager(db_manager_mock, storage_path)

def test_complete_multipart_upload_success(multipart_upload_manager, tmp_path):
    bucket_name = 'test-bucket'
    object_key = 'test-object'
    upload_id = 'test-upload-id'

    # Create test part files
    part1 = tmp_path / 'part1'
    part2 = tmp_path / 'part2'
    part1.write_bytes(b'Part 1 content')
    part2.write_bytes(b'Part 2 content')

    multipart_upload_manager.db_manager.get_object_from_management_table.return_value = {
        'parts': [
            {'PartNumber': 1, 'FilePath': str(part1)},
            {'PartNumber': 2, 'FilePath': str(part2)}
        ]
    }

    multipart_upload_manager.storage_path = str(tmp_path)

    multipart_upload_manager.complete_multipart_upload(bucket_name, object_key, upload_id)

    complete_file_path = os.path.join(str(tmp_path), f'{bucket_name}/{object_key}_complete')
    assert os.path.exists(complete_file_path)
    with open(complete_file_path, 'rb') as f:
        assert f.read() == b'Part 1 contentPart 2 content'

    assert not os.path.exists(str(part1))
    assert not os.path.exists(str(part2))

def test_complete_multipart_upload_empty_parts(multipart_upload_manager, tmp_path):
    bucket_name = 'test-bucket'
    object_key = 'test-object'
    upload_id = 'test-upload-id'

    multipart_upload_manager.db_manager.get_object_from_management_table.return_value = {
        'parts': []
    }

    multipart_upload_manager.storage_path = str(tmp_path)

    multipart_upload_manager.complete_multipart_upload(bucket_name, object_key, upload_id)

    complete_file_path = os.path.join(str(tmp_path), f'{bucket_name}/{object_key}_complete')
    assert os.path.exists(complete_file_path)
    assert os.path.getsize(complete_file_path) == 0

def test_complete_multipart_upload_file_not_found(multipart_upload_manager, tmp_path):
    bucket_name = 'test-bucket'
    object_key = 'test-object'
    upload_id = 'test-upload-id'

    multipart_upload_manager.db_manager.get_object_from_management_table.return_value = {
        'parts': [
            {'PartNumber': 1, 'FilePath': '/non/existent/path'}
        ]
    }

    multipart_upload_manager.storage_path = str(tmp_path)

    with pytest.raises(FileNotFoundError):
        multipart_upload_manager.complete_multipart_upload(bucket_name, object_key, upload_id)

def test_complete_multipart_upload_unsorted_parts(multipart_upload_manager, tmp_path):
    bucket_name = 'test-bucket'
    object_key = 'test-object'
    upload_id = 'test-upload-id'

    part1 = tmp_path / 'part1'
    part2 = tmp_path / 'part2'
    part1.write_bytes(b'Part 1 content')
    part2.write_bytes(b'Part 2 content')

    multipart_upload_manager.db_manager.get_object_from_management_table.return_value = {
        'parts': [
            {'PartNumber': 2, 'FilePath': str(part2)},
            {'PartNumber': 1, 'FilePath': str(part1)}
        ]
    }

    multipart_upload_manager.storage_path = str(tmp_path)

    multipart_upload_manager.complete_multipart_upload(bucket_name, object_key, upload_id)

    complete_file_path = os.path.join(str(tmp_path), f'{bucket_name}/{object_key}_complete')
    assert os.path.exists(complete_file_path)
    with open(complete_file_path, 'rb') as f:
        assert f.read() == b'Part 1 contentPart 2 content'

def test_complete_multipart_upload_permission_error(multipart_upload_manager, tmp_path):
    bucket_name = 'test-bucket'
    object_key = 'test-object'
    upload_id = 'test-upload-id'

    part1 = tmp_path / 'part1'
    part1.write_bytes(b'Part 1 content')
    os.chmod(str(part1), 0o000)

    multipart_upload_manager.db_manager.get_object_from_management_table.return_value = {
        'parts': [
            {'PartNumber': 1, 'FilePath': str(part1)}
        ]
    }

    multipart_upload_manager.storage_path = str(tmp_path)

    with pytest.raises(PermissionError):
        multipart_upload_manager.complete_multipart_upload(bucket_name, object_key, upload_id)

    os.chmod(str(part1), 0o644)  # Restore permissions for cleanup
class TestMultipartUploadManager:
    @pytest.fixture
    def multipart_upload_manager(self, tmp_path):
        storage_path = tmp_path / "storage"
        storage_path.mkdir()
        db_manager = MagicMock()
        return MultipartUploadManager(str(storage_path), db_manager)

    @pytest.mark.asyncio
    async def test_complete_multipart_upload_success(self, multipart_upload_manager, tmp_path):
        bucket_name = "test-bucket"
        object_key = "test-object"
        upload_id = "test-upload-id"

        # Create test part files
        part1 = tmp_path / "part1"
        part2 = tmp_path / "part2"
        part1.write_bytes(b"Part 1 content")
        part2.write_bytes(b"Part 2 content")

        multipart_upload_manager.db_manager.get_object_from_management_table.return_value = MagicMock(
            parts=[
                {"PartNumber": 1, "FilePath": str(part1)},
                {"PartNumber": 2, "FilePath": str(part2)},
            ]
        )

        await multipart_upload_manager.complete_multipart_upload(bucket_name, object_key, upload_id)

        complete_file_path = os.path.join(multipart_upload_manager.storage_path, f'{bucket_name}/{object_key}_complete')
        assert os.path.exists(complete_file_path)
        with open(complete_file_path, 'rb') as f:
            assert f.read() == b"Part 1 contentPart 2 content"

        assert not os.path.exists(str(part1))
        assert not os.path.exists(str(part2))

    @pytest.mark.asyncio
    async def test_complete_multipart_upload_empty_parts(self, multipart_upload_manager):
        bucket_name = "test-bucket"
        object_key = "test-object"
        upload_id = "test-upload-id"

        multipart_upload_manager.db_manager.get_object_from_management_table.return_value = MagicMock(parts=[])

        with pytest.raises(ValueError, match="No parts found for the multipart upload"):
            await multipart_upload_manager.complete_multipart_upload(bucket_name, object_key, upload_id)

    @pytest.mark.asyncio
    async def test_complete_multipart_upload_missing_part_file(self, multipart_upload_manager, tmp_path):
        bucket_name = "test-bucket"
        object_key = "test-object"
        upload_id = "test-upload-id"

        part1 = tmp_path / "part1"
        part1.write_bytes(b"Part 1 content")
        non_existent_part = tmp_path / "non_existent_part"

        multipart_upload_manager.db_manager.get_object_from_management_table.return_value = MagicMock(
            parts=[
                {"PartNumber": 1, "FilePath": str(part1)},
                {"PartNumber": 2, "FilePath": str(non_existent_part)},
            ]
        )

        with pytest.raises(FileNotFoundError):
            await multipart_upload_manager.complete_multipart_upload(bucket_name, object_key, upload_id)

    @pytest.mark.asyncio
    async def test_complete_multipart_upload_out_of_order_parts(self, multipart_upload_manager, tmp_path):
        bucket_name = "test-bucket"
        object_key = "test-object"
        upload_id = "test-upload-id"

        part1 = tmp_path / "part1"
        part2 = tmp_path / "part2"
        part1.write_bytes(b"Part 1 content")
        part2.write_bytes(b"Part 2 content")

        multipart_upload_manager.db_manager.get_object_from_management_table.return_value = MagicMock(
            parts=[
                {"PartNumber": 2, "FilePath": str(part2)},
                {"PartNumber": 1, "FilePath": str(part1)},
            ]
        )

        await multipart_upload_manager.complete_multipart_upload(bucket_name, object_key, upload_id)

        complete_file_path = os.path.join(multipart_upload_manager.storage_path, f'{bucket_name}/{object_key}_complete')
        assert os.path.exists(complete_file_path)
        with open(complete_file_path, 'rb') as f:
            assert f.read() == b"Part 1 contentPart 2 content"

    @pytest.mark.asyncio
    async def test_complete_multipart_upload_large_number_of_parts(self, multipart_upload_manager, tmp_path):
        bucket_name = "test-bucket"
        object_key = "test-object"
        upload_id = "test-upload-id"

        num_parts = 1000
        parts = []
        expected_content = b""

        for i in range(1, num_parts + 1):
            part_file = tmp_path / f"part{i}"
            part_content = f"Part {i} content".encode()
            part_file.write_bytes(part_content)
            parts.append({"PartNumber": i, "FilePath": str(part_file)})
            expected_content += part_content

        multipart_upload_manager.db_manager.get_object_from_management_table.return_value = MagicMock(parts=parts)

        await multipart_upload_manager.complete_multipart_upload(bucket_name, object_key, upload_id)

        complete_file_path = os.path.join(multipart_upload_manager.storage_path, f'{bucket_name}/{object_key}_complete')
        assert os.path.exists(complete_file_path)
        with open(complete_file_path, 'rb') as f:
            assert f.read() == expected_content

        for part in parts:
            assert not os.path.exists(part['FilePath'])
