import pytest
import os
import json
import sys
from unittest.mock import MagicMock, patch, mock_open

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Service.Classes.MultiPartUploadService   import MultipartUploadService
# Helper function to create a temporary database and storage path

@pytest.fixture
def setup_service():
    db_file = 'test.db'
    storage_path = 'test_storage'
    # Create a temporary database and storage directory
    os.makedirs(storage_path, exist_ok=True)
    service = MultipartUploadService(db_file, storage_path)
    yield service, db_file, storage_path
    # Clean up
    if os.path.exists(storage_path):
        for file in os.listdir(storage_path):
            os.remove(os.path.join(storage_path, file))
        os.rmdir(storage_path)


def test_initiate_multipart_upload(setup_service):
    service, _, _ = setup_service
    with patch.object(service.multipart_manager, 'create_multipart_upload', return_value='upload_id_123') as mock_create:
        upload_id = service.initiate_multipart_upload('bucket', 'object_key')
        assert upload_id == 'upload_id_123'
        mock_create.assert_called_once()


def test_split_file_into_parts(setup_service):
    service, _, _ = setup_service
    mock_file_content = 'a' * 10 * 1024 * 1024  # 10 MB
    with patch('builtins.open', mock_open(read_data=mock_file_content)) as mock_file:
        parts = service.split_file_into_parts('fake_path', part_size=5 * 1024 * 1024)
        assert len(parts) == 2  # Should split into 2 parts
        assert parts[0][0] == 1
        assert parts[1][0] == 2


def test_upload_part(setup_service):
    service, _, storage_path = setup_service
    upload_id = 'upload_id_123'
    part_number = 1
    part_data = 'test_part_data'
    file_path = os.path.join(storage_path, f'{upload_id}_part_{part_number}.txt')

    service.upload_part(upload_id, part_number, part_data)

    assert os.path.exists(file_path)
    with open(file_path, 'r') as f:
        data = f.read()

    assert data == part_data
    os.remove(file_path)



def test_upload_file_parts(setup_service):
    service, _, _ = setup_service
    with patch.object(service, 'split_file_into_parts', return_value=[(1, 'part1_data'), (2, 'part2_data')]), \
         patch.object(service, 'upload_part') as mock_upload_part:
        service.upload_file_parts('upload_id_123', 'fake_path', part_size=1024)
        assert mock_upload_part.call_count == 2


def test_list_parts(setup_service):
    service, db_file, _ = setup_service
    # Mock data
    mock_data = [(1, 'object_key', 'bucket', json.dumps([{'PartNumber': 1, 'FilePath': 'path1'}, {'PartNumber': 2, 'FilePath': 'path2'}]))]
    with patch.object(service.multipart_manager.object_manager, 'get_from_memory', return_value=mock_data) as mock_get:
        parts = service.list_parts('upload_id_123')
        assert len(parts) == 2
        assert parts[0]['PartNumber'] == 1
        assert parts[1]['PartNumber'] == 2
        mock_get.assert_called_once()


def test_complete_upload(setup_service):
    service, _, _ = setup_service
    mock_parts = [
        {'PartNumber': 1, 'FilePath': 'path1'},
        {'PartNumber': 2, 'FilePath': 'path2'}
    ]
    with patch.object(service.multipart_manager.object_manager, 'get_from_memory', return_value=[('upload_id_123', 'object_key', 'bucket', json.dumps(mock_parts))]) as mock_get, \
         patch('builtins.open', mock_open()) as mock_open_file, \
         patch('os.remove') as mock_remove, \
         patch.object(service.multipart_manager, 'complete_multipart_upload') as mock_complete_upload:
        complete_file_path = service.complete_upload('upload_id_123', 'bucket', 'object_key')
        assert complete_file_path == os.path.join('test_storage', 'complete_object_key')
        mock_get.assert_called_once()
        mock_open_file.assert_called()
        mock_remove.assert_called()
        mock_complete_upload.assert_called_once()

def test_convert_to_object(setup_service):
    service, _, _ = setup_service
    mock_data = [(1, 'object_key', 'bucket', json.dumps([{'PartNumber': 1, 'FilePath': 'path1'}]))]
    result = service.convert_to_object(mock_data)
    assert result.bucket_name == 'bucket'
    assert result.object_key == 'object_key'
    assert result.upload_id == 1
    assert len(result.parts) == 1