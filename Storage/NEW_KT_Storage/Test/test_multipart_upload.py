import os
import json
import sys
import sqlite3
import pytest
from unittest.mock import MagicMock, patch, mock_open
import uuid
import tempfile

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Service.Classes.MultiPartUploadService import MultipartUploadService

@pytest.fixture(scope="function")
def setup_service():
    db_file = ':memory:'
    storage_path = 'test_storage'
    # Create a temporary database and storage directory
    os.makedirs(storage_path, exist_ok=True)
    
    service = MultipartUploadService(db_file, storage_path)
    
    yield service, db_file, storage_path

    if os.path.exists(storage_path):
        for file in os.listdir(storage_path):
            os.remove(os.path.join(storage_path, file))
        os.rmdir(storage_path)

@pytest.fixture(scope="function")
def create_file():
    # Create a temporary file
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    
    try:
        # Write some content to the file
        temp_file.write(b'This is a temporary file with some content.')
        temp_file.flush()  # Ensure content is written to disk
        temp_file.close()  # Close the file before using it
        
        yield temp_file.name  # Provide the file path to the test
    finally:
        # Cleanup: Delete the file after the test is done
        os.remove(temp_file.name)
    

def test_initiate_multipart_upload(setup_service):
    service, _, _ = setup_service
    with patch.object(service.multipart_manager, 'create_multipart_upload', return_value='upload_id_123') as mock_create:
        upload_id = service.initiate_multipart_upload('bucket', 'object_key')
        assert upload_id == 'upload_id_123'
        mock_create.assert_called_once()


def test_split_file_into_parts(setup_service, create_file):
    service, _, _ = setup_service
    # Write 10MB content to the file
    mock_file_content = 'a' * 10 * 1024 * 1024  # 10 MB
    with open(create_file, 'w') as f:
        f.write(mock_file_content)

    parts = service.split_file_into_parts(create_file, part_size=5 * 1024 * 1024)
    
    assert len(parts) == 2  # Should split into 2 parts
    assert parts[0][0] == 1
    assert parts[1][0] == 2


def test_upload_file_parts(setup_service, create_file):
    service, _, _ = setup_service
    upload_id = str(uuid.uuid4())  # Generate a valid UUID
    with patch.object(service, 'split_file_into_parts', return_value=[(1, 'part1_data'), (2, 'part2_data')]), \
         patch.object(service, 'upload_part') as mock_upload_part:
        # Use the correct file path from the create_file fixture
        service.upload_file_parts(upload_id, create_file, part_size=1024)
        assert mock_upload_part.call_count == 2


def test_split_file_into_parts_empty_file(setup_service,create_file):
    service, _, _ = setup_service
    with patch('builtins.open', mock_open(read_data='')) as mock_file:
        parts = service.split_file_into_parts(str(create_file), part_size=1024)
        assert len(parts) == 0  # Should return empty list


def test_upload_part_invalid_upload_id(setup_service):
    service, _, _ = setup_service
    with pytest.raises(ValueError):
        service.upload_part('invalid_upload_id', 1, 'part_data')


def test_upload_file_parts_invalid_path(setup_service):
    upload_id = str(uuid.uuid4())  # Generate a valid UUID
    service, _, _ = setup_service
    with pytest.raises(FileNotFoundError):
        service.upload_file_parts(upload_id, 'invalid_path')


def test_convert_to_object_invalid_data(setup_service):
    service, _, _ = setup_service
    with pytest.raises(ValueError):
        service.convert_to_object([])


def test_list_parts_empty_upload_id(setup_service):
    service, _, _ = setup_service
    with pytest.raises(ValueError):
        service.list_parts('invalid_upload_id')


def test_split_file_into_parts_large_file(setup_service, create_file):
    service, _, _ = setup_service
    mock_file_content = 'a' * (10 * 1024 * 1024)  # 10 MB
    # כתיבת תוכן של 10MB לקובץ
    with open(create_file, 'w') as f:
        f.write(mock_file_content)

    # עכשיו ניתן לפצל את הקובץ לחלקים
    parts = service.split_file_into_parts(create_file, part_size=6 * 1024 * 1024)
    
    assert len(parts) == 2  # אמור לפצל ל-2 חלקים

def test_convert_to_object_with_missing_data(setup_service):
    service, _, _ = setup_service
    with pytest.raises(ValueError):
        service.convert_to_object([])



def test_split_file_into_parts_invalid_part_size(setup_service, create_file):
    service, _, _ = setup_service
    with pytest.raises(ValueError):
        service.split_file_into_parts(create_file, part_size=-1024)  # Invalid part size


def test_split_file_into_parts_nonexistent_file(setup_service):
    service, _, _ = setup_service
    with pytest.raises(FileNotFoundError):
        service.split_file_into_parts('nonexistent_file.txt', part_size=1024)  # Non-existent file


def test_upload_part_with_missing_part_data(setup_service):
    service, _, _ = setup_service
    with pytest.raises(ValueError):
        service.upload_part('upload_id_123', 1, None)  # Missing part data


def test_upload_file_parts_large_part_size(setup_service, create_file):
    service, _, _ = setup_service
    upload_id = str(uuid.uuid4())
    with pytest.raises(ValueError):
        service.upload_file_parts(upload_id, create_file, part_size=50 * 1024 * 1024)  # Part size too large


def test_list_parts_invalid_upload_id(setup_service):
    service, _, _ = setup_service
    with pytest.raises(ValueError):
        service.list_parts('invalid_upload_id')  # Invalid upload ID


def test_complete_upload_invalid_upload_id(setup_service):
    service, _, _ = setup_service
    with pytest.raises(ValueError):
        service.complete_upload('invalid_upload_id')  # Invalid upload ID


def test_complete_upload_missing_parts(setup_service):
    service, _, _ = setup_service
    with patch.object(service.multipart_manager.object_manager, 'get_from_memory', return_value=[('upload_id_123', 'object_key', 'bucket', '[]')]):
        with pytest.raises(ValueError):
            service.complete_upload('upload_id_123')  # No parts to complete upload


def test_convert_to_object_invalid_format(setup_service):
    service, _, _ = setup_service
    invalid_obj = [('upload_id', 'object_key', 'bucket', 'not_json')]
    with pytest.raises(ValueError):
        service.convert_to_object(invalid_obj)  # Invalid format for parts data


