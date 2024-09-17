import pytest
import os
import json
import sqlite3
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Service.Classes.MultiPartUploadService import MultipartUploadService
from Models.MultipartUploadModel import MultipartUploadModel

@pytest.fixture(scope='module')
def setup_database():
    db_file = 'test_db.db'
    storage_path = 'test_storage'

    if not os.path.exists(storage_path):
        os.makedirs(storage_path)

    # Create service and setup database
    service = MultipartUploadService(db_file, storage_path)
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS mng_MultipartUploads (
        object_id TEXT PRIMARY KEY,
        object_name TEXT NOT NULL,
        bucket_name TEXT NOT NULL,
        parts TEXT NOT NULL
    )
    ''')
    conn.commit()
    conn.close()

    yield service, db_file, storage_path

    # Cleanup
    if os.path.exists(db_file):
        os.remove(db_file)
    if os.path.exists(storage_path):
        for root, dirs, files in os.walk(storage_path, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(storage_path)

def test_initiate_multipart_upload(setup_database):
    service, db_file, _ = setup_database
    bucket_name = 'test_bucket'
    object_key = 'test_file.txt'

    upload_id = service.initiate_multipart_upload(bucket_name, object_key)
    assert upload_id is not None

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM mng_MultipartUploads WHERE object_id=?', (upload_id,))
    row = cursor.fetchone()
    conn.close()

    assert row is not None
    assert row[2] == bucket_name
    assert row[1] == object_key

def test_split_file_into_parts(setup_database):
    service, _, _ = setup_database
    file_path = 'test_file.txt'
    part_size = 5
    with open(file_path, 'w') as f:
        f.write('test_data')

    expected_parts = [(1, 'test_data')]
    parts = service.split_file_into_parts(file_path, part_size)

    assert parts == expected_parts
    os.remove(file_path)

def test_upload_part(setup_database):
    service, _, storage_path = setup_database
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

def test_upload_file_parts(setup_database):
    service, _, storage_path = setup_database
    upload_id = 'upload_id_123'
    file_path = 'test_file.txt'
    part_size = 5
    with open(file_path, 'w') as f:
        f.write('test_data')

    service.upload_file_parts(upload_id, file_path, part_size)

    parts = service.split_file_into_parts(file_path, part_size)
    for part_number, _ in parts:
        part_file_path = os.path.join(storage_path, f'{upload_id}_part_{part_number}.txt')
        assert os.path.exists(part_file_path)
        os.remove(part_file_path)

    os.remove(file_path)