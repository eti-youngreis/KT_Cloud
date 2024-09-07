import os
import pytest
import asyncio
import json
from PyStorage import S3ClientSimulator

functions = S3ClientSimulator()

@pytest.fixture
def create_test_metadata(tmp_path):
    metadata_file_path = tmp_path / "metadata.json"
    data = {
        "server": {
            "buckets": {
                "bucket1": {
                    "objects": {
                        "file.txt": {
                            "versions": {
                                "123": {
                                    "etag": "etag1",
                                    "size": 1024,
                                    "lastModified": "2023-07-01T12:00:00Z",
                                    "isLatest": False,
                                    "acl": {
                                        "owner": "user1",
                                        "permissions": ["READ", "WRITE"]
                                    },
                                    "LegalHold": {
                                        "Status": "OFF"
                                    },
                                    "Retention": {
                                        "Mode": "GOVERNANCE",
                                        "RetainUntilDate": "2024-12-31T23:59:59Z"
                                    }
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
    
    with open(metadata_file_path, 'r', encoding='utf-8') as f:
        metadata = json.load(f)
    
    functions.metadata_manager.metadata = metadata
    functions.metadata_manager.metadata_file = str(metadata_file_path)

    yield metadata_file_path

    if os.path.exists(metadata_file_path):
        os.remove(metadata_file_path)

@pytest.mark.asyncio
async def test_put_object_legal_hold(create_test_metadata):
    bucket = "bucket1"
    key = "file.txt"
    legal_hold_status = "ON"
    version_id = "123"
    result = await functions.put_object_legal_hold(bucket, key, legal_hold_status, version_id)
    assert result == {"LegalHold": {"Status": legal_hold_status}}

@pytest.mark.asyncio
async def test_get_object_legal_hold(create_test_metadata):
    bucket = "bucket1"
    key = "file.txt"
    version_id = "123"
    result = await functions.get_object_legal_hold(bucket, key, version_id)
    assert result == {"LegalHold": {"Status": "OFF"}}

@pytest.mark.asyncio
async def test_put_object_retention(create_test_metadata):
    bucket = "bucket1"
    key = "file.txt"
    retention_mode = "GOVERNANCE"
    retain_until_date = "2024-12-31T23:59:59Z"
    version_id = "123"
    result = await functions.put_object_retention(bucket, key, retention_mode, retain_until_date, version_id)
    assert result == {"Retention": {"Mode": retention_mode, "RetainUntilDate": retain_until_date}}

@pytest.mark.asyncio
async def test_get_object_retention(create_test_metadata):
    bucket = "bucket1"
    key = "file.txt"
    version_id = "123"
    result = await functions.get_object_retention(bucket, key, version_id)
    assert result == {"Retention": {"Mode": "GOVERNANCE", "RetainUntilDate": "2024-12-31T23:59:59Z"}}

@pytest.mark.asyncio
async def test_put_object_legal_hold_invalid_status(create_test_metadata):
    bucket = "bucket1"
    key = "file.txt"
    legal_hold_status = "INVALID_STATUS"
    version_id = "123"
    result = await functions.put_object_legal_hold(bucket, key, legal_hold_status, version_id)
    assert result == {'Error': 'Invalid value: Legal hold status must be either \'ON\' or \'OFF\''}

@pytest.mark.asyncio
async def test_put_object_legal_hold_empty_bucket(create_test_metadata):
    bucket = ""
    key = "file.txt"
    legal_hold_status = "ON"
    version_id = "123"
    result = await functions.put_object_legal_hold(bucket, key, legal_hold_status, version_id)
    assert result == {'Error': 'Invalid value: Bucket name must be a non-empty string'}

@pytest.mark.asyncio
async def test_put_object_legal_hold_empty_key(create_test_metadata):
    bucket = "bucket1"
    key = ""
    legal_hold_status = "ON"
    version_id = "123"
    result = await functions.put_object_legal_hold(bucket, key, legal_hold_status, version_id)
    assert result == {'Error': 'Invalid value: Object key must be a non-empty string'}

@pytest.mark.asyncio
async def test_get_object_legal_hold_empty_bucket(create_test_metadata):
    bucket = ""
    key = "file.txt"
    version_id = "123"
    result = await functions.get_object_legal_hold(bucket, key, version_id)
    assert result == {'Error': 'Invalid value: Bucket name must be a non-empty string'}

@pytest.mark.asyncio
async def test_get_object_legal_hold_empty_key(create_test_metadata):
    bucket = "bucket1"
    key = ""
    version_id = "123"
    result = await functions.get_object_legal_hold(bucket, key, version_id)
    assert result == {'Error': 'Invalid value: Object key must be a non-empty string'}

@pytest.mark.asyncio
async def test_get_object_retention_empty_bucket(create_test_metadata):
    bucket = ""
    key = "file.txt"
    version_id = "123"
    result = await functions.get_object_retention(bucket, key, version_id)
    assert result == {'Error': 'Invalid value: Bucket name must be a non-empty string'}

@pytest.mark.asyncio
async def test_get_object_retention_empty_key(create_test_metadata):
    bucket = "bucket1"
    key = ""
    version_id = "123"
    result = await functions.get_object_retention(bucket, key, version_id)
    assert result == {'Error': 'Invalid value: Object key must be a non-empty string'}

@pytest.mark.asyncio
async def test_put_object_lock_configuration(create_test_metadata):
    bucket = "bucket1"
    object_lock_enabled = "Enabled"
    mode = "COMPLIANCE"
    days = 365
    years = 0
    result = await functions.put_object_lock_configuration(bucket, object_lock_enabled, mode, days, years)
    assert result == {"ObjectLock": {"ObjectLockEnabled": "Enabled", "Rule": {"DefaultRetention": {"Mode": "COMPLIANCE", "Days": 365}}}}

@pytest.mark.asyncio
async def test_put_object_lock_configuration_invalid_mode(create_test_metadata):
    bucket = "bucket1"
    object_lock_enabled = "Enabled"
    mode = "INVALID_MODE"
    days = 365
    years = 0
    result = await functions.put_object_lock_configuration(bucket, object_lock_enabled, mode, days, years)
    assert result == {'Error': 'Invalid value: Retention mode must be either \'GOVERNANCE\' or \'COMPLIANCE\''}

@pytest.mark.asyncio
async def test_put_object_lock_configuration_invalid_days(create_test_metadata):
    bucket = "bucket1"
    object_lock_enabled = "Enabled"
    mode = "GOVERNANCE"
    days = -1
    years = 0
    result = await functions.put_object_lock_configuration(bucket, object_lock_enabled, mode, days, years)
    assert result == {'Error': 'Invalid value: Days must be a positive integer'}

@pytest.mark.asyncio
async def test_put_object_lock_configuration_invalid_years(create_test_metadata):
    bucket = "bucket1"
    object_lock_enabled = "Enabled"
    mode = "GOVERNANCE"
    days = 30
    years = -1
    result = await functions.put_object_lock_configuration(bucket, object_lock_enabled, mode, days, years)
    assert result == {'Error': 'Invalid value: Years must be a positive integer'}

@pytest.mark.asyncio
async def test_put_object_lock_configuration_invalid_bucket(create_test_metadata):
    bucket = ""
    object_lock_enabled = "Enabled"
    mode = "GOVERNANCE"
    days = 30
    years = 1
    result = await functions.put_object_lock_configuration(bucket, object_lock_enabled, mode, days, years)
    assert result == {'Error': 'Invalid value: Bucket name must be a non-empty string'}

@pytest.mark.asyncio
async def test_put_object_lock_configuration_invalid_object_lock_enabled(create_test_metadata):
    bucket = "bucket1"
    object_lock_enabled = "INVALID_VALUE"
    mode = "GOVERNANCE"
    days = 30
    years = 1
    result = await functions.put_object_lock_configuration(bucket, object_lock_enabled, mode, days, years)
    assert result == {'Error': 'Invalid value: Object lock enabled must be either \'Enabled\' or \'Disabled\''}

@pytest.mark.asyncio
async def test_put_object_retention_invalid_mode(create_test_metadata):
    bucket = "bucket1"
    key = "file.txt"
    retention_mode = "INVALID_MODE"
    retain_until_date = "2024-12-31T23:59:59Z"
    version_id = "123"
    result = await functions.put_object_retention(bucket, key, retention_mode, retain_until_date, version_id)
    assert result == {'Error': 'Invalid value: Retention mode must be either \'GOVERNANCE\' or \'COMPLIANCE\''}

@pytest.mark.asyncio
async def test_put_object_retention_invalid_date(create_test_metadata):
    bucket = "bucket1"
    key = "file.txt"
    retention_mode = "GOVERNANCE"
    retain_until_date = "INVALID_DATE"
    version_id = "123"
    result = await functions.put_object_retention(bucket, key, retention_mode, retain_until_date, version_id)
    assert result == {'Error': 'Invalid value: Retain until date must be a valid date in the format YYYY-MM-DDTHH:MM:SSZ'}

@pytest.mark.asyncio
async def test_put_object_retention_empty_bucket(create_test_metadata):
    bucket = ""
    key = "file.txt"
    retention_mode = "GOVERNANCE"
    retain_until_date = "2024-12-31T23:59:59Z"
    version_id = "123"
    result = await functions.put_object_retention(bucket, key, retention_mode, retain_until_date, version_id)
    assert result == {'Error': 'Invalid value: Bucket name must be a non-empty string'}

@pytest.mark.asyncio
async def test_put_object_retention_empty_key(create_test_metadata):
    bucket = "bucket1"
    key = ""
    retention_mode = "GOVERNANCE"
    retain_until_date = "2024-12-31T23:59:59Z"
    version_id = "123"
    result = await functions.put_object_retention(bucket, key, retention_mode, retain_until_date, version_id)
    assert result == {'Error': 'Invalid value: Object key must be a non-empty string'}
