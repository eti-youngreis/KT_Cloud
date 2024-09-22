
import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from Storage.NEW_KT_Storage.Models.LockModel import LockModel
from Storage.NEW_KT_Storage.Service.Classes.LockService import LockService


@pytest.fixture
def lock_service():
    """Fixture to provide a LockService instance."""
    service = LockService()
    service.lock_manager = MagicMock()  # Mocking LockManager
    service.storageManager = MagicMock()  # Mocking StorageManager
    return service

def test_create_lock_success(lock_service:LockService):
    # lock_service.lock_manager.getAllLocks.return_value = []
    lock = lock_service.create_lock("bucket1", "object1", "write", 1, "d")
    assert lock.lock_id == "bucket1.object1"
    assert lock.lock_mode == "write"
    assert lock.retain_until > datetime.now()

def test_create_lock_existing_raises_error(lock_service:LockService):
    lock_service.lock_manager.getAllLocks.return_value = []
    lock_service.create_lock("bucket1", "object1", "write", 1, "d")
    with pytest.raises(ValueError):
        lock_service.create_lock("bucket1", "object1", "read", 5, "h")

def test_delete_lock_success(lock_service:LockService):
    time = lock_service.calculate_retention_duration(1, "d")
    lock = LockModel("bucket1", "object1", time, "write")
    lock_service.lock_map = {"bucket.object": 'lock',"bucket1.object1": lock}
    lock_service.delete_lock("bucket1.object1")
    print(lock_service.lock_map)
    assert "bucket1.object1" not in lock_service.lock_map
    
def test_delete_lock_nonexistent_raises_error(lock_service:LockService):
    lock_id = "bucket1.object1"
    with pytest.raises(ValueError, match=f"The lock with ID '{lock_id}' does not exists."):
        lock_service.delete_lock(lock_id)

def test_get_lock_success(lock_service:LockService):
    lock = LockModel("bucket1", "object1", datetime.now() + timedelta(days=1), "write")
    lock_service.lock_map = {"bucket1.object1": lock}
    result = lock_service.get_lock("bucket1.object1")
    assert result == lock

def test_get_lock_nonexistent_raises_error(lock_service:LockService):
    with pytest.raises(ValueError):
        lock_service.get_lock("nonexistent_lock")

def test_calculate_retention_duration_days(lock_service:LockService):
    retain_until = lock_service.calculate_retention_duration(3, "d")
    assert retain_until > datetime.now()
    assert (retain_until - datetime.now()).days == 3
    
def test_calculate_retention_duration_days(lock_service:LockService):
    retain_until = lock_service.calculate_retention_duration(3, "d")
    assert retain_until > datetime.now()
    assert (retain_until - datetime.now()).days == 3

def test_calculate_retention_duration_hours(lock_service:LockService):
    retain_until = lock_service.calculate_retention_duration(5, "h")
    assert retain_until > datetime.now()
    assert (retain_until - datetime.now()).seconds // 3600 == 5

def test_calculate_retention_duration_invalid_unit(lock_service:LockService):
    with pytest.raises(ValueError, match="Unsupported time unit"):
        lock_service.calculate_retention_duration(5, "z")

def test_is_lock_expired_true(lock_service:LockService):
    expired_lock = LockModel("bucket1", "object1", datetime.now() - timedelta(days=1), "write")
    assert lock_service.is_lock_expired(expired_lock)

def test_is_lock_expired_false(lock_service:LockService):
    active_lock = LockModel("bucket1", "object1", datetime.now() + timedelta(days=1), "all")
    assert not lock_service.is_lock_expired(active_lock)

def test_remove_expired_locks(lock_service:LockService):
    expired_lock = LockModel("bucket1", "object1", datetime.now() - timedelta(days=1), "write")
    active_lock = LockModel("bucket2", "object2", datetime.now() + timedelta(days=1), "read")
    lock_service.lock_map = {
        "bucket1.object1": expired_lock,
        "bucket2.object2": active_lock
    }
    lock_service.locks_IDs_list = [("bucket1.object1", expired_lock.retain_until),
                                ("bucket2.object2", active_lock.retain_until)]
    
    lock_service.remove_expired_locks()
    
    assert "bucket1.object1" not in lock_service.lock_map
    assert "bucket2.object2" in lock_service.lock_map

def test_is_object_updatable_when_locked(lock_service:LockService):
    lock = LockModel("bucket1", "object1", datetime.now() + timedelta(days=1), "write")
    lock_service.lock_map = {"bucket1.object1": lock}
    assert not lock_service.is_object_updatable("bucket1", "object1")

def test_is_object_updatable_when_unlocked(lock_service:LockService):
    assert lock_service.is_object_updatable("bucket1", "object1")

def test_is_object_deletable_when_locked(lock_service:LockService):
    lock = LockModel("bucket1", "object1", datetime.now() + timedelta(days=1), "delete")
    lock_service.lock_map = {"bucket1.object1": lock}
    assert not lock_service.is_object_deletable("bucket1", "object1")

def test_is_object_deletable_when_unlocked(lock_service:LockService):
    assert lock_service.is_object_deletable("bucket1", "object1")
