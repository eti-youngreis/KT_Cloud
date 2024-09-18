import pytest
import tempfile
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

from Storage.NEW_KT_Storage.Service.Classes.BucketService import BucketService
@pytest.fixture
def bucket_service():
    """Create a BucketService object for the tests with a temporary place"""
    with tempfile.TemporaryDirectory() as tmpdir:
        bucket_service = BucketService(storage_path=tmpdir)
        yield bucket_service

def test_create_bucket_valid(bucket_service):
    """Test for creating a valid bucket."""
    length = len(bucket_service.buckets)
    bucket_service.create("valid-bucket", "owner123")
    assert len(bucket_service.buckets) == length+1
    assert bucket_service.get("valid-bucket")
    bucket_service.delete("valid-bucket")


def test_create_bucket_existing(bucket_service):
    """Test for creating an already existing bucket"""
    bucket_service.create("test-bucket", "owner123")
    with pytest.raises(ValueError, match="This bucket already exists."):
        bucket_service.create("test-bucket", "owner123")
    bucket_service.delete("test-bucket")

def test_delete_bucket_existing(bucket_service):
    """Test for creating an already existing bucket"""
    bucket_service.create("test-bucket-unique", "owner123")
    assert "test-bucket-unique" in [b.bucket_name for b in bucket_service.buckets]
    bucket_service.delete("test-bucket-unique")
    assert "test-bucket-unique" not in bucket_service.buckets

def test_get_bucket_existing(bucket_service):
    """Test for returning an existing deed"""
    bucket_service.create("test-bucket", "owner123")
    bucket = bucket_service.get("test-bucket")
    assert bucket.bucket_name == "test-bucket"
    bucket_service.delete("test-bucket")

def test_create_bucket_too_long_name(bucket_service):
    """Test for creating a bucket with a name that is too long."""
    long_bucket_name = "a" * 64
    with pytest.raises(ValueError, match="Bucket name must be between 3 and 63 characters."):
        bucket_service.create(long_bucket_name, "owner123")

def test_create_bucket_with_empty_name(bucket_service):
    """Test for creating a bucket with an empty name."""
    with pytest.raises(ValueError, match="Bucket name must be between 3 and 63 characters."):
        bucket_service.create("", "owner123")

def test_create_bucket_with_special_characters(bucket_service):
    """Test for creating a bucket with special characters in the name."""
    with pytest.raises(ValueError, match="Bucket name contains invalid characters."):
        bucket_service.create("bucket@name", "owner123")


def test_get_bucket_with_empty_name(bucket_service):
    """Test for getting a bucket with an empty name."""
    with pytest.raises(ValueError, match="This bucket does not exist."):
        bucket_service.get("")

def test_create_bucket_with_valid_owner_name(bucket_service):
    """Test for creating a bucket with a valid owner name."""
    length = len(bucket_service.buckets)
    bucket_service.create("valid-bucket", "owner123")
    assert len(bucket_service.buckets) == length+1
    assert bucket_service.get("valid-bucket").owner == "owner123"
    bucket_service.delete("valid-bucket")

def test_delete_bucket_twice(bucket_service):
    """Test for deleting a bucket twice."""
    length = len(bucket_service.buckets)
    bucket_service.create("test-bucket", "owner123")
    bucket_service.delete("test-bucket")
    assert length == len(bucket_service.buckets)
    with pytest.raises(ValueError, match="This bucket does not exist."):
        bucket_service.delete("test-bucket")

def test_create_bucket_with_owner_long(bucket_service):
    """Test for creating a bucket with owner of incorrect length"""
    with pytest.raises(ValueError, match="Owner is not in the valid length"):
        bucket_service.create("valid-buckets","g")

def test_delete_nonexistent_bucket(bucket_service):
    """Test for deleting a nonexistent bucket."""
    with pytest.raises(ValueError, match="This bucket does not exist."):
        bucket_service.delete("nonexistent-bucket")


def test_get_bucket_nonexistent(bucket_service):
    """Test check if buckt exist"""
    with pytest.raises(ValueError, match="This bucket does not exist."):
        bucket_service.get("nonexistent-bucket")
