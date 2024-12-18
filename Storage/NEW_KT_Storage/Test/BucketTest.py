from datetime import datetime
import pytest
import tempfile
import sys
import os
from Storage.NEW_KT_Storage.Exceptions.BucketExceptions import BucketAlreadyExistsError, InvalidRegionError, \
    InvalidBucketNameError, InvalidOwnerError, BucketNotFoundError
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
    bucket_service.create("valid-bucket", "owner123","us-east-1")
    assert len(bucket_service.buckets) == length+1
    assert bucket_service.get("valid-bucket")
    bucket_service.delete("valid-bucket")


def test_create_bucket_existing(bucket_service):
    """Test for creating an already existing bucket"""
    bucket_name = "test-bucket"
    bucket_service.create(bucket_name, "owner123")
    with pytest.raises(BucketAlreadyExistsError, match=f"The bucket '{bucket_name}' already exists."):
        bucket_service.create(bucket_name, "owner123")
    bucket_service.delete("test-bucket")


def test_create_bucket_with_not_valid_region(bucket_service):
    """Test if region not valid"""
    region = "us-east-123"
    with pytest.raises(InvalidRegionError, match=f"Invalid region: '{region}'."):
        bucket_service.create("test-bucket", "test", region)


def test_create_bucket_too_long_name(bucket_service):
    """Test for creating a bucket with a name that is too long."""
    long_bucket_name = "a" * 64
    with pytest.raises(InvalidBucketNameError, match="Bucket name must be between 3 and 63 characters."):
        bucket_service.create(long_bucket_name, "owner123")


def test_create_bucket_with_empty_name(bucket_service):
    """Test for creating a bucket with an empty name."""
    with pytest.raises(InvalidBucketNameError, match="Bucket name must be between 3 and 63 characters."):
        bucket_service.create("", "owner123")


def test_create_bucket_with_special_characters(bucket_service):
    """Test for creating a bucket with special characters in the name."""
    with pytest.raises(InvalidBucketNameError, match="Bucket name contains invalid characters."):
        bucket_service.create("bucket@name", "owner123")


def test_create_bucket_with_valid_owner_name(bucket_service):
    """Test for creating a bucket with a valid owner name."""
    length = len(bucket_service.buckets)
    bucket_service.create("valid-bucket", "owner123")
    assert len(bucket_service.buckets) == length+1
    bucket_service.delete("valid-bucket")


def test_create_bucket_with_owner_long(bucket_service):
    """Test for creating a bucket with owner of incorrect length"""
    with pytest.raises(InvalidOwnerError, match="Owner is not in the valid length"):
        bucket_service.create("valid-buckets", "g", "us-east-1")


def test_create_bucket_with_valid_region_optional(bucket_service):
    """Test for creating a bucket with an optional valid region."""
    length = len(bucket_service.buckets)
    bucket_service.create("valid-bucket-region", "owner123", "us-west-2")
    assert len(bucket_service.buckets) == length+1
    assert bucket_service.get("valid-bucket-region")
    bucket_service.delete("valid-bucket-region")


def test_create_bucket_without_owner(bucket_service):
    """Test for creating a bucket without specifying an owner."""
    owner_name=""
    with pytest.raises(InvalidOwnerError, match=f"Invalid owner name. Provided owner name: '{owner_name}'."):
        bucket_service.create("bucket-name", "")


def test_create_bucket_with_min_length_name(bucket_service):
    """Test for creating a bucket with the minimum valid length name."""
    bucket_service.create("abc", "owner123")
    assert "abc" in [b.bucket_name for b in bucket_service.buckets]
    bucket_service.delete("abc")


def test_create_bucket_with_max_length_name(bucket_service):
    """Test for creating a bucket with the maximum valid length name."""
    bucket_service.create("a" * 63, "owner123")
    assert "a" * 63 in [b.bucket_name for b in bucket_service.buckets]
    bucket_service.delete("a" * 63)


def test_create_bucket_with_leading_trailing_spaces(bucket_service):
    """Test for creating a bucket with leading/trailing spaces in the name."""
    with pytest.raises(InvalidBucketNameError, match="Bucket name contains invalid characters."):
        bucket_service.create("  bucket-name  ", "owner123")


def test_create_bucket_with_non_string_name(bucket_service):
    """Test for creating a bucket with a non-string name."""
    bucket_name = 123
    with pytest.raises(InvalidBucketNameError, match=f"Invalid bucket name. Provided bucket name: '{bucket_name}'."):
        bucket_service.create(bucket_name, "owner123")


def test_create_bucket_with_whitespace_in_name(bucket_service):
    """Test for creating a bucket with whitespace in the name."""
    with pytest.raises(InvalidBucketNameError, match="Bucket name contains invalid characters."):
        bucket_service.create("bucket name", "owner123")


def test_bucket_creation_time(bucket_service):
    """Test to ensure the creation time of the bucket is set correctly."""
    bucket_service.create("time-check-bucket", "owner123")
    bucket = bucket_service.get("time-check-bucket")
    assert bucket.create_at is not None
    bucket_service.delete("time-check-bucket")


def test_delete_bucket_existing(bucket_service):
    """Test for creating an already existing bucket"""
    bucket_service.create("test-bucket-unique", "owner123")
    assert "test-bucket-unique" in [b.bucket_name for b in bucket_service.buckets]
    bucket_service.delete("test-bucket-unique")
    assert "test-bucket-unique" not in bucket_service.buckets


def test_bucket_creation_creates_directories(bucket_service):
    """Test to ensure that directories are created for the bucket."""
    bucket_name = "dir-check-bucket"
    bucket_service.create(bucket_name, "owner123")
    bucket_dir = os.path.join(bucket_service.storage_manager.base_directory, 'buckets', bucket_name)
    locks_dir = os.path.join(bucket_dir, 'locks')
    assert os.path.isdir(bucket_dir), f"Expected directory {bucket_dir} to exist."
    assert os.path.isdir(locks_dir), f"Expected directory {locks_dir} to exist."
    bucket_service.delete(bucket_name)


def test_delete_bucket_twice(bucket_service):
    """Test for deleting a bucket twice."""
    length = len(bucket_service.buckets)
    bucket_name = "test-bucket"
    bucket_service.create(bucket_name, "owner123")
    bucket_service.delete(bucket_name)
    assert length == len(bucket_service.buckets)
    with pytest.raises(BucketNotFoundError, match=f'{bucket_name} not found.'):
        bucket_service.delete(bucket_name)


def test_get_with_valid_bucket(bucket_service):
    """Test for get valid bucket"""
    bucket_service.create("test-bucket", "owner123","us-east-1")
    bucket = bucket_service.get("test-bucket")
    assert bucket.owner == "owner123" and bucket.region == "us-east-1" and bucket.bucket_name == "test-bucket"
    bucket_service.delete("test-bucket")


def test_get_is_create_time_right(bucket_service):
    """Test get time created valid"""
    bucket_service.create("bucket-test", "test")
    current_time = datetime.now()
    bucket_time = bucket_service.get("bucket-test").create_at
    current_time_minute = current_time.replace(second=0, microsecond=0)
    bucket_time_minute = bucket_time.replace(second=0, microsecond=0)
    time_difference = abs((bucket_time_minute - current_time_minute).total_seconds())
    assert time_difference <= 1
    bucket_service.delete("bucket-test")


def test_get_bucket_with_non_string_name(bucket_service):
    """Test for creating a bucket with a non-string name."""
    bucket_name = 123
    with pytest.raises(InvalidBucketNameError, match=f"Invalid bucket name. Provided bucket name: '{bucket_name}'."):
        bucket_service.delete(bucket_name)


def test_delete_nonexistent_bucket(bucket_service):
    """Test for deleting a nonexistent bucket."""
    bucket_name = "nonexistent-bucket"
    with pytest.raises(BucketNotFoundError, match=f'{bucket_name} not found.'):
        bucket_service.delete(bucket_name)


def test_get_bucket_existing(bucket_service):
    """Test for returning an existing deed"""
    bucket_service.create("test-bucket", "owner123")
    bucket = bucket_service.get("test-bucket")
    assert bucket.bucket_name == "test-bucket"
    bucket_service.delete("test-bucket")


def test_get_bucket_with_empty_name(bucket_service):
    """Test for getting a bucket with an empty name."""
    bucket_name = ""
    with pytest.raises(BucketNotFoundError, match=f'{bucket_name} not found.'):
        bucket_service.get(bucket_name)


def test_get_bucket_nonexistent(bucket_service):
    """Test check if buckt exist"""
    bucket_name = "nonexistent-bucket"
    with pytest.raises(BucketNotFoundError, match=f'{bucket_name} not found.'):
        bucket_service.get(bucket_name)






