from Storage.NEW_KT_Storage.Models.BucketModel import Bucket
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from Storage.NEW_KT_Storage.DataAccess.BucketManager import BucketManager
import Storage.NEW_KT_Storage.Validation.BucketValidations as BucketValidations
import Storage.NEW_KT_Storage.Exceptions.BucketExceptions as BucketExceptions


class BucketService:
    def __init__(self,storage_path="D:/s3_project/server"):
        self.storage_manager = StorageManager(storage_path)
        self.bucket_manager = BucketManager("D:/s3_project/tables/Buckets.db")
        self.buckets = self.load_buckets()

    def load_buckets(self):
        data_list = self.bucket_manager.object_manager.get_all_objects_from_memory("Bucket")
        return [Bucket(bucket_name=row[0], owner=row[1], region=row[2], create_at=row[3]) for row in data_list]

    def create(self, bucket_name: str, owner: str, region: str = None):
            """Create a new Bucket."""
            if BucketValidations.bucket_exists(self.buckets, bucket_name):
                raise BucketExceptions.BucketAlreadyExistsError(bucket_name)
            if not BucketValidations.valid_type_paramters(bucket_name):
                raise BucketExceptions.InvalidBucketNameError(bucket_name)
            if not BucketValidations.is_length_range(bucket_name):
                raise BucketExceptions.InvalidBucketNameError(bucket_name, "Bucket name must be between 3 and 63 characters.")
            if not BucketValidations.is_bucket_name_valid(bucket_name):
                raise BucketExceptions.InvalidBucketNameError(bucket_name, "Bucket name contains invalid characters.")
            if not BucketValidations.is_valid_owner(owner):
                raise BucketExceptions.InvalidOwnerError(owner)
            if not BucketValidations.is_length_owner_valid(owner):
                raise BucketExceptions.InvalidOwnerError(owner, "Owner is not in the valid length")
            if region is not None:
                if not BucketValidations.is_region_valid(region):
                    raise BucketExceptions.InvalidRegionError(region)

            new_bucket = Bucket(bucket_name, owner, region)
            self.buckets.append(new_bucket)
            self.storage_manager.create_directory(f'buckets/{bucket_name}')
            self.storage_manager.create_directory(f'buckets/{bucket_name}/locks')
            self.bucket_manager.createInMemoryBucket(new_bucket)

    def delete(self, bucket_name):
        """Delete bucket."""
        if not BucketValidations.valid_type_paramters(bucket_name):
            raise BucketExceptions.InvalidBucketNameError(bucket_name)

        if not BucketValidations.bucket_exists(self.buckets, bucket_name):
            raise BucketExceptions.BucketNotFoundError(bucket_name)

        bucket = self.get(bucket_name)
        self.buckets.remove(bucket)
        self.storage_manager.delete_directory(bucket_name)
        self.bucket_manager.deleteInMemoryBucket(bucket)

    def get(self, bucket_name):
        """Get Bucket by name."""
        if not BucketValidations.valid_type_paramters(bucket_name):
            raise BucketExceptions.InvalidBucketNameError(bucket_name)
        if not BucketValidations.bucket_exists(self.buckets, bucket_name):
            raise BucketExceptions.BucketNotFoundError(bucket_name)

        for bucket in self.buckets:
            if bucket.bucket_name == bucket_name:
                return bucket
        return None
