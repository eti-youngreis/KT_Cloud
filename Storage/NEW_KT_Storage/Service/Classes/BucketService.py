from Storage.NEW_KT_Storage.Models.BucketModel import Bucket
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from Storage.NEW_KT_Storage.DataAccess.BucketManager import BucketManager
import Storage.NEW_KT_Storage.Validation.BucketValidations as BucketValidations
class BucketService:
    def __init__(self,storage_path="D:/s3_project/server"):
        self.storage_manager = StorageManager(storage_path)
        self.bucket_manager = BucketManager("D:/s3_project/tables/Buckets.db")
        self.create_table()
        self.buckets = self.load_buckets()

    def load_buckets(self):
        data_list = self.bucket_manager.object_manager.get_all_objects_from_memory("Bucket")
        return [Bucket(bucket_name=row[0], owner=row[1],region=row[2],create_at=row[3]) for row in data_list]
    def create_table(self):
        table_columns = "object_id TEXT PRIMARY KEY", "Owner TEXT", "Region TEXT", "created_at DATETIME"
        columns_str = ", ".join(table_columns)
        self.bucket_manager.object_manager.object_manager.db_manager.create_table("mng_Buckets", columns_str)

    def create(self, bucket_name: str, owner: str,region:str):
        """Create a new Bucket."""
        if BucketValidations.bucket_exists(self.buckets, bucket_name):
            raise ValueError("This bucket already exists.")
        if not BucketValidations.is_length_range(bucket_name):
            raise ValueError("Bucket name must be between 3 and 63 characters.")
        if not  BucketValidations.is_bucket_name_valid(bucket_name):
             raise ValueError("Bucket name contains invalid characters.")
        if not BucketValidations.is_valid_owner(owner):
            raise ValueError("Owner name contains invalid characters.")
        if not BucketValidations.is_length_owner_valid(owner):
            raise ValueError("Owner is not in the valid length")

        new_bucket = Bucket(bucket_name, owner, region)
        self.buckets.append(new_bucket)
        self.storage_manager.create_directory(f'buckets/{bucket_name}')
        self.bucket_manager.createInMemoryBucket(new_bucket)

    def delete(self, bucket_name):
        """delete Bucket"""
        if not BucketValidations.bucket_exists(self.buckets, bucket_name):
            raise ValueError("This bucket does not exist.")

        bucket = self.get(bucket_name)
        self.buckets.remove(bucket)
        self.storage_manager.delete_directory(bucket_name)
        self.bucket_manager.deleteInMemoryBucket(bucket)

    def get(self, bucket_name):
        """get Bucket by name"""
        if not BucketValidations.bucket_exists(self.buckets, bucket_name):
            raise ValueError("This bucket does not exist.")

        for bucket in self.buckets:
            if bucket.bucket_name == bucket_name:
                return bucket
        return None
        # bucket = self.bucket_manager.getBucket(bucket_name)

