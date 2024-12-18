from Storage.NEW_KT_Storage.DataAccess.ObjectManager import ObjectManager
from Storage.NEW_KT_Storage.Models.BucketModel import Bucket


class BucketManager:

    def __init__(self, db_file: str):
        """Initialize BucketManager with the database connection."""
        self.object_manager = ObjectManager(db_file=db_file)
        self.object_name = "Bucket"
        self.object_manager.object_manager.db_manager.create_table("mng_Buckets", Bucket.table_structure)

    def createInMemoryBucket(self, bucket: Bucket):
        self.object_manager.save_in_memory(self.object_name,bucket.to_sql())

    def deleteInMemoryBucket(self, bucket: Bucket):
        self.object_manager.delete_from_memory_by_pk(self.object_name, Bucket.pk_column, bucket.bucket_name)




