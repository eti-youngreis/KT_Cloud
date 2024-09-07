from DataAccess.StorageManager import StorageManager
from DataAccess.ObjectManager import ObjectManager
from Models.BucketModel import BucketModel

class BucketDAL:
    def __init__(self): 

        self.storage_manager = StorageManager()
        self.object_manager = ObjectManager()


    def create(self,bucket_name) -> None:
        """create a new bucket."""
        return ObjectManager.create(bucket_name)
        

    def insert(self) -> None:
        """Insert a new Object into managment file."""
        pass

    def update(self) -> None:
        """Update an existing object in managment file."""
        pass

    def get(self) -> BucketModel:
        """Get bucket from managment file."""
        return ObjectManager.get(bucket_name)

        

    def delete(self) -> None:
        """Delete an object from managment file."""
        return ObjectManager.delete(bucket_name)

