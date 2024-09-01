from Validation import is_valid_bucket_name, is_valid_policy_name
from DataAccess.BucketDAL import BucketDAL
from Service.Abc.STOE import STOE
from Models.BucketModel import BucketModel
from DataAccess.StorageManager import StorageManager

class Bucket(STOE):

    def __init__(self):
        self.BucketDAL = BucketDAL()
        self.StorageManager=StorageManager()

    async def create(self, bucket_name: str)->BucketModel:
        """Create a new bucket."""

        # Validating the bucket name
        if not is_valid_bucket_name(bucket_name) :
            raise ValueError(f"Invalid bucket name: '{bucket_name}'.")
        
        bucket_obj=BucketModel(bucket_name)

        await BucketDAL.create(bucket_name,bucket_obj)
        await StorageManager.create(bucket_name)
        return bucket_obj

    def delete(self,bucket_name: str)->bool:
        """Delete an existing storage object."""

        bucket = self.get(bucket_name)

        if not bucket:
            raise ValueError(f"Bucket '{bucket_name}' not found.")

        await BucketDAL.delete(bucket_name,bucket_obj)
        await StorageManager.delete(bucket_name)
        return True

    def get(self, *args, **kwargs):
        """get storage object."""
        pass

    def put(self, *args, **kwargs):
        """put storage object."""
        pass

    def list(self)->list:
        """list storage object."""
        pass
        

    def head(self, *args, **kwargs):
        """check if object exists and is accessible with the appropriate user permissions."""
        pass
  