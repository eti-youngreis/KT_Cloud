from Validation import is_valid_bucket_name, is_valid_policy_name
from DataAccess.BucketDAL import BucketDAL
from Service.Abc.STOE import STOE

class Bucket(STOE):

    def __init__(self):
        self.BucketDAL = BucketDAL()

    def create(self, bucket_name: str, **kwargs):
        """Create a new bucket."""
        #בדיקת שם תקין
         if not is_valid_bucket_name(bucket_name) :
            raise ValueError(f"Invalid bucket name: '{bucket_name}'.")

        #לבדוק אם זה שם יחיד 
        if self.get(bucket_name):
            raise ValueError("Bucket already exists")

        return BucketDAL.create()

    def delete(self, *args,**kwargs):
        """Delete an existing storage object."""
        pass

    def get(self, *args, **kwargs):
        """get storage object."""
        pass

    def put(self, *args, **kwargs):
        """put storage object."""
        pass

    def list(self, *args, **kwargs):
        """list storage object."""
        pass

    def head(self, *args, **kwargs):
        """check if object exists and is accessible with the appropriate user permissions."""
        pass
  