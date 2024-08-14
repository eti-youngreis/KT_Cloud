from Abc import STOE
from Validation import is_valid_bucket_name, is_valid_policy_name

class Bucket(STOE):
    
    def create(self,*args, **kwargs):
        """Create a new bucket."""
        pass

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
  