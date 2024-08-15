from Abc import STOE
from Validation import is_valid_bucket_name, is_valid_policy_name
from DataAccess import ObjectManager
from Models.CorsModel import CORSConfigurationModel
from Validtion import validate_cors_configuration


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

    def put_cors_configuration(self, bucket_name, cors_configuration):
        # בדיקת קיום הדלי
        bucket = ObjectManager.get_bucket(bucket_name)
        if not bucket:
            raise ValueError(f"Bucket '{bucket_name}' does not exist.")
        
        # ולידציה של CORS configuration
        if not self.validate_cors_configuration(cors_configuration):
            raise ValueError("Invalid CORS configuration format.")
        
        # יצירת אובייקט CORSConfigurationModel והגדרת CORS לדלי
        cors_config = CORSConfigurationModel(cors_configuration)
        bucket.cors_configuration = cors_config
        bucket.ObjectManager.update()
        print(f"CORS configuration updated for bucket '{bucket_name}'.")


    def get_cors_configuration(self, bucket_name, cors_configuration):
        bucket = ObjectManager.get_bucket(bucket_name)
        return bucket.cors_configuration
        print(f"CORS configuration updated for bucket '{bucket_name}'.")

  
    def delete_cors_configuration(self, bucket_name, cors_configuration):
        bucket = get_bucket(bucket_name)
        bucket.cors_configuration = None
        bucket.ObjectManager.update()
        print(f"CORS configuration updated for bucket '{bucket_name}'.")


  