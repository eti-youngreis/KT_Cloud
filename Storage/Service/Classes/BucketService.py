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
        if not is_valid_bucket_name(bucket_name):
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

        

    async def get(self, bucket_name: str)->BucketModel:
        """get storage object."""

        if not bucket_name:
            raise ValueError("Bucket name must be provided.")

        metadata=await StorageManager.get(bucket_name)
        if not metadata:
            raise ValueError("Bucket with this name was not found.")

        return metadata

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
        
        # Checking the existence of the bucket
        bucket = ObjectManager.get_bucket(bucket_name)
        if not bucket:
            raise ValueError(f"Bucket '{bucket_name}' does not exist.")
        
        # Cors configuration validations
        if not self.validate_cors_configuration(cors_configuration):
            raise ValueError("Invalid CORS configuration format.")
        
        # Creating a cors object and defining the cors configuration for the bucket
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

    def put_bucket_encryption(self, bucket:Bucket, is_encrypted = True):
        if bucket.name not in self.object_manager.get_buckets():
            raise FileNotFoundError("bucket or key not exist")
        bucket.encrypt_mode= True
        for obj in bucket.objects.values():
            obj:ObjectModel
            obj_content= self.object_manager.read(bucket.name, obj.key)
            encrypted_data, obj_encrypt_config = obj.encryption.encrypt_content(obj_content)
            self.object_service.put(obj, encrypted_data, obj_encrypt_config)

