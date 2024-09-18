from Storage.NEW_KT_Storage.DataAccess.BucketObjectManager import BucketObjectManager
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from Storage.NEW_KT_Storage.Models.BucketObjectModel import BucketObject
from Storage.NEW_KT_Storage.Service.Abc.STO import STO
import Storage.NEW_KT_Storage.Validation.BucketObjectValiditions as validation
# from Storage.NEW_KT_Storage.DataAccess.VersionManager import VersionManager
# from Storage.NEW_KT_Storage.Service.Classes.ObjectVersioningService import ObjectVersioningService
# from Storage.NEW_KT_Storage.Controller.ObjectVersioningController import ObjectVersioningController



class BucketObjectService(STO):
    def __init__(self, path):
        self.dal = BucketObjectManager(path)
        self.storage_manager = StorageManager(path)
        # versions:
        # manager = VersionManager(path+"\\version.db",path)
        # service = ObjectVersioningService(manager)
        # self.versions_controller = ObjectVersioningController(service)

    def validation_for_object(self, **attributes):
        if not validation.check_required_params_object(attributes):
            raise ValueError("bucket_name and object_key are required")
        bucket_name = attributes.get('bucket_name', None)
        object_key = attributes.get('object_key', None)
        if bucket_name is None:
            raise ValueError("bucket_name is required, cant be None")
        if object_key is None:
            raise ValueError("object_key is required, cant be None")
        if not validation.is_bucket_name_valid(bucket_name):
            raise ValueError("Incorrect bucket name")
        if not validation.is_bucket_object_name_valid(object_key):
            raise ValueError("Incorrect object key")
        if not self.storage_manager.is_directory_exist(bucket_name):
            raise ValueError("Bucket not found")

    def create(self, **attributes):
        '''Create a new BucketObject.'''
        self.validation_for_object(**attributes)
        if not self.storage_manager.is_directory_exist(attributes.get('bucket_name', None)):
            raise ValueError("Bucket not found")

        # create physical object
        bucket_name = attributes.get('bucket_name', None)
        object_key = attributes.get('object_key', None)
        full_path = bucket_name + "\\" + object_key
        content = attributes.get('content', '')
        self.storage_manager.create_file(full_path, content)

        # save object in memory
        bucket_object = BucketObject(**attributes)
        self.dal.createInMemoryBucketObject(bucket_object)

    def delete(self, bucket_name, object_key, version_id=None):
        '''Delete an existing BucketObject.'''

        self.validation_for_object(bucket_name=bucket_name, object_key=object_key)
        if not self.storage_manager.is_file_exist(bucket_name + "\\" + object_key):
            raise ValueError("Object not found")

        if version_id is None:
            # delete from memory using BucketObjectManager
            self.dal.deleteInMemoryBucketObject(bucket_name, object_key)

            # delete physical object
            path = bucket_name + '\\' + object_key
            self.storage_manager.delete_file(path)

        else:
            pass
            # call the delete function of version
            # self.versions_controller.delete_version_object(bucket_name,object_key,version_id)

    def put(self, **attributes):
        '''Modify an existing BucketObject.'''
        '''Add an object to the bucket and if the object exists then delete the old object and add the new one'''

        self.validation_for_object(**attributes)

        bucket_name = attributes.get('bucket_name', None)
        object_key = attributes.get('object_key', None)
        version_id = attributes.get('version_id', None)
        content = attributes.get('content', '')

        if version_id is None:
            # Deletes the old object if it exists
            full_path = bucket_name + "\\" + object_key
            if self.storage_manager.is_file_exist(full_path):
                self.delete(bucket_name, object_key)

            # Add the object
            self.storage_manager.create_file(full_path, content)
            bucket_object = BucketObject(**attributes)
            self.dal.createInMemoryBucketObject(bucket_object)
        else:
            # call the create functions of version
            # self.versions_controller.create_version_object(bucket_name, object_key,content)
            pass

    def get(self, bucket_name, object_key, version_id=None):
        '''get object.'''

        self.validation_for_object(bucket_name=bucket_name, object_key=object_key)
        if not self.storage_manager.is_file_exist(bucket_name + "\\" + object_key):
            raise ValueError("Object not found")

        if version_id is None:
            return self.dal.describeBucketObject(bucket_name + object_key)
        else:
            pass
            # call the function of get_version from version_controller
            # self.versions_controller.get_version_object(bucket_name,object_key,version_id)

    def get_all(self, bucket_name):
        '''get all objects'''

        if bucket_name is None or not validation.is_bucket_name_valid(bucket_name):
            raise ValueError("Incorrect bucket name")
        if not self.storage_manager.is_directory_exist(bucket_name):
            raise ValueError("Bucket not found")
        if self.storage_manager.list_files_in_directory(bucket_name)==[]:
            raise ValueError("There are no objects in the bucket")

        return self.dal.getAllObjects(bucket_name)
