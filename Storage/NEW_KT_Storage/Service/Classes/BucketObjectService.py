from typing import Dict, Optional

from aiofiles import os

from Storage.NEW_KT_Storage.DataAccess.BucketObjectManager import BucketObjectManager
# from Models import BucketObjectModel
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from Storage.NEW_KT_Storage.Models.BucketObjectModel import BucketObject
from Storage.NEW_KT_Storage.Service.Abc.STO import STO
import Storage.NEW_KT_Storage.Validation.BucketObjectValiditions as validation


# from Storage.NEW_KT_Storage.Controller.BucketController import BucketController


# from Validation import Validation
# from DataAccess import BucketObjectManager





class BucketObjectService(STO):
    def __init__(self):
        self.dal = BucketObjectManager()
        self.storage_manager = StorageManager("C:\\Users\\user1\\Desktop\\server")
        # self.bucket_object: BucketObject = None

    # validations here
    def validation_for_object(self,**attributes):


        if not validation.check_required_params_object(attributes):
            raise ValueError("bucket_name and object_key are required")
        bucket_name = attributes.get('bucket_name', None)
        object_key = attributes.get('object_key', None)
        if bucket_name is None:
            raise ValueError("bucket_name is required, cant be None")
        if object_key is None:
            raise ValueError("object_key is required, cant be None")
        if not validation.is_bucket_object_name_valid(bucket_name):
            raise ValueError("Incorrect length of bucket name")
        if not validation.is_bucket_object_name_valid(object_key):
            raise ValueError("Incorrect length of object key")


    def create(self, **attributes):
        '''Create a new BucketObject.'''
        self.validation_for_object(**attributes)

        # create physical object
        bucket_name = attributes.get('bucket_name',None)
        object_key = attributes.get('object_key',None)
        full_path = bucket_name+"\\"+object_key
        content=attributes.get('content','')
        self.storage_manager.create_file(full_path,content)

        # save object in memory
        bucket_object = BucketObject(**attributes)
        self.dal.createInMemoryBucketObject(bucket_object)


    def delete(self,bucket_name, object_key,version_id=None):

        '''Delete an existing BucketObject.'''
        self.validation_for_object(bucket_name=bucket_name,object_key=object_key)

        if version_id is None:
            # delete from memory using BucketObjectManager
            self.dal.deleteInMemoryBucketObject(bucket_name, object_key)

            # delete physical object
            path = bucket_name + '\\' + object_key
            self.storage_manager.delete_file(path)

        else:
            pass
            # call the delete function of version

    def put(self, **attributes):
        '''Modify an existing BucketObject.'''
        self.validation_for_object(**attributes)

        bucket_name = attributes.get('bucket_name', None)
        object_key = attributes.get('object_key', None)
        version_id= attributes.get('version_id', None)

        if version_id is None:
            full_path = bucket_name + "\\" + object_key
            if self.storage_manager.is_file_exist(full_path):
                self.delete(bucket_name,object_key)
            content=attributes.get('content', '')
            self.storage_manager.create_file(full_path, content)

            bucket_object = BucketObject(**attributes)
            self.dal.createInMemoryBucketObject(bucket_object)
        else:
            # call the create functions of version
            pass



    def get(self,bucket_name,object_key,version_id=None):

        self.validation_for_object(bucket_name=bucket_name,object_key=object_key)
        '''get code object.'''
        if version_id is None:
            return self.dal.describeBucketObject(bucket_name+object_key)
        else:
            pass
            # call the function of get_version from version_controller


    def get_all(self,bucket_name):

        if bucket_name is None or not validation.is_bucket_object_name_valid(bucket_name):
            raise ValueError("Incorrect bucket name")

        return self.dal.getAllObjects(bucket_name)


