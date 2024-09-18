from typing import Dict, Optional
from Models import aclObjectModel
from Abc import STO
from Validation import Validation
from DataAccess import AclObjectManager

class AclObjectService(STO):
    def __init__(self, dal: AclObjectManager):
        self.dal = dal
    
    
    # validations here
    

    def create(self, **attributes):
        '''Create a new BucketObject.'''
        # create object in code using BucketObjectModel.init()- assign all **attributes
        # create physical object as described in task
        # save in memory using BucketObjectManager.createInMemoryBucketObject() function
        pass


    def delete(self):
        '''Delete an existing BucketObject.'''
        # assign None to code object
        # delete physical object
        # delete from memory using BucketObjectManager.deleteInMemoryBucketObject() function- send criteria using self attributes
        pass


    def describe(self):
        '''Describe the details of BucketObject.'''
        # use BucketObjectManager.describeBucketObject() function
        pass


    def put(self, **updates):
        '''Modify an existing BucketObject.'''
        # update object in code
        # modify physical object
        # update object in memory using BucketObjectManager.modifyInMemoryBucketObject() function- send criteria using self attributes
        pass


    def get(self):
        '''get code object.'''
        # return real time object
        pass




class ACLService:
    def create(self, acl_object):
        # קריאה לניהול אובייקט ושימוש בפונקציה של insert_proxy
        KT_Storage.ObjectManager.insert_proxy(acl_object)

    def get(self, acl_object_id):
        # קריאה לפונקציה שמביאה אובייקט
        return KT_Storage.ObjectManager.get_proxy(acl_object_id)

    def delete(self, acl_object_id):
        # מחיקה של אובייקט
        KT_Storage.ObjectManager.delete_proxy(acl_object_id)

    def modify(self, acl_object):
        # עדכון אובייקט קיים
        KT_Storage.ObjectManager.update_proxy(acl_object)

    def describe(self, acl_object_id):
        # מתארים את האובייקט
        return KT_Storage.ObjectManager.describe_proxy(acl_object_id)
