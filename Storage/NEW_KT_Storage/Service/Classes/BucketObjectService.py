from typing import Dict, Optional
from Models import BucketObjectModel
from Abc import STO
from Validation import Validation
from DataAccess import BucketObjectManager

class BucketObjectService(STO):
    def __init__(self, dal: BucketObjectManager):
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