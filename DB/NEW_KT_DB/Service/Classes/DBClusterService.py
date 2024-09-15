from typing import Dict, Optional
from DataAccess import ClusterManager
from Models import DBClusterModel
from Abc import DBO
from Validation import Validation
from DataAccess import DBClusterManager

class DBClusterService(DBO):
    def __init__(self, dal: ClusterManager):
        self.dal = dal
    
    
    # validations here
    

    def create(self, **attributes):
        '''Create a new DBCluster.'''
        # create object in code using DBClusterModel.init()- assign all **attributes
        # create physical object as described in task
        # save in memory using DBClusterManager.createInMemoryDBCluster() function
        pass


    def delete(self):
        '''Delete an existing DBCluster.'''
        # assign None to code object
        # delete physical object
        # delete from memory using DBClusterManager.deleteInMemoryDBCluster() function- send criteria using self attributes
        pass


    def describe(self):
        '''Describe the details of DBCluster.'''
        # use DBClusterManager.describeDBCluster() function
        pass


    def modify(self, **updates):
        '''Modify an existing DBCluster.'''
        # update object in code
        # modify physical object
        # update object in memory using DBClusterManager.modifyInMemoryDBCluster() function- send criteria using self attributes
        pass


    def get(self):
        '''get code object.'''
        # return real time object
        pass
