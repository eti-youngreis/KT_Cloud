from Abc import DBO
from DataAccess import DBProxyManger
from Models import DBProxyModel
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
import json
# from Validation import Validation



class DBProxyService(DBO):
    def __init__(self, dal: DBProxyManger, storage_manager: StorageManager) -> None:
        self.dal = dal
        self.storage_manager = storage_manager

    # validations here
    

    def create(self, **attributes):
        '''Create a new DBCluster.'''
        # create object in code using DBClusterModel.init()- assign all **attributes
        proxy = DBProxyModel(**attributes)
        # create physical object as described in task
        json_content = json.dumps(proxy.to_dict())
        self.storage_manager.create_file(proxy.db_proxy_name + ".json",json_content )
        # save in memory using DBClusterManager.createInMemoryDBCluster() function
        object_name = self.__class__.__name__.replace('Service', '')
        self.dal.create_in_memory_DBproxy(object_name, proxy.to_dict(), proxy.db_proxy_name)
        



    def delete(self, db_proxy_name):
        '''Delete an existing DBCluster.'''
        # assign None to code object
        # delete physical object
        self.storage_manager.delete_file(db_proxy_name + ".json")
        # delete from memory using DBClusterManager.deleteInMemoryDBCluster() function- send criteria using self attributes
        object_name = self.__class__.__name__.replace('Service', '')
        self.dal.delete_in_memory_DBProxy(object_name=object_name, object_id=db_proxy_name)
        


    def describe(self, db_proxy_name):
        '''Describe the details of DBCluster.'''
        # use DBClusterManager.describeDBCluster() function
        object_name = self.__class__.__name__.replace('Service', '')
        return self.dal.describe_DBProxy(object_name, db_proxy_name)


    def modify(self, db_proxy_name, **updates):
        '''Modify an existing DBCluster.'''
        # update object in code
        # modify physical object
        # update object in memory using DBClusterManager.modifyInMemoryDBCluster() function- send criteria using self attributes
        object_name = self.__class__.__name__.replace('Service', '')
        self.dal.modify_DBProxy(object_name=object_name, updates=updates, db_proxy_name=db_proxy_name)



    def get(self):
        '''get code object.'''
        # return real time object
        pass
