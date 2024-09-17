from typing import Dict, Any
import json
import sqlite3
from NEW_KT_DB.DataAccess.ObjectManager import ObjectManager

class DBClusterParameterGroupManager:
    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        self.object_manager = ObjectManager(db_file)
        # self.table_name ='cluster_managment'
        # self.create_table()


    def createInMemoryDBCluster(self, data, id):
        self.object_manager.save_in_memory(self.__class__.__name__[:-len("Manager")], data, id)


    def deleteInMemoryDBCluster(self, id):
        self.object_manager.delete_from_memory(self.__class__.__name__[:-len("Manager")], object_id= id)


    def describeDBCluster(self, id):
        self.object_manager.get_from_memory(self.__class__.__name__[:-len("Manager")],object_id= id)


    def modifyDBCluster(self, id, data):
        self.object_manager.update_in_memory(self.__class__.__name__[:-len("Manager")], data, object_id= id)
    
    def get(self, id):
        return self.object_manager.get_from_memory(self.__class__.__name__[:-len("Manager")],object_id= id)

    def get_all_groups(self):
        return self.object_manager.get_from_memory(self.__class__.__name__[:-len("Manager")])

    def is_identifier_exist(self, id):
        return self.object_manager.is_object_exist(self.__class__.__name__[:-len("Manager")],object_id= id)



