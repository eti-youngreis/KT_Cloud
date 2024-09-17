from typing import Dict, Any
import json
import sqlite3
from NEW_KT_DB.DataAccess.ObjectManager import ObjectManager

class DBClusterManager:
    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        self.object_manager = ObjectManager(db_file)
        # self.table_name ='cluster_managment'
        # self.create_table()


    def createInMemoryDBCluster(self):
        self.object_manager.save_in_memory()


    def deleteInMemoryDBCluster(self):
        self.object_manager.delete_from_memory()


    def describeDBCluster(self):
        self.object_manager.get_from_memory()


    def modifyDBCluster(self):
        self.object_manager.update_in_memory()

    def get_all_clusters(self):
         return self.object_manager.get_from_memory(self.__class__.__name__[:-len("Manager")])
   