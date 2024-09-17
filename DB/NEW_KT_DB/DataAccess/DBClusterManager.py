from typing import Dict, Any
import json
import sqlite3
from DataAccess import ObjectManager
from typing import Optional

class DBClusterManager:
    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        self.object_manager = ObjectManager.ObjectManager(db_file)
        self.object_name ='clusters'
        self.pk_column = 'ClusterID'
        # self.create_table()


    def createInMemoryDBCluster(self, cluster_to_save):
        self.object_manager.save_in_memory(self.object_name, cluster_to_save)


    def deleteInMemoryDBCluster(self,cluster_identifier):
        self.object_manager.delete_from_memory_by_pk(self.object_name, self.pk_column, cluster_identifier)

    def describeDBCluster(self, cluster_id):
        self.object_manager.get_from_memory(self.object_name, criteria=f" {self.pk_column} = {cluster_id}")

    def modifyDBCluster(self, cluster_id, updates):
        self.object_manager.update_in_memory(self.object_name, updates, criteria=f" {self.pk_column} = {cluster_id}")

    def select(self, name:Optional[str] = None, columns = ["*"]):
        data = self.object_manager.get_from_memory(self.object_name,criteria=f" {self.pk_column} = {name}")
        if data:
            data_to_return = [{col:data[col] for col in columns}]
            data_to_return[self.pk_column] = name
            return data_to_return
            
        else:
            raise ValueError(f"db cluster with name '{name}' not found")

    def is_exists(self, name):
        """check if object exists in table"""
        try:
            self.select(name)
            return True
        except:
            return False