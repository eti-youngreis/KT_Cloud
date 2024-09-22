from typing import Dict, Any
import json
import sqlite3
from NEW_KT_DB.DataAccess import ObjectManager
from NEW_KT_DB.Models.DBClusterModel import Cluster
from typing import Optional

class DBClusterManager:
    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        self.object_manager = ObjectManager.ObjectManager(db_file)
        self.object_manager.create_management_table(Cluster.object_name, table_structure = Cluster.table_schema)

    def createInMemoryDBCluster(self, cluster_to_save):
        self.object_manager.save_in_memory(Cluster.object_name, cluster_to_save)


    def deleteInMemoryDBCluster(self,cluster_identifier):
        self.object_manager.delete_from_memory_by_pk(Cluster.object_name, Cluster.pk_column, cluster_identifier)

    def describeDBCluster(self, cluster_id):
        return self.object_manager.get_from_memory(Cluster.object_name, criteria=f" {Cluster.pk_column} = '{cluster_id}'")

    def modifyDBCluster(self, cluster_id, updates):
        self.object_manager.update_in_memory(Cluster.object_name, updates, criteria=f" {Cluster.pk_column} = '{cluster_id}'")

    def get(self, cluster_id: str):
        data = self.object_manager.get_from_memory(Cluster.object_name, criteria=f" {Cluster.pk_column} = '{cluster_id}'")
        if data:
            data_mapping = {'db_cluster_identifier':cluster_id}
            for key, value in data[cluster_id].items():
                data_mapping[key] = value 
            return Cluster(**data_mapping)
        else:
            None

    def is_db_instance_exist(self, db_cluster_identifier: int) -> bool:
        """
        Check if a DBInstance with the given identifier exists in memory.

        Params: db_instance_identifier: The primary key (ID) of the DBInstance to check.
        
        Return: True if the DBInstance exists, otherwise False.
        """
        # Check if the object exists by its primary key in the management table
        return bool(self.object_manager.db_manager.is_object_exist(
            self.object_manager._convert_object_name_to_management_table_name(Cluster.object_name), 
            criteria=f"{Cluster.pk_column} = '{db_cluster_identifier}'"
        ))
        
    def get_all_clusters(self):
        return self.object_manager.get_all_objects_from_memory(Cluster.object_name)