from typing import Dict, Any
import json
import sqlite3
from NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from NEW_KT_DB.Models.DBClusterParameterGroupModel import DBClusterParameterGroup


class DBClusterParameterGroupManager:
    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        self.object_manager = ObjectManager(db_file)
        self.object_manager.create_management_table(
        DBClusterParameterGroup.get_object_name(), DBClusterParameterGroup.table_structure, 'TEXT')


    def createInMemoryDBCluster(self, data):
        self.object_manager.save_in_memory(self.__class__.__name__[:-len("Manager")], data)


    def deleteInMemoryDBCluster(self, group_name):
        self.object_manager.delete_from_memory_by_pk(self.__class__.__name__[:-len("Manager")], pk_column=DBClusterParameterGroup.pk_column, pk_value=group_name)

    def modifyDBCluster(self, group_name, data):
        self.object_manager.update_in_memory(self.__class__.__name__[:-len("Manager")], updates=data, criteria=f'{DBClusterParameterGroup.pk_column} = "{group_name}"')
    
    def get(self, group_name):
        return self.object_manager.get_from_memory(self.__class__.__name__[:-len("Manager")], columns='*', criteria=f'{DBClusterParameterGroup.pk_column} = "{group_name}"')

    def get_all_groups(self):
        return self.object_manager.get_all_objects_from_memory(self.__class__.__name__[:-len("Manager")])

    def is_identifier_exist(self, group_name):
        result= self.object_manager.get_from_memory(self.__class__.__name__[:-len("Manager")], columns='*', criteria=f'{DBClusterParameterGroup.pk_column} = "{group_name}"')  
        if result !=[]:
            return True
        return False   