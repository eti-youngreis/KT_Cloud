import os
import sys
from typing import Dict, Any, List
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from DataAccess.ObjectManager import ObjectManager
from Models.DBInstanceModel import DBInstance

class DBInstanceManager:
    def __init__(self, object_manager:ObjectManager):
        self.object_manager = object_manager
        self.object_manager.create_management_table( DBInstance.table_name, DBInstance.table_structure)

    def createInMemoryDBInstance(self,db_instance:DBInstance):
        columns=self.object_manager.db_manager.get_column_names_of_table(self.object_manager._convert_object_name_to_management_table_name(DBInstance.table_name))
        columns_str = ', '.join(columns)
        self.object_manager.save_in_memory(DBInstance.table_name,db_instance.to_sql(),columns=columns_str)


    def deleteInMemoryDBInstance(self,db_instance_identifier:str):
        self.object_manager.delete_from_memory_by_pk(pk_column=DBInstance.pk_column ,pk_value= db_instance_identifier,object_name=DBInstance.table_name)


    def describeDBInstance(self,db_instance_identifier:str):
        return self.object_manager.get_from_memory(criteria=f"{DBInstance.pk_column} = '{db_instance_identifier}'",object_name=DBInstance.table_name,columns='*')#[db_instance_identifier]


    def modifyDBInstance(self,db_instance_identifier,updates):
        self.object_manager.update_in_memory(criteria=f"{DBInstance.pk_column} = '{db_instance_identifier}'",object_name=DBInstance.table_name,updates=updates)
    
    def is_db_instance_identifier_exist(self, db_instance_identifier: int) -> bool:
        '''Check if an db_instance with the given ID exists in the database.'''
        return self.object_manager.db_manager.is_object_exist(self.object_manager._convert_object_name_to_management_table_name(DBInstance.table_name),criteria=f"{DBInstance.pk_column} = '{db_instance_identifier}'")
