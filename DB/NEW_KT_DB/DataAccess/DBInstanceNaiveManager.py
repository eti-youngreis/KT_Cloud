import os
import sys
from typing import Dict, Any, List
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from DataAccess.ObjectManager import ObjectManager
from Models.DBInstanceNaiveModel import DBInstance

class DBInstanceManager:
    def __init__(self, object_manager: ObjectManager):
        self.object_manager = object_manager
        # Create the management table for DBInstance using its object name and table structure
        self.object_manager.create_management_table(DBInstance.object_name, DBInstance.table_structure)

    def createInMemoryDBInstance(self, db_instance: DBInstance):
        """
        Create a new DBInstance in memory.

        Params db_instance: DBInstance object to be saved in memory.
        """
        self.object_manager.save_in_memory(DBInstance.object_name, db_instance.to_sql())

    def deleteInMemoryDBInstance(self, db_instance_identifier: str):
        """
        Delete a DBInstance from memory by its identifier.

        Params: db_instance_identifier: The primary key (ID) of the DBInstance to delete.
        """
        self.object_manager.delete_from_memory_by_pk(
            pk_column=DBInstance.pk_column, 
            pk_value=db_instance_identifier, 
            object_name=DBInstance.object_name
        )

    def describeDBInstance(self, db_instance_identifier: str):
        """
        Retrieve the details of a DBInstance based on its identifier.

        Params: db_instance_identifier: The primary key (ID) of the DBInstance to describe.
        
        Return: List of DBInstance attributes matching the criteria.
        """
        # Fetch DBInstance from memory using its primary key
        return self.object_manager.get_from_memory(
            criteria=f"{DBInstance.pk_column} = '{db_instance_identifier}'", 
            object_name=DBInstance.object_name, 
            columns='*'
        )

    def modifyDBInstance(self, db_instance_identifier: str, updates: str):
        """
        Modify the attributes of an existing DBInstance in memory.

        Params: db_instance_identifier: The primary key (ID) of the DBInstance to modify.
                updates: SQL-like string containing the updates to be applied (e.g., "port = '3306'").
        """
        # Update the instance's attributes based on the provided update string
        self.object_manager.update_in_memory(
            criteria=f"{DBInstance.pk_column} = '{db_instance_identifier}'", 
            object_name=DBInstance.object_name, 
            updates=updates
        )
    
    def is_db_instance_exist(self, db_instance_identifier: int) -> bool:
        """
        Check if a DBInstance with the given identifier exists in memory.

        Params: db_instance_identifier: The primary key (ID) of the DBInstance to check.
        
        Return: True if the DBInstance exists, otherwise False.
        """
        # Check if the object exists by its primary key in the management table    
        try:
            res =  bool(self.object_manager.db_manager.is_object_exist(
            self.object_manager._convert_object_name_to_management_table_name(DBInstance.object_name), 
            criteria=f"{DBInstance.pk_column} = '{db_instance_identifier}'"
        ))
            return res
        except Exception as e:  # Catch specific exceptions or use Exception for general errors
            # print(f"Error checking DB cluster existence: {e}")
            return False
