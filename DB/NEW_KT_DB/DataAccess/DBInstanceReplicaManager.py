"""
DBInstanceManager Module
------------------------

This module provides the `DBInstanceManager` class, which manages `DBInstanceModel` objects in memory using an `ObjectManager`.
The manager handles operations such as creating, modifying, describing, and deleting DBInstance records stored in memory.

### Classes:
    - DBInstanceManager: A class for managing in-memory database instances (`DBInstanceModel`), using JSON serialization for object data storage.

### Methods:
    - `__init__(db_file: str)`: Initializes the `DBInstanceManager` with a database file and creates the management table.
    - `close_connections()`: Closes any open database connections.
    - `createInMemoryDBInstance(db_instance)`: Stores a `DBInstanceModel` object in memory by serializing it as JSON.
    - `modifyDBInstance(db_instance)`: Modifies an existing `DBInstanceModel` in memory by updating its metadata.
    - `describeDBInstance(db_instance_identifier) -> Dict[str, Any]`: Retrieves a `DBInstanceModel` object by its identifier and returns its metadata.
    - `deleteInMemoryDBInstance(db_instance_identifier)`: Deletes a `DBInstanceModel` from memory using its identifier.
    - `getDBInstance(db_instance_identifier)`: Fetches a `DBInstanceModel` object by its identifier.
    - `is_db_instance_exists(db_instance_identifier) -> bool`: Checks if a `DBInstanceModel` exists in memory by its identifier.

### Example Usage:
    db_instance_manager = DBInstanceManager('db_file.db')

    # Create a DBInstance in memory
    db_instance = DBInstanceModel(db_instance_identifier="db1", status="available")
    db_instance_manager.createInMemoryDBInstance(db_instance)

    # Modify a DBInstance
    db_instance.status = "stopped"
    db_instance_manager.modifyDBInstance(db_instance)

    # Describe a DBInstance
    metadata = db_instance_manager.describeDBInstance("db1")

    # Check if a DBInstance exists
    exists = db_instance_manager.is_db_instance_exists("db1")

    # Delete a DBInstance
    db_instance_manager.deleteInMemoryDBInstance("db1")

### Dependencies:
    - ObjectManager: A class responsible for low-level management of data in memory.
    - DBInstanceModel: A model class representing a database instance, serialized as JSON for storage.

"""


from typing import Dict, Any
import json
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from KT_Cloud.DB.NEW_KT_DB.Models.DBInstanceReplicaModel import DBInstanceModel


class DBInstanceManager:
    object_name = __name__.split('.')[-1].replace('Manager', '').lower()

    def __init__(self, db_file: str):
        self.object_manager = ObjectManager(db_file)
        self.object_manager.create_management_table(
            self.object_name, DBInstanceModel.table_structure, pk_column_data_type='TEXT')


    def createInMemoryDBInstance(self, db_instance):
        metadata = json.dumps(db_instance.to_dict())
        data = (db_instance.db_instance_identifier, metadata)
        self.object_manager.save_in_memory(self.object_name, data)

    def modifyDBInstance(self, db_instance):
        metadata = json.dumps(db_instance.to_dict())
        updates = f"metadata = '{metadata}'"
        criteria = f"db_instance_identifier = '{db_instance.db_instance_identifier}'"
        self.object_manager.update_in_memory(
            self.object_name, updates, criteria)

    def describeDBInstance(self, db_instance_identifier) -> Dict[str, Any]:
        criteria = f"db_instance_identifier = '{db_instance_identifier}'"
        result = self.object_manager.get_from_memory(
            self.object_name, "*", criteria)

        if result:
            metadata = json.loads(result[0][1])
            return metadata
        else:
            raise ValueError(f"DB Instance with identifier {db_instance_identifier} not found.")

    def deleteInMemoryDBInstance(self, db_instance_identifier):
        criteria = f"db_instance_identifier = '{db_instance_identifier}'"
        self.object_manager.delete_from_memory_by_criteria(
            self.object_name, criteria)

    def getDBInstance(self, db_instance_identifier):
        criteria = f"db_instance_identifier = '{db_instance_identifier}'"
        # result = self.object_manager.get_from_memory(self.object_name, ["*"], criteria)
        result = self.object_manager.get_from_memory(
            self.object_name, "*", criteria)

        return result

    def isDbInstanceExists(self, db_instance_identifier):
        try:
            self.object_manager.get_from_memory(
                self.object_name, db_instance_identifier,)
            return True
        except ValueError:
            return False
    
    def is_db_instance_exist(self, db_instance_identifier: int) -> bool:
        """
        Check if a DBInstance with the given identifier exists in memory.

        Params: db_instance_identifier: The primary key (ID) of the DBInstance to check.
        
        Return: True if the DBInstance exists, otherwise False.
        """
        # Check if the object exists by its primary key in the management table
        return bool(self.object_manager.db_manager.is_object_exist(
            self.object_manager._convert_object_name_to_management_table_name(self.object_name), 
            criteria=f"db_instance_identifier = '{db_instance_identifier}'"
        ))