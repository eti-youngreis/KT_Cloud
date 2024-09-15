from typing import Dict, Any
import json
from DataAccess import ObjectManager


class DBInstanceManager:
    def __init__(self, db_file: str):
        self.object_manager = ObjectManager(db_file)
        self.table_name = 'db_instance'
        self.create_table()

    def create_table(self):
        table_structure = '''
            db_instance_identifier TEXT PRIMARY KEY,
            metadata TEXT NOT NULL
        '''
        self.object_manager.create_management_table(
            self.table_name, table_structure)

    def createInMemoryDBInstance(self, db_instance):
        metadata = json.dumps(db_instance.to_dict())
        data = (db_instance.db_instance_identifier, metadata)
        self.object_manager.save_in_memory(self.table_name, data)

    def modifyDBInstance(self, db_instance):
        metadata = json.dumps(db_instance.to_dict())
        updates = {'metadata': metadata}
        criteria = f"db_instance_identifier = '{
            db_instance.db_instance_identifier}'"
        self.object_manager.update_in_memory(
            type_object='db_instance', updates=updates, criteria=criteria)

    def describeDBInstance(self, db_instance_identifier) -> Dict[str, Any]:
        criteria = f"db_instance_identifier = '{db_instance_identifier}'"
        result = self.object_manager.get_from_memory(
            type_object='db_instance', columns=["*"], criteria=criteria)

        if result:
            metadata = json.loads(result['metadata'])
            return metadata
        else:
            raise ValueError(f"DB Instance with identifier { db_instance_identifier} not found.")

    def deleteInMemoryDBInstance(self, db_instance_identifier):
        criteria = f"db_instance_identifier = '{db_instance_identifier}'"
        self.object_manager.delete_from_memory(
            self.table_name, criteria)

    def getDBInstance(self, db_instance_identifier):
        criteria = f"db_instance_identifier = '{db_instance_identifier}'"
        result = self.object_manager.get_from_memory(
            type_object=self.table_name, columns=["*"], criteria=criteria)

        # להחיות את האובייקט    
