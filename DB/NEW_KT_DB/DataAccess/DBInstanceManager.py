from typing import Dict, Any
import json
from DataAccess import ObjectManager

class DBInstanceManager:
    def __init__(self, db_file: str):
        self.object_manager = ObjectManager(db_file)
        self.table_name = 'db_instance_management'
        self.create_table()

    def create_table(self):
        table_structure = '''
            db_instance_identifier TEXT PRIMARY KEY,
            metadata TEXT NOT NULL
        '''
        self.object_manager.create_management_table(self.table_name, table_structure)

    def save_to_management_table(self, db_instance):
        metadata = json.dumps(db_instance.to_dict())
        data = (db_instance.db_instance_identifier, metadata)
        self.object_manager.save_in_memory(self.table_name, data)

    def update_management_table(self, db_instance):
        metadata = json.dumps(db_instance.to_dict())
        updates = {'metadata': metadata}
        criteria = f"db_instance_identifier = '{db_instance.db_instance_identifier}'"
        self.object_manager.update_object_in_management_table_by_criteria(self.table_name, updates, criteria)

    def get_from_management_table(self, db_instance_identifier) -> Dict[str, Any]:
        criteria = f"db_instance_identifier = '{db_instance_identifier}'"
        result = self.object_manager.get_object_from_management_table(self.table_name, criteria)
        if result:
            metadata = json.loads(result['metadata'])
            return metadata
        else:
            raise ValueError(f"DB Instance with identifier {db_instance_identifier} not found.")

    def delete_from_management_table(self, db_instance_identifier):
        criteria = f"db_instance_identifier = '{db_instance_identifier}'"
        self.object_manager.delete_object_from_management_table(self.table_name, criteria)
