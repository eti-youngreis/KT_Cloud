from typing import Dict, Any, List

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Models.DBSubnetGroupModel import DBSubnetGroup

class DBSubnetGroupManager:
    def __init__(self, object_manager):
        self.object_manager = object_manager
        self.object_manager.create_management_table(DBSubnetGroup.table_name, DBSubnetGroup.table_structure)

    def create(self, subnet_group: DBSubnetGroup):
        self.object_manager.insert_object_to_management_table(DBSubnetGroup.table_name, subnet_group.to_sql())

    def get(self, name: str):
        data = self.object_manager.get_object_from_management_table(DBSubnetGroup.pk_column, DBSubnetGroup.table_name, name)
        if data:
            data_mapping = {'db_subnet_group_name':name}
            for key, value in data[name].items():
                data_mapping[key] = value 
            return DBSubnetGroup(**data_mapping)
        else:
            raise ValueError(f"subnet group with name '{name}' not found")

    
    def delete(self, name: str):
        self.object_manager.delete_object_from_management_table(DBSubnetGroup.table_name, f"{DBSubnetGroup.pk_column}='{name}'")

    def describe(self, name: str): 
        return self.get(name).to_dict()
    
    def modify(self, subnet_group: DBSubnetGroup):
        updates = subnet_group.to_dict()
        del updates['db_subnet_group_name']
        self.object_manager.update_object_in_management_table_by_criteria(table_name = DBSubnetGroup.table_name, updates = updates, criteria = f"{DBSubnetGroup.pk_column}='{subnet_group.db_subnet_group_name}'")