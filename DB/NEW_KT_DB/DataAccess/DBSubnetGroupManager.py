from typing import Dict, Any, List

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Models.DBSubnetGroupModel import DBSubnetGroup

class DBSubnetGroupManager:
    def __init__(self, db_manager, object_manager):
        self.db_manager = db_manager
        self.object_manager = object_manager
        self.db_manager.create_table(DBSubnetGroup.table_name, DBSubnetGroup.table_structure)

    def create(self, subnet_group: DBSubnetGroup):
        self.object_manager.save_in_memory(DBSubnetGroup.table_name, subnet_group.to_sql())

    def get(self, name: str):
        cols = ['db_subnet_group_name', 'db_subnet_group_description', 'vpc_id', 'subnets', 'db_subnet_group_arn', 'status']
        data = self.object_manager.get_from_memory(DBSubnetGroup.pk_column, DBSubnetGroup.table_name, name, cols = cols)
        return data
    
    def delete(self, name: str):
        self.object_manager.delete_from_memory(name)

    def describe(self, name: str): 
        cols = ['db_subnet_group_name', 'db_subnet_group_description', 'vpc_id', 'subnets', 'db_subnet_group_arn', 'status']
        return self.object_manager.get_from_memory(DBSubnetGroup.pk_column, DBSubnetGroup.table_name, name, cols)
    
    def modify(self, subnet_group: DBSubnetGroup):
        updates = subnet_group.to_dict()
        del updates['db_subnet_group_name']
        self.object_manager.update_in_memory(DBSubnetGroup.pk_column, subnet_group.db_subnet_group_name, DBSubnetGroup.table_name, updates)