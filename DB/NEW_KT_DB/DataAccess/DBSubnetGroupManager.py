from typing import Dict, Any, List

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from DataAccess import ObjectManager
from Models.DBSubnetGroupModel import DBSubnetGroup


class DBSubnetGroupManager:
    def __init__(self, object_manager: ObjectManager):
        self.object_manager = object_manager
        self.object_manager.create_management_table(
            DBSubnetGroup.table_name, DBSubnetGroup.table_structure
        )

    def create(self, subnet_group: DBSubnetGroup):
        self.object_manager.save_in_memory(
            DBSubnetGroup.table_name, subnet_group.to_sql_insert()
        )

    def get(self, name: str):
        data = self.object_manager.get_from_memory(
            DBSubnetGroup.table_name, criteria=f"{DBSubnetGroup.pk_column} = '{name}'"
        )
        if data:
            return DBSubnetGroup(*data[0])
        else:
            raise ValueError(f"subnet group with name '{name}' not found")

    def delete(self, name: str):
        exists = self.object_manager.get_from_memory(
            DBSubnetGroup.table_name, criteria=f"{DBSubnetGroup.pk_column} = '{name}'"
        )
        # if doesn't exist -> exists = []
        if exists:
            self.object_manager.delete_from_memory_by_pk(
                DBSubnetGroup.table_name, DBSubnetGroup.pk_column, name
            )
        else:
            raise ValueError(f"subnet group with name '{name}' doesn't exist")

    def describe(self, name: str):
        return self.get(name).to_dict()

    def modify(self, subnet_group: DBSubnetGroup):
        updates = subnet_group.to_sql_update()
        self.object_manager.update_in_memory(
            DBSubnetGroup.table_name,
            updates,
            criteria=f"{DBSubnetGroup.pk_column} = '{subnet_group.db_subnet_group_name}'",
        )

    def list_db_subnet_groups(self):
        results = self.object_manager.get_from_memory(DBSubnetGroup.table_name)
        return [DBSubnetGroup(*result) for result in results]
