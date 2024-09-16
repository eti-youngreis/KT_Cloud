from typing import Dict, Any
import json
import sqlite3
from DataAccess import ObjectManager

class DBInstanceManager:
    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        self.object_manager = ObjectManager(db_file)
        self.table_name ='instance_managment'
        self.create_table()

    def create_table(self):
        '''create objects table in the database'''
        table_schema = 'db_instance_id TEXT PRIMARY KEY ,metadata TEXT NOT NULL'
        self.object_manager.create_management_table(self.table_name, table_schema)


    def createInMemoryDBInstance(self):
        self.object_manager.save_in_memory()


    def deleteInMemoryDBInstance(self):
        self.object_manager.delete_from_memory()


    def describeDBInstance(self):
        self.object_manager.get_from_memory()


    def modifyDBInstance(self):
        self.object_manager.update_in_memory()
    
    def is_db_instance_identifier_exist(self, db_instance_identifier: int) -> bool:
        '''Check if an db_instance with the given ID exists in the database.'''
        result = self.db_manager.select_and_return_records_from_table(self.table_name, ['object_id'], f'object_id = {object_id}')
        return bool(result)

