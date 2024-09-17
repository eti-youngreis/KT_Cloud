from typing import Dict, Any
import json
import sqlite3
from DataAccess import ObjectManager

class DBSnapshotManager:
    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        self.object_manager = ObjectManager(db_file)
        self.create_table()


    def createInMemoryDBSnapshot(self):
        self.object_manager.save_in_memory()


    def deleteInMemoryDBSnapshot(self):
        self.object_manager.delete_from_memory()


    def describeDBSnapshot(self):
        self.object_manager.get_from_memory()


    def modifyDBSnapshot(self):
        self.object_manager.update_in_memory()
    