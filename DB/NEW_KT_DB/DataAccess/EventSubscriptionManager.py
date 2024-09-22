from typing import Dict, Any
import json
import sqlite3
from DataAccess import ObjectManager

class EventSubscriptionManager:
    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        self.object_manager = ObjectManager(db_file)
        self.table_name ='eventt_subscription'
        self.create_table()


    def createInMemoryEventSubscription(self):
        self.object_manager.save_in_memory()


    def deleteInMemoryEventSubscription(self):
        self.object_manager.delete_from_memory()


    def describeEventSubscription(self):
        self.object_manager.get_from_memory()


    def modifyEventSubscription(self):
        self.object_manager.update_in_memory()
    
