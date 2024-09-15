from typing import Dict, Any
import json
import sqlite3
from DataAccess.ObjectManager import ObjectManager

class DBProxyEndpointManager:
    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        self.object_manager = ObjectManager(db_file)


    def createInMemoryDBProxyEndpoint(self, db_proxy_endpoint_id, db_proxy_endpoint_metadata):
        self.object_manager.save_in_memory(db_proxy_endpoint_id, db_proxy_endpoint_metadata)


    def deleteInMemoryDBProxyEndpoint(self, db_proxy_endpoint_id):
        self.object_manager.delete_from_memory(db_proxy_endpoint_id)


    def describeDBProxyEndpoint(self, db_proxy_endpoint_id):
        self.object_manager.get_from_memory(db_proxy_endpoint_id)


    def modifyDBProxyEndpoint(self, db_proxy_endpoint_id):
        self.object_manager.update_in_memory(db_proxy_endpoint_id)
    
