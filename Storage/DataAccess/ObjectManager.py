from DataAccess import StorageManager
from typing import Dict, Any
import json
import sqlite3

class ObjectManager:
    def __init__(self, storage_manager:StorageManager): 
        self.storage_manager = storage_manager
    
    """here will be access to json managment file"""
    def __init__(self):
        pass
        
    def insert(self) -> None:
        """Insert a new Object into managment file."""
        pass

    def update(self) -> None:
        """Update an existing object in managment file."""
        pass

    def get(self) -> None:
        """Get object from managment file."""
        pass
        

    def delete(self) -> None:
        """Delete an object from managment file."""
        pass