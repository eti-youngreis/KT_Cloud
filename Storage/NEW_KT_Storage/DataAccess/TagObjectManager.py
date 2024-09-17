from typing import Dict, Any
import json
import sqlite3
import sys
import os

# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from Models.TagObjectModel import TagObject
from DataAccess.ObjectManager import ObjectManager


class TagObjectManager:
    def __init__(self, db_file: str = "tag"):
        """Initialize ObjectManager with the database connection."""
        self.object_manager = ObjectManager("tag",db_file)
        # self.object_manager.create_table()

    def createInMemoryTagObject(self, tag: TagObject):
        return self.object_manager.save_in_memory(tag)

    def deleteInMemoryTagObject(self, tag: TagObject):
        return self.object_manager.delete_from_memory(tag)

    def describeTagObject(self, tag: TagObject):
        return self.object_manager.get_from_memory(tag)

    def putTagObject(self, tag: TagObject):
        return self.object_manager.update_in_memory(tag)

    def get_tag_object_from_memory(self, tag:TagObject):
        return self.object_manager.get_from_memory(tag)
    
        
