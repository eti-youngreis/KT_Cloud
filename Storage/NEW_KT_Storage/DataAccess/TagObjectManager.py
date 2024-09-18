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
    def __init__(self, db_file: str = "Tags.db"):
        """Initialize ObjectManager with the database connection."""
        self.object_manager = ObjectManager(db_file)

        table_columns = "Key TEXT PRIMARY KEY", "Value TEXT"
        columns_str = ", ".join(table_columns)
        self.object_manager.object_manager.db_manager.create_table("mng_Tags", columns_str)

    def createInMemoryTagObject(self, tag: TagObject):
        return self.object_manager.save_in_memory(
            object_name=TagObject.OBJECT_NAME, object_info=tag.to_sql()
        )

    def deleteInMemoryTagObject(self, key: str):
        return self.object_manager.delete_from_memory_by_pk(
            object_name=TagObject.OBJECT_NAME, pk_column=TagObject.PK_COULMN, pk_value=key
        )

    def describeTagObject(self):
        return self.object_manager.get_from_memory(object_name=TagObject.OBJECT_NAME)

    def putTagObject(self, last_key: str, updates: str = None):
        if  not updates:
            raise ValueError("No fields to update")

        return self.object_manager.update_in_memory(
            object_name=TagObject.OBJECT_NAME,
            updates=updates,
            criteria=f""" {TagObject.PK_COULMN}='{last_key}' """,
        )

    def get_tag_object_from_memory(self, key: str):
        return self.object_manager.get_from_memory(
            object_name=TagObject.OBJECT_NAME, criteria=f""" {TagObject.PK_COULMN}='{key}' """
        )
