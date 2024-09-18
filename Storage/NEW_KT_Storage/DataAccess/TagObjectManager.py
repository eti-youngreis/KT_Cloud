from typing import Dict, Any
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from Models.TagObjectModel import TagObject
from DataAccess.ObjectManager import ObjectManager

class TagObjectManager:
    def __init__(self, db_file: str = "Tags.db"):
        """Initialize ObjectManager with the database connection."""
        self.object_manager = ObjectManager(db_file)
        self.table_structure = ", ".join(["Key TEXT PRIMARY KEY", "Value TEXT"])
        self.object_manager.object_manager.db_manager.create_table("mng_Tags", self.table_structure)

    def createInMemoryTagObject(self, tag: TagObject):
        """Save a TagObject instance in memory."""
        return self.object_manager.save_in_memory(
            object_name=TagObject.OBJECT_NAME, object_info=tag.to_sql()
        )

    def deleteInMemoryTagObject(self, key: str):
        """Delete a TagObject from memory based on its primary key (key)."""
        return self.object_manager.delete_from_memory_by_pk(
            object_name=TagObject.OBJECT_NAME, pk_column=TagObject.PK_COULMN, pk_value=key
        )

    def describeTagObject(self):
        """Retrieve a list of TagObjects from memory."""
        return self.object_manager.get_from_memory(object_name=TagObject.OBJECT_NAME)

    def putTagObject(self, last_key: str, updates: str = None):
        """Update a TagObject in memory based on its primary key."""
        if not updates:
            raise ValueError("No fields to update")

        return self.object_manager.update_in_memory(
            object_name=TagObject.OBJECT_NAME,
            updates=updates,
            criteria=f""" {TagObject.PK_COULMN}='{last_key}' """,
        )

    def get_tag_object_from_memory(self, key: str):
        """Retrieve a TagObject from memory based on its primary key (key)."""
        return self.object_manager.get_from_memory(
            object_name=TagObject.OBJECT_NAME, criteria=f""" {TagObject.PK_COULMN}='{key}' """
        )
