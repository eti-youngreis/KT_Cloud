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
        self.object_manager.object_manager.db_manager.create_table("mng_Tags", TagObject.TABLE_STRUCTURE)

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



    def describeTagObject(self) -> list[TagObject]:
        """Retrieve a list of TagObjects from memory.

        Returns:
            list: A list of TagObject instances.

        Raises:
            NoTagObjectsFoundError: If no TagObjects are found in memory.
        """
        # Retrieve the list of tuples (key, value) from memory
        results = self.object_manager.get_from_memory(object_name=TagObject.OBJECT_NAME)

        if results == []:
            return []

        # Create a list of TagObject instances from the results
        tag_objects = [TagObject(key=res[0], value=res[1]) for res in results]

        return tag_objects


    def putTagObject(self, old_key: str, updates: str = None):
        """Update a TagObject in memory based on its primary key."""
        if not updates:
            raise ValueError("No fields to update")

        return self.object_manager.update_in_memory(
            object_name=TagObject.OBJECT_NAME,
            updates=updates,
            criteria=f""" {TagObject.PK_COULMN}='{old_key}' """,
        )

    def get_tag_object_from_memory(self, key: str) -> TagObject:
        """Retrieve a TagObject from memory based on its primary key (key).

        Args:
            key (str): The primary key of the TagObject to retrieve.

        Returns:
            TagObject: The retrieved TagObject instance, or None if not found.
        """
        # Retrieve data from memory based on the primary key
        result = self.object_manager.get_from_memory(
            object_name=TagObject.OBJECT_NAME, 
            criteria=f"{TagObject.PK_COULMN}='{key}'"
        )

        if not result or not result[0]:
            raise KeyError("No TagObject.") 

        # Create a TagObject from the retrieved data
        key, value = result[0]  # Unpack key and value from the result
        tag_object = TagObject(key=key, value=value)
        
        return tag_object
