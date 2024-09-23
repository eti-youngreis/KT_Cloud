from typing import Dict, Any
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from Models.TagModel import Tag
from DataAccess.ObjectManager import ObjectManager


class TagManager:
    def __init__(self, db_file: str = "Tags.db"):
        """Initialize ObjectManager with the database connection."""
        self.object_manager = ObjectManager(db_file)
        self.object_manager.object_manager.db_manager.create_table(
            "mng_Tags", Tag.TABLE_STRUCTURE
        )

    def createInMemoryTag(self, tag: Tag):
        """Save a Tag instance in memory."""
        return self.object_manager.save_in_memory(
            object_name=Tag.OBJECT_NAME, object_info=tag.to_sql()
        )

    def deleteInMemoryTag(self, key: str):
        """Delete a Tag from memory based on its primary key (key)."""
        return self.object_manager.delete_from_memory_by_pk(
            object_name=Tag.OBJECT_NAME, pk_column=Tag.PK_COULMN, pk_value=key
        )

    def describeTag(self) -> list[Tag]:
        """Retrieve a list of Tags from memory.

        Returns:
            list: A list of Tag instances.

        Raises:
            NoTagsFoundError: If no Tags are found in memory.
        """
        # Retrieve the list of tuples (key, value) from memory
        results = self.object_manager.get_from_memory(object_name=Tag.OBJECT_NAME)

        if results == []:
            return []

        # Create a list of Tag instances from the results
        tag_objects = [Tag(key=res[0], value=res[1]) for res in results]

        return tag_objects

    def putTag(self, old_key: str, updates: str = None):
        """Update a Tag in memory based on its primary key."""
        if not updates:
            raise ValueError("No fields to update")

        return self.object_manager.update_in_memory(
            object_name=Tag.OBJECT_NAME,
            updates=updates,
            criteria=f""" {Tag.PK_COULMN}='{old_key}' """,
        )

    def get_tag_object_from_memory(self, key: str) -> Tag:
        """Retrieve a Tag from memory based on its primary key (key).

        Args:
            key (str): The primary key of the Tag to retrieve.

        Returns:
            Tag: The retrieved Tag instance, or None if not found.
        """
        # Retrieve data from memory based on the primary key
        result = self.object_manager.get_from_memory(
            object_name=Tag.OBJECT_NAME, criteria=f"{Tag.PK_COULMN}='{key}'"
        )

        if not result or not result[0]:
            raise KeyError("No Tag.")

        # Create a Tag from the retrieved data
        key, value = result[0]  # Unpack key and value from the result
        tag_object = Tag(key=key, value=value)

        return tag_object
