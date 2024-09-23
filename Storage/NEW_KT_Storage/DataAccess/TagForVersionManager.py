from typing import List
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from Models.TagModel import Tag
from Models.TagForVersionModel import TagForVersion
from DataAccess.ObjectManager import ObjectManager


class TagForVersionManager:
    def __init__(self, db_file: str = "TagForVersions.db"):
        """Initialize ObjectManager with the database connection."""
        self.object_manager = ObjectManager(db_file)
        self.object_manager.object_manager.db_manager.create_table(
            "mng_TagForVersions", TagForVersion.TABLE_STRUCTURE
        )

    def create_in_memory_tag_for_version(self, tag_for_version: TagForVersion) -> None:
        """Save a Tag instance in memory."""
        self.object_manager.save_in_memory(
            object_name=TagForVersion.OBJECT_NAME,
            object_info=tag_for_version.to_sql()
        )

    def delete_in_memory_tag_for_version(self, tag_key: str, version_id: str) -> None:
        """Delete a Tag from memory based on its primary key (key)."""
        self.object_manager.delete_from_memory_by_criteria(
            object_name=TagForVersion.OBJECT_NAME,
            criteria=f"TagKey='{tag_key}' AND VersionId='{version_id}'"
        )

    def describe_tag_for_version_object(self) -> List[TagForVersion]:
        """Retrieve a list of Tags from memory.

        Returns:
            List[TagForVersion]: A list of Tag instances.
        """
        results = self.object_manager.get_from_memory(object_name=TagForVersion.OBJECT_NAME)

        # Return empty list if no results
        if not results:
            return []

        # Create a list of TagForVersion instances from the results
        return [TagForVersion(tag_key=res[0], version_id=res[1]) for res in results]

    def get_tags_from_memory_by_version(self, version_id: str) -> List[TagForVersion]:
        """Retrieve Tags from memory based on their version ID.

        Args:
            version_id (str): The version ID of the Tags to retrieve.

        Returns:
            List[TagForVersion]: A list of TagForVersion instances, or an empty list if none found.
        """
        results = self.object_manager.get_from_memory(
            object_name=TagForVersion.OBJECT_NAME,
            criteria=f"VersionId='{version_id}'"
        )

        # Return empty list if no results
        if not results:
            return []

        # Create a list of TagForVersion instances from the results
        return [TagForVersion(tag_key=res[0], version_id=res[1]) for res in results]
