import os
import sys

# Adjusting the path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from DataAccess.TagForVersionManager import TagForVersionManager
from Models.TagForVersionModel import TagForVersion
from Validation import TagValidation as validation


class TagForVersionService:
    def __init__(self, db_file: str = "TagForVersions.db") -> None:
        """Initialize the TagService with a TagManager instance."""
        self.tag_dal = TagForVersionManager(db_file)

    def _validate_key(self, key: str) -> None:
        """Validate the key parameter."""
        if not validation.check_required_params(key):
            raise ValueError("Key is required.")
        if not validation.is_valid_key_name(key):
            raise ValueError("Incorrect key name.")

    def _validate_value(self, value: str) -> None:
        """Validate the value parameter."""
        if not validation.is_valid_key_name(value):
            raise ValueError("Incorrect value name.")

    def add_tag(self, tag_key: str, version_id: str) -> None:
        """Create a new Tag and save it in memory.

        Args:
            tag_key (str): The key for the Tag.
            version_id (str): The version ID for the Tag.
        """
        self._validate_key(tag_key)
        tag_for_version = TagForVersion(tag_key, version_id)
        self.tag_dal.createInMemoryTagforVersion(tag_for_version)

    def remove_tag(self, tag_key: str, version_id: str) -> TagForVersion:
        """Remove a Tag from memory by its key and version ID.

        Args:
            tag_key (str): The key of the Tag to remove.
            version_id (str): The version ID associated with the Tag.
        """
        return self.tag_dal.deleteInMemoryTagForVersion(tag_key=tag_key, version_id=version_id)

    def get_tags_by_version(self, version_id: str):
        """Retrieve all tags associated with a specific version ID."""
        return self.tag_dal.get_tags_from_memory_by_version(version_id=version_id)

    def describe_tags(self) -> list:
        """Describe all tags for versions."""
        return self.tag_dal.describeTagForVersionObject()
