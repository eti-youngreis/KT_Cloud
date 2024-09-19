import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from DataAccess.TagObjectManager import TagObjectManager
from DataAccess.StorageManager import StorageManager
from Models.TagObjectModel import TagObject
from Validation import TagValidation as validation


class TagObjectService:
    def __init__(
        self, db_file: str = "Tags.db", storage_file: str = "local_storage"
    ) -> None:
        """Initialize the TagObjectService with a TagObjectManager instance."""
        self.tag_dal = TagObjectManager(db_file)
        self.tags = []
        self.load_tags()

    def load_tags(self):
        self.tags = self.tag_dal.describeTagObject()

    def validation_for_key(self, key):
        if not validation.check_required_params(key):
            raise ValueError("key is required")
        if not validation.is_valid_key_name(key):
            raise ValueError("Incorrect key name")

    def validation_for_value(self, value):
            if not validation.is_valid_key_name(value):
                raise ValueError("Incorrect key name")
            
    def create(self, key, value) -> None:
        """Create a new TagObject and save it in memory.

        Args:
            key (str): The key for the TagObject.
            value (str): The value for the TagObject.
        """
        self.validation_for_key(key=key)

        self.validation_for_value(value=value)
        
        if validation.key_exists(tags=self.tags, key=key):
            raise KeyError("Duplicate key")
        
        tag = TagObject(key, value)
        create_result = self.tag_dal.createInMemoryTagObject(tag)
        self.load_tags()
        return create_result

    def get(self, key: str) -> TagObject:
        """Retrieve a TagObject from memory by its key.

        Args:
            key (str): The key of the TagObject to retrieve.
        """
        self.validation_for_key(key=key)

        if not validation.key_exists(tags=self.tags, key=key):
            raise KeyError("no such key")

        return self.tag_dal.get_tag_object_from_memory(key)

    def delete(self, key: str):
        """Delete a TagObject from memory by its key.

        Args:
            key (str): The key of the TagObject to delete.
        """
        self.validation_for_key(key=key)

        if not validation.key_exists(tags=self.tags, key=key):
            raise KeyError("no such key")

        delete_result = self.tag_dal.deleteInMemoryTagObject(key)
        self.load_tags()
        return delete_result

    def modify(self, old_key: str, key: str = None, value: str = None):
        """Modify an existing TagObject's key and/or value.

        Args:
            old_key (str): The original key of the TagObject to modify.
            key (str, optional): The new key to set. Defaults to None.
            value (str, optional): The new value to set. Defaults to None.
        """
        self.validation_for_key(key=old_key)

        if not validation.key_exists(tags=self.tags, key=old_key):
            raise KeyError("no such key")

        update_fields = ""

        if key is not None:
            self.validation_for_key(key=key)
            if validation.key_exists(tags=self.tags, key=key):
                raise ValueError("Douplicate key")
            update_fields += f"""Key = '{key}' """

        if value is not None:
            self.validation_for_value(value=value)

            if update_fields:
                update_fields += ", "
            update_fields += f"""Value = '{value}' """

        put_tag_result = self.tag_dal.putTagObject(
            old_key=old_key, updates=update_fields
        )
        self.load_tags()
        return put_tag_result

    def describe(self):
        """Retrieve a list of all TagObjects from memory."""
        return self.tag_dal.describeTagObject()


