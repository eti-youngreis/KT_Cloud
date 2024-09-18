import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from DataAccess.TagObjectManager import TagObjectManager
from Models.TagObjectModel import TagObject


class TagObjectService:
    def __init__(self) -> None:
        """Initialize the TagObjectService with a TagObjectManager instance."""
        self.tag_dal = TagObjectManager()

    def create(self, key, value)->None:
        """Create a new TagObject and save it in memory.

        Args:
            key (str): The key for the TagObject.
            value (str): The value for the TagObject.
        """
        tag = TagObject(key, value)
        return self.tag_dal.createInMemoryTagObject(tag)

    def get(self, key: str)->TagObject:
        """Retrieve a TagObject from memory by its key.

        Args:
            key (str): The key of the TagObject to retrieve.
        """
        return self.tag_dal.get_tag_object_from_memory(key)

    def delete(self, key: str):
        """Delete a TagObject from memory by its key.

        Args:
            key (str): The key of the TagObject to delete.
        """
        return self.tag_dal.deleteInMemoryTagObject(key)

    def modify(self, last_key: str, key: str = None, value: str = None):
        """Modify an existing TagObject's key and/or value.

        Args:
            last_key (str): The original key of the TagObject to modify.
            key (str, optional): The new key to set. Defaults to None.
            value (str, optional): The new value to set. Defaults to None.
        """
        update_fields = ""

        if key is not None:
            update_fields += f'''Key = '{key}' '''

        if value is not None:
            if update_fields:
                update_fields += ", "
            update_fields += f'''Value = '{value}' '''

        return self.tag_dal.putTagObject(last_key=last_key, updates=update_fields)

    def describe(self):
        """Retrieve a list of all TagObjects from memory.
        """
        return self.tag_dal.describeTagObject()
