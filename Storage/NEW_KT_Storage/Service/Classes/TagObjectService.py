import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from DataAccess.TagObjectManager import TagObjectManager
from Models.TagObjectModel import TagObject


class TagObjectService:
    def __init__(self) -> None:
        self.tag_dal = TagObjectManager()

    def create(self, key, value):
        tag = TagObject(key, value)
        return self.tag_dal.createInMemoryTagObject(tag)

    def get(self, key: str):
        return self.tag_dal.get_tag_object_from_memory(key)

    def delete(self, key: str):
        return self.tag_dal.deleteInMemoryTagObject(key)

    def modify(self, last_key: str, key: str = None, value: str=None):
        update_fields = ""

        if key is not None:
            update_fields += f'''Key = '{key}' '''

        if value is not None:
            if update_fields:
                update_fields += ", "
            update_fields += f'''Value = '{value}' '''

        return self.tag_dal.putTagObject( last_key=last_key, updates = update_fields)
    
    def describe(self):
        return self.tag_dal.describeTagObject()
