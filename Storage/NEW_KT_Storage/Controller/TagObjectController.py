from typing import Optional, Dict
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from Models import TagObjectModel
from Service.Classes.TagObjectService import TagObjectService


class TagObjctController:
    def __init__(self):
        self.tag_service = TagObjectService()

    def create_tag(self, key, value):
        return self.tag_service.create(key, value)

    def get_tag(self, key:str)->TagObjectModel:
        return self.tag_service.get(key)

    def delete_tag(self, key: str):
        return self.tag_service.delete(key)

    def modify_tag(self, key: str, changes: Dict):
        return self.tag_service.modify(key, changes)

    def describe_tag(self):
        return self.tag_service.describe()
