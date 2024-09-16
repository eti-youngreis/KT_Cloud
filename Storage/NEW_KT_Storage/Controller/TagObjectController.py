from typing import Optional
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from Models import TagObjectModel
from Service.Classes import TagObjectService


class TagObjctController:
    def __init__(self):
        self.tag_service = TagObjectService()

    def create_tag(self, key, value,version_id):
        return self.tag_service.create(key, value)

    def get_tag(self, ):
        return self.tag_service.get(pk_value, pk_column)

    def delete_tag(self, pk_value, pk_column):
        return self.tag_service.delete(pk_value, pk_column)

    def modify_tag(self):
        return self.tag_service.modify()

    def describe_tag(self):
        return self.tag_service.describe()
