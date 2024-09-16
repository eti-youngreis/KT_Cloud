import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from DataAccess.TagObjectManager import TagObjectManager
from Models.TagObjectModel import TagObject


class TagObjectService:
    def __init__(self) -> None:
        self.tag_dal = TagObjectManager()

    def create(self, key, value):
        tag = TagObject(key, value)
        return self.tag_dal.createInMemoryTagObject(tag)

    # def get(self, tag: TagObject):
    #     return self.tag_dal.get(tag)

    def delete(self, tag: TagObject):
        return self.tag_dal.deleteInMemoryTagObject(tag)

    def modify(self, tag: TagObject):
        return self.tag_dal.putTagObject(tag)

    def describe(self, tag: TagObject):
        return self.tag_dal.describeTagObject(tag)
