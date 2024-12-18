from typing import Optional, Dict
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from Models import TagModel
from Service.Classes.TagService import TagService


class TagController:
    def __init__(self, db_file: str = "Tags.db"):
        self.tag_service = TagService(db_file)

    def create_tag(self, key, value):
        return self.tag_service.create(key, value)

    def get_tag(self, key: str) -> TagModel:
        return self.tag_service.get(key)

    def delete_tag(self, key: str):
        return self.tag_service.delete(key)

    def modify_tag(self, old_key: str, key: str, value: str):
        return self.tag_service.modify(old_key, key, value)

    def describe_tags(self):
        return self.tag_service.describe()
