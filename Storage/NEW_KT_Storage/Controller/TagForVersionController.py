from typing import Optional, Dict
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from Models import TagModel
from Service.Classes.TagForVersionService import TagForVersionService


class TagForVersionController:
    def __init__(self):
        self.tag_service = TagForVersionService()

    def add_tag(self, key: str, version_id: str):
        return self.tag_service.add_tag(key, version_id)

    def remove_tag(self, tag_key: str, version_id: str) -> TagModel:
        return self.tag_service.remove_tag(tag_key=tag_key, version_id=version_id)

    def get_tags_by_version(self, version_id: str):
        return self.tag_service.get_tags_by_version(version_id)

