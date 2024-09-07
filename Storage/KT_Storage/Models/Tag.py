from typing import Dict, List, Optional


class Tag:
    def __init__(self, key, value):
        self.key = key
        self.value = value

    # def add_tag(self, key, value):
    #     # Add tag with key and value
    #     self.tags[key] = value

    # def remove_tag(self, key):
    #     # Remove tag by key
    #     if key in self.tags:
    #         del self.tags[key]

    # def get_tag(self, key):
    #     # Remove tag by key
    #     return self.tags.get(key, None)

    # def list_tags(self):
    #     # Return all tags
    #     return self.tags

    # def __str__(self):
    #     return str(self.tags)

    def to_dict(self) -> Dict[str, str]:
        return {"key": self.key, "Value": self.value}
