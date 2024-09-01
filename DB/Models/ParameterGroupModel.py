from abc import abstractmethod
from typing import Dict, Optional, List

class ParameterGroupModel:
    def __init__(self, group_name: str, group_family: str, description: Optional[str] = None, tags: Optional[List[str]] = None):
        self.group_name = group_name
        self.group_family = group_family
        self.description = description
        self.parameters = self.load_default_parameters()
        self.tags = tags

    def to_dict(self) -> Dict:
        return {
            'group_name': self.group_name,
            'group_family': self.group_family,
            'description': self.description,
            'parameters': self.parameters,
            'tags': self.tags
        }

    @abstractmethod
    def load_default_parameters(self):
        pass