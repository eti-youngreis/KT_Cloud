
from typing import Dict
class OptionGroupModel:
    def __init__(self, engine_name, major_engine_version, option_group_description, option_group_name, tags = None):
        self.engine_name = engine_name
        self.major_engine_version = major_engine_version
        self.option_group_description = option_group_description
        self.option_group_name = option_group_name
        self.tags = tags
        self.available = True

    def to_dict(self) -> Dict:
        return {
            'engineName': self.engine_name,
            'majorEngineVersion': self.major_engine_version,
            'optionGroupDescription': self.option_group_description,
            'optionGroupName': self.option_group_name,
            'tags': self.tags,
            'available': self.available
        }
