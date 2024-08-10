from typing import Dict, Optional
from DataAccess import DataAccessLayer
from Models import OptionGroupModel
from Abc import DBO
from Validation import is_valid_engineName, is_valid_optionGroupName

class OptionGroupService(DBO):
    def __init__(self, dal: DataAccessLayer):
        self.dal = dal

    def create(self, engine_name: str, major_engine_version: str, option_group_description: str, option_group_name: str, tags: Optional[Dict] = None):
        """Create a new Option Group."""
        if not is_valid_engineName(engine_name):
            raise ValueError(f"Invalid engineName: {engine_name}")
        if not is_valid_optionGroupName(option_group_name):
            raise ValueError(f"Invalid optionGroupName: {option_group_name}")

        option_group = OptionGroupModel(engine_name, major_engine_version, option_group_description, option_group_name, tags)
        self.dal.insert('OptionGroup', option_group.to_dict())

    def delete(self, option_group_name: str):
        """Delete an existing Option Group."""
        if not self.dal.exists('OptionGroup', option_group_name):
            raise ValueError(f"Option Group '{option_group_name}' does not exist.")
        self.dal.delete('OptionGroup', option_group_name)

    def describe(self, option_group_name: str) -> Dict:
        """Retrieve the details of an Option Group."""
        data = self.dal.select('OptionGroup', option_group_name)
        if data is None:
            raise ValueError(f"Option Group '{option_group_name}' does not exist.")
        return data

    def modify(self, option_group_name: str, updates: Dict):
        """Modify an existing Option Group."""
        if not self.dal.exists('OptionGroup', option_group_name):
            raise ValueError(f"Option Group '{option_group_name}' does not exist.")
        
        current_data = self.dal.select('OptionGroup', option_group_name)
        if current_data is None:
            raise ValueError(f"Option Group '{option_group_name}' does not exist.")
        
        updated_data = {**current_data, **updates}
        self.dal.update('OptionGroup', option_group_name, updated_data)
