from DataAccess import DataAccessLayer
from Models import OptionGroupModel
from Abc import DBO
from Validation import is_valid_engineName, is_valid_optionGroupName

class OptionGroupService(DBO):
    def __init__(self, dal: DataAccessLayer):
        self.dal = dal

    def create(self, engine_name, major_engine_version, option_group_description, option_group_name, tags):
        """Create a new Option Group."""
        
    def delete(self, option_group_name):
        """Delete an existing Option Group."""

    def modify(self, option_group_name, apply_immediately, options_to_include, options_to_remove):
        """Modify an existing Option Group."""

    def copy(self, source_option_group_identifier, target_option_group_description, target_option_group_identifier, tags):
        """Copy an Option Group."""

    def describe(self, option_group_name, engine_name, major_engine_version, marker, max_records):
        """Retrieve the details of an Option Group or all Option Groups."""

    def describe_by_engine_name(engine_name, major_engine_version, marker, max_records):
        """Retrieve the details of an Option Group."""

