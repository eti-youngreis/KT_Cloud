from DataAccess import OptionGroupManager
from Models import OptionGroupModel
from Abc import DBO
from Validation import Validation 
from typing import Dict, List
from sqlite3 import OperationalError

class OptionGroupService(DBO):
    def __init__(self, dal: OptionGroupManager):
        self.dal = dal
        self.table_managment = 'option_group'

    def is_valid_engine_name(self, engine_name: str) -> bool:
        '''Check if the engineName is valid.'''
        valid_engine_names = {
            'db2-ae', 'db2-se', 'mariadb', 'mysql', 'oracle-ee',
            'oracle-ee-cdb', 'oracle-se2', 'oracle-se2-cdb',
            'postgres', 'sqlserver-ee', 'sqlserver-se',
            'sqlserver-ex', 'sqlserver-web'
        }

        return Validation.string_in_dict(engine_name, valid_engine_names)

    def is_valid_optionGroupName(self, option_group_name: str) -> bool:
        '''
        Check if the optionGroupName is valid based on the pattern and length.

        The optionGroupName must:
        - Be between 1 and 255 characters in length.
        - Start with a letter.
        - Contain only letters, digits, or hyphens.
        - Not contain two consecutive hyphens.
        - End with a letter or digit.

        Args:
            option_group_name (str): The name of the option group to validate.

        Returns:
            bool: True if the optionGroupName is valid, False otherwise.
        '''
        pattern = r'^[a-zA-Z](?!.*--)[a-zA-Z0-9-]{0,253}[a-zA-Z0-9]$'
        return Validation.is_length_in_range(option_group_name, 1, 255) and Validation.is_string_matches_regex(option_group_name, pattern)

    def is_option_group_exists(self, option_group_name: str) -> bool:
        '''Check if an option group with the given name already exists.'''
        return self.dal.is_option_group_exists(option_group_name)

    def get_option_group_count(self) -> int:
        return self.dal.get_option_group_count()

    def create(self, engine_name, major_engine_version, option_group_description, option_group_name, tags):
        '''Create a new option group and insert it into the object_management table.'''
        if not self.is_valid_engine_name(engine_name):
            raise ValueError(f'Invalid engineName: {engine_name}')

        if not self.is_valid_optionGroupName(option_group_name):
            raise ValueError(f'Invalid optionGroupName: {option_group_name}')
        
        if self.is_option_group_exists(option_group_name):
            raise ValueError(f'Option group \'{option_group_name}\' already exists.')

        if self.get_option_group_count() >= 20:
            raise ValueError(f'Quota of 20 option groups exceeded for this AWS account.')
        new_option_group = OptionGroupModel(engine_name, major_engine_version, option_group_description, option_group_name, tags)

        try:
            self.dal.create(new_option_group.to_dict())
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}')

    def delete(self, option_group_name):
        '''Delete an existing Option Group.'''
        pass

    def modify(self, option_group_name, apply_immediately, options_to_include, options_to_remove):
        '''Modify an existing Option Group.'''
        pass

    def copy(self, source_option_group_identifier, target_option_group_description, target_option_group_identifier, tags):
        '''Copy an Option Group.'''
        pass

    def describe(self, option_group_name, engine_name, major_engine_version, marker, max_records):
        '''Retrieve the details of an Option Group or all Option Groups.'''
        pass

    def describe_by_engine_name(engine_name, major_engine_version, marker, max_records):
        '''Retrieve the details of an Option Group.'''
        pass
