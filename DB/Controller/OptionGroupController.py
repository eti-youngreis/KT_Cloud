from Service import OptionGroupService
class OptionGroupClient:
    def __init__(self, optionGroupService):
        self.service = optionGroupService

    def create_option_group(self, engine_name, major_engine_version, option_group_description, option_group_name, tags = None):
        self.service.create(engine_name, major_engine_version, option_group_description, option_group_name, tags)

    def delete_option_group(self, option_group_name):
        self.service.delete(option_group_name)

    def modify_option_group(self, option_group_name, apply_immediately = False, options_to_include = None, options_to_remove = None):
        self.service.modify(option_group_name, apply_immediately, options_to_include, options_to_remove)

    def copy_option_group(self, source_option_group_identifier, target_option_group_description, target_option_group_identifier, tags = None):
        self.service.copy(source_option_group_identifier, target_option_group_description, target_option_group_identifier, tags)
        
    def describe_option_groups(self, option_group_name = None, engine_name = None, major_engine_version = None, marker = None, max_records = 100):
        self.service.describe(option_group_name, engine_name, major_engine_version, marker, max_records)

    def describe_option_group_options(self, engine_name, major_engine_version = None, marker = None, max_records = 100):
        self.service.describe_by_engine_name(engine_name, major_engine_version, marker, max_records)

