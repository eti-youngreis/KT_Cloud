from Service import OptionGroupService
class OptionGroupController:
    def __init__(self, service: OptionGroupService):
        self.service = service

    def create_option_group(self, engine_name: str, major_engine_version: str, option_group_description: str, option_group_name: str, tags: Optional[Dict] = None):
        self.service.create(engine_name, major_engine_version, option_group_description, option_group_name, tags)

    def delete_option_group(self, option_group_name: str):
        self.service.delete(option_group_name)
