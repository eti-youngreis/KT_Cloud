from typing import Optional, Dict
from DB.KT_DB.Models.DBParameterGroupModel import DBParameterGroupModel
from DB.KT_DB.Validation.Validation import is_valid_user_group_name
from DB.KT_DB.Service.Classes.ParameterGroupService import ParameterGroupService
from DataAccess import DBParameterGroupManager

class DBParameterGroupService(ParameterGroupService):
    """
    Service class for managing DB parameter groups.
    """

    def __init__(self, dal: DBParameterGroupManager):
        """
        Initialize the service with a ObjectManager instance.
        """
        self.dal = dal

    def create(self, group_name: str, group_family: str, description: Optional[str] = None):
        """
        Create a new DB parameter group.

        :param group_name: The name of the parameter group.
        :param group_family: The family to which the parameter group belongs.
        :param description: An optional description for the parameter group.
        """
        super().create(group_name, group_family, description, is_cluster=False)

        # if not is_valid_user_group_name(group_name):
        #     raise ValueError(f"group_name {group_name} is not valid")
        # if self.dal.is_identifier_exist(group_name):
        #     raise ValueError(f"ParameterGroup with NAME '{group_name}' already exists.")
        # group = DBParameterGroupModel(group_name, group_family, description)
        # self.dal.create(group.to_dict(),group_name)
        # return self.describe(group_name)
        # super().create(group_name, group_family, description)
        # print(f"Creating parameter group '{group_name}' in family '{group_family}' with description '{description}'")

    def delete(self, group_name: str):
        """
        Delete an existing DB parameter group.

        :param group_name: The name of the parameter group to delete.
        """
        super().delete(group_name)
        
    def describe_group(parameter_group_name: str =None, max_records: int =100, marker: str=None):
        super().describe_group('DBParameterGroups', parameter_group_name, max_records, marker)

    def describe(self, data: Dict):
        """
        Describe a specific DB parameter group.

        :param group_name: The name of the parameter group to describe.
        :return: A dictionary containing details about the parameter group.
        """
        super().describe('DBParameterGroupName', 'DBParameterGroupArn', data)
        # print(f"Describing parameter group '{group_name}'")
        # return {"GroupName": group_name, "Parameters": {}}

    # def modify(self, group_name: str, updates: Dict[str, Optional[Dict[str, str]]]):
    #     """
    #     Modify an existing DB parameter group.

    #     :param group_name: The name of the parameter group to modify.
    #     :param updates: A dictionary with the updates to apply to the parameter group.
    #     """
    #     print(f"Modifying parameter group '{group_name}' with updates: {updates}")
