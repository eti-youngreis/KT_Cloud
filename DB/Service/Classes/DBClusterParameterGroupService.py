from typing import Optional, Dict
from DB.Models.DBClusterParameterGroupModel import DBClusterParameterGroupModel
from DB.Service.Classes.ParameterGroupService import ParameterGroupService
from DataAccess import DBClusterParameterGroupManager
from DB.Validation.Validation import is_valid_user_group_name, is_valid_number

class DBClusterParameterGroupService(ParameterGroupService):
    """
    Service class for managing DBCluster parameter groups.
    """

    def __init__(self, dal: DBClusterParameterGroupManager):
        """
        Initialize the service with a ObjectManager instance.
        """
        self.dal = dal

    def create(self, group_name: str, group_family: str, description: Optional[str] = None):
        """
        Create a new DBCluster parameter group.

        :param group_name: The name of the parameter group.
        :param group_family: The family to which the parameter group belongs.
        :param description: An optional description for the parameter group.
        """

        if not is_valid_user_group_name(group_name):
            raise ValueError(f"group_name {group_name} is not valid")
        if self.dal.is_identifier_exist(group_name):
            raise ValueError(f"UserGroup with NAME '{group_name}' already exists.")
        group = DBClusterParameterGroupModel(group_name, group_family, description)
        self.dal.create(group.to_dict(),group_name)
        return self.describe(group_name)
        # super().create(group_name, group_family, description)
        # print(f"Creating parameter group '{group_name}' in family '{group_family}' with description '{description}'")

    def delete(self, group_name: str):
        """
        Delete an existing DBCluster parameter group.

        :param group_name: The name of the parameter group to delete.
        """
        if group_name == "default":
            raise ValueError("You can't delete a default DB cluster parameter group")
        if not self.dal.is_identifier_exist(group_name):
            raise ValueError(f"DB Cluster Parameter Group '{group_name}' does not exist.")
        self.dal.delete(group_name)

        # super().delete(group_name, 'DBClusterParameterGroup')
        # print(f"Deleting parameter group '{group_name}'")

    def describe(self, group_name: str) -> Dict:
        """
        Describe a specific DBCluster parameter group.

        :param group_name: The name of the parameter group to describe.
        :return: A dictionary containing details about the parameter group.
        """
        data=self.get(group_name)
        describe = {
            'DBClusterParameterGroupName': group_name,
            'DBParameterGroupFamily': data['group_family'],
            'Description': data['description'],
            'DBClusterParameterGroupArn': f'arn:aws:rds:region:account:dbcluster-parameter_group/{self.parameter_group_name}'
        }
        return describe

    def get(self, group_name: str) -> Dict:
        """
        Retrieve the details of a user group.
        
        Args:
            group_name (str): The name of the group to retrieve.
        
        Returns:
            Dict: A dictionary containing the group details.
        
        Raises:
            ValueError: If the group does not exist.
        """
        return self.dal.get(group_name)
        # super().describe(group_name)
        # print(f"Describing parameter group '{group_name}'")
        # return {"GroupName": group_name, "Parameters": {}}

    # def modify(self, group_name: str, updates: Dict[str, Optional[Dict[str, str]]]):
    #     """
    #     Modify an existing DBCluster parameter group.

    #     :param group_name: The name of the parameter group to modify.
    #     :param updates: A dictionary with the updates to apply to the parameter group.
    #     """
    #     print(f"Modifying parameter group '{group_name}' with updates: {updates}")
