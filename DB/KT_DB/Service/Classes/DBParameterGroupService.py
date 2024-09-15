from typing import Optional, Dict
from DB.KT_DB.Models.DBParameterGroupModel import DBParameterGroupModel
from DB.KT_DB.Validation.Validation import is_valid_user_group_name
from DB.KT_DB.Service.Classes.ParameterGroupService import ParameterGroupService
from DataAccess import DBParameterGroupManager, ClusterManager

class DBParameterGroupService(ParameterGroupService):
    """
    Service class for managing DB parameter groups.
    """

    def __init__(self, dal: DBParameterGroupManager, dal_cluster: ClusterManager):
        """
        Initialize the service with a DBParameterGroupManager and ClusterManager instance.

        :param dal: DBParameterGroupManager instance to interact with the database.
        :param dal_cluster: ClusterManager instance to handle cluster-related operations.
        """
        super().__init__(dal, dal_cluster)

    def create(self, group_name: str, group_family: str, description: Optional[str] = None):
        """
        Create a new DB parameter group.

        :param group_name: The name of the parameter group.
        :param group_family: The family to which the parameter group belongs.
        :param description: An optional description for the parameter group.
        :return: A dictionary containing details about the created parameter group.
        """
        super().create(group_name, group_family, description, is_cluster=False)

    def delete(self, group_name: str):
        """
        Delete an existing DB parameter group.

        :param group_name: The name of the parameter group to delete.
        :raises ValueError: If the group_name is 'default' or if the group doesn't exist.
        :raises ValueError: If the group is associated with any DB clusters.
        """
        super().delete(group_name)

    def describe_group(self, parameter_group_name: str = None, max_records: int = 100, marker: str = None):
        """
        Describe DB parameter groups.

        :param parameter_group_name: The name of the specific parameter group to describe. Optional.
        :param max_records: The maximum number of records to return. Defaults to 100.
        :param marker: A marker for pagination to start listing from.
        :return: A dictionary containing the details of the parameter group(s).
        """
        super().describe_group('DBParameterGroups', parameter_group_name, max_records, marker)

    def describe(self, data: Dict):
        """
        Describe a specific DB parameter group.

        :param data: A dictionary containing the details of the parameter group.
        :return: A dictionary with the name, family, description, and ARN of the parameter group.
        """
        return super().describe('DBParameterGroupName', 'DBParameterGroupArn', data)

    def describe_parameters(self, group_name: str, source: str, max_records: int, marker: str) -> Dict:
        """
        Describe the parameters of a specific DB parameter group.

        :param group_name: The name of the parameter group.
        :param source: The source of the parameter (e.g., user-defined, system).
        :param max_records: The maximum number of records to return.
        :param marker: The marker for pagination to start listing from.
        :return: A dictionary containing details about the parameters of the group.
        """
        return super().describe_parameters(group_name, source, max_records, marker)

    def modify(self, group_name: str, parameters: list[Dict[str, any]]):
        """
        Modify an existing DB parameter group.

        :param group_name: The name of the parameter group to modify.
        :param parameters: A list of dictionaries containing the parameter updates.
        :return: A dictionary containing details about the modified parameter group.
        :raises ValueError: If a parameter is not modifiable or the apply method is invalid.
        """
        return super().modify('DBParameterGroupName', group_name, parameters)
