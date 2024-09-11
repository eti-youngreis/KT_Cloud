from abc import abstractmethod
from typing import Optional, Dict
# from DataAccess import ObjectManager
from Abc import DBO
from DB.KT_DB.Models.ParameterGroupModel import ParameterGroupModel
from DB.KT_DB.Validation.Validation import is_valid_user_group_name
from KT_DB.Models.DBClusterParameterGroupModel import DBClusterParameterGroupModel
from DataAccess import ClusterManager


class ParameterGroupService(DBO):
    """
    Service class for managing generic parameter groups.
    """

    def __init__(self, dal, dal_cluster: ClusterManager):
        """
        Initialize the service with a ObjectManager instance.

        :param dal:  instance to interact with the database.
        """
        self.dal = dal
        self.dal_cluster=dal_cluster

    def create(self, group_name: str, group_family: str, description: Optional[str] = None, is_cluster: bool=True):
        """
        Create a new parameter group.

        :param group_name: The name of the parameter group.
        :param group_family: The family to which the parameter group belongs.
        :param description: An optional description for the parameter group.
        """
        if not is_valid_user_group_name(group_name):
            raise ValueError(f"group_name {group_name} is not valid")
        if self.dal.is_identifier_exist(group_name):
            raise ValueError(f"ParameterGroup with NAME '{group_name}' already exists.")
        if is_cluster:
            group = DBClusterParameterGroupModel(group_name, group_family, description)
        else:
             group = ParameterGroupModel(group_name, group_family, description)
        self.dal.create(group.to_dict(),group_name)
        print(f"Creating parameter group '{group_name}' in family '{group_family}' with description '{description}'")
        return self.describe(group_name)
        

    def delete(self, group_name: str, class_name: str):
        """
        Delete an existing parameter group.

        :param group_name: The name of the parameter group to delete.
        :param class_name: The class name of the parameter group.
        """
        if group_name == "default":
            raise ValueError("You can't delete a default parameter group")
        if not self.dal.is_identifier_exist(group_name):
            raise ValueError(f"Parameter Group '{group_name}' does not exist.")
        data=self.dal_cluster.get_all_objects()
        clusters=list(data.values())
        for c in clusters:
            if c['db_cluster_parameter_group_name']==group_name:
                raise ValueError("Can't be associated with any DB clusters")
        self.dal.delete(group_name)
        print(f"Deleting parameter group '{group_name}'")

    def describe_group(self, title: str, parameter_group_name: str =None, max_records: int =100, marker: str=None) -> Dict:
        """
        Describe a specific parameter group.

        :param group_name: The name of the parameter group to describe.
        :param class_name: The class name of the parameter group.
        :return: A dictionary containing details about the parameter group.
        """
       
        parameter_groups_local = []
        if parameter_group_name is not None:
            data=self.dal.get(parameter_group_name)            
            parameter_groups_local.append(self.describe(data))
        else:
            result=self.dal.get_all_groups()
            parameter_groups=list(result.values())
            count = 0
            for p in parameter_groups:
                if p['group_name'] == marker or marker is None:
                    marker = None
                    count += 1
                    if count <= max_records:
                        parameter_groups_local.append(self.describe(p))
                    else:
                        marker = p['group_name']
        if marker is None:
            return {title: parameter_groups_local}
        return {'Marker': marker, title: parameter_groups_local}


    

        # print(f"Describing parameter group '{group_name}'")
        # return {"GroupName": group_name, "Parameters": {}}

    def modify(self, group_name: str, updates: Dict[str, Optional[Dict[str, str]]]):
        """
        Modify an existing parameter group.

        :param group_name: The name of the parameter group to modify.
        :param updates: A dictionary with the updates to apply to the parameter group.
        """
        print(f"Modifying parameter group '{group_name}' with updates: {updates}")

    # @abstractmethod
    def describe(self, name: str, arn: str, data: Dict) -> Dict:
        # data=self.get(group_name)
        describe = {
            name: data['group_name'],
            'DBParameterGroupFamily': data['group_family'],
            'Description': data['description'],
            arn: f'arn:aws:rds:region:account:dbcluster-parameter_group/{data["group_name"]}'
        }
        return describe