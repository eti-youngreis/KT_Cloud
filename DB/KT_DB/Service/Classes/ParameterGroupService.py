from abc import abstractmethod
from typing import Optional, Dict
# from DataAccess import ObjectManager
from Abc import DBO
from DB.KT_DB.Models.ParameterGroupModel import ParameterGroupModel
from DB.KT_DB.Validation.Validation import is_valid_user_group_name, is_valid
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
        :param dal_cluster: ClusterManager instance to handle cluster-related operations.
        """
        self.dal = dal
        self.dal_cluster = dal_cluster

    def create(self, group_name: str, group_family: str, description: Optional[str] = None, is_cluster: bool = True):
        """
        Create a new parameter group.

        :param group_name: The name of the parameter group.
        :param group_family: The family to which the parameter group belongs.
        :param description: An optional description for the parameter group.
        :param is_cluster: Indicates if the group is a DBCluster parameter group. Defaults to True.
        :return: A dictionary containing details about the created parameter group.
        """
        if not is_valid_user_group_name(group_name):
            raise ValueError(f"group_name {group_name} is not valid")
        if self.dal.is_identifier_exist(group_name):
            raise ValueError(f"ParameterGroup with NAME '{group_name}' already exists.")
        if is_cluster:
            group = DBClusterParameterGroupModel(group_name, group_family, description)
        else:
            group = ParameterGroupModel(group_name, group_family, description)
        self.dal.create(group.to_dict(), group_name)
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
        data = self.dal_cluster.get_all_objects()
        clusters = list(data.values())
        for c in clusters:
            if c['db_cluster_parameter_group_name'] == group_name:
                raise ValueError("Can't delete parameter group associated with any DB clusters")
        self.dal.delete(group_name)
        print(f"Deleting parameter group '{group_name}'")

    def describe_group(self, title: str, parameter_group_name: str = None, max_records: int = 100, marker: str = None) -> Dict:
        """
        Describe a specific parameter group.

        :param title: The title for the output data.
        :param parameter_group_name: The name of the parameter group to describe. Optional.
        :param max_records: The maximum number of records to return.
        :param marker: The marker to start listing from. Used for pagination.
        :return: A dictionary containing details about the parameter group(s).
        """
        parameter_groups_local = []
        if parameter_group_name is not None:
            data = self.dal.get(parameter_group_name)
            parameter_groups_local.append(self.describe(data))
        else:
            result = self.dal.get_all_groups()
            parameter_groups = list(result.values())
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

    def describe_parameters(self, group_name: str, source: str, max_records: int, marker: str) -> Dict:
        """
        Describe the parameters of a specific parameter group.

        :param group_name: The name of the parameter group to describe.
        :param source: The source of the parameter (e.g., user-defined, system).
        :param max_records: The maximum number of records to return.
        :param marker: The marker to start listing from. Used for pagination.
        :return: A dictionary containing details about the parameters of the group.
        """
        data = self.get(group_name)
        parameters_local = []
        count = 0
        for p in data['parameters']:
            if p.parameter_name == marker or marker is None:
                marker = None
                if p.source == source:
                    count += 1
                    if count <= max_records:
                        parameters_local.append(p.describe())
                    else:
                        marker = p.parameter_name
                        break

        result = {'Parameters': parameters_local}
        if marker is not None:
            result['Marker'] = marker
        return result

    def camel_to_snake_case(name: str) -> str:
        """
        Convert a CamelCase string to snake_case.

        :param name: The CamelCase string to convert.
        :return: The snake_case version of the input string.
        """
        return ''.join(['_' + c.lower() if c.isupper() else c for c in name]).lstrip('_')

    def convert_dict_keys_to_snake_case(self, input_dict: Dict) -> Dict:
        """
        Convert all keys in a dictionary from CamelCase to snake_case.

        :param input_dict: The input dictionary with CamelCase keys.
        :return: A new dictionary with snake_case keys.
        """
        return {self.camel_to_snake_case(key): value for key, value in input_dict.items()}

    def modify(self, title: str, group_name: str, parameters: Optional[list[Dict[str, any]]] = None):
        """
        Modify an existing parameter group.

        :param title: The title for the output data.
        :param group_name: The name of the parameter group to modify.
        :param parameters: A list of dictionaries with updates to apply to the parameter group.
        :return: A dictionary containing details about the modified parameter group.
        """
        update = []
        parameter_group = self.dal.get(group_name)
        for new_parameter in parameters:
            is_valid(new_parameter['IsModifiable'], [True, False], 'IsModifiable')
            is_valid(new_parameter['ApplyMethod'], ['immediate', 'pending-reboot'], 'ApplyMethod')
            for old_parameter in parameter_group['parameters']:
                if new_parameter['ParameterName'] == old_parameter['parameter_name']:
                    if old_parameter['IsModifiable'] == False:
                        raise ValueError(f"You can't modify the parameter {old_parameter['parameter_name']}")
                    new_parameter_updates = self.convert_dict_keys_to_snake_case(new_parameter)
                    update.append(new_parameter_updates)
                else:
                    update.append(old_parameter)
        new_data = {'parameters': update}
        updated_data = {**parameter_group, **new_data}
        self.dal.update(group_name, updated_data)
        return {title: group_name}

    @abstractmethod
    def describe(self, name: str, arn: str, data: Dict) -> Dict:
        """
        Abstract method to describe a parameter group.

        :param name: The name of the parameter group.
        :param arn: The Amazon Resource Name (ARN) for the parameter group.
        :param data: The data for the parameter group.
        :return: A dictionary containing the description of the parameter group.
        """
        describe = {
            name: data['group_name'],
            'DBParameterGroupFamily': data['group_family'],
            'Description': data['description'],
            arn: f'arn:aws:rds:region:account:dbcluster-parameter_group/{data["group_name"]}'
        }
        return describe
