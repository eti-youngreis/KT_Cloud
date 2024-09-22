from abc import abstractmethod
import json
import os
import sys
from typing import Optional, Dict
from NEW_KT_DB.Service.Abc.DBO import DBO
from NEW_KT_DB.Validation.GeneralValidations import is_valid_user_group_name, is_valid
from NEW_KT_DB.Models.DBClusterParameterGroupModel import DBClusterParameterGroup
from NEW_KT_DB.DataAccess import DBClusterManager#, DBClusterParameterGroupManager
from NEW_KT_DB.DataAccess import DBClusterParameterGroupManager
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager

class DBClusterParameterGroupService(DBO):
    """
    Service class for managing generic parameter groups.
    """
    column_index_mapping = {
        'group_name': 0,
        'group_family': 1,
        'description': 2,
        'parameters': 3
    }

    def __init__(self, dal:DBClusterParameterGroupManager, dal_cluster: DBClusterManager, storage_manager: StorageManager):
        """
        Initialize the service with a ObjectManager instance.

        :param dal:  instance to interact with the database.
        :param dal_cluster: ClusterManager instance to handle cluster-related operations.
        """
        self.dal = dal
        self.dal_cluster = dal_cluster
        self.storage_manager=storage_manager

    def create(self, group_name: str, group_family: str, description: Optional[str] = None):
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
        group = DBClusterParameterGroup(group_name, group_family, description)
        parameter_group_dict=group.to_dict()
        data_tuple = (
        parameter_group_dict['group_name'],
        parameter_group_dict['group_family'],
        parameter_group_dict.get('description', None),
        json.dumps(parameter_group_dict['parameters']) 
    )
        self.dal.createInMemoryDBCluster(data_tuple)
        file_name=f'db_cluster_parameter_groups/db_cluster_parameter_group_{group_name}.json'
        self.storage_manager.create_directory('db_cluster_parameter_groups')
        self.storage_manager.create_file(file_name, json.dumps(parameter_group_dict))
        print(f"Creating parameter group '{group_name}' in family '{group_family}' with description '{description}'")
        group_tuple=self.get(group_name)
        return self.describe(group_tuple)

    def delete(self, group_name: str):
        """
        Delete an existing parameter group.

        :param group_name: The name of the parameter group to delete.
        :param class_name: The class name of the parameter group.
        """
        if group_name == "default":
            raise ValueError("You can't delete a default parameter group")
        if not self.dal.is_identifier_exist(group_name):
            raise ValueError(f"Parameter Group '{group_name}' does not exist.")
        clusters = self.dal_cluster.get_all_clusters()
        for c in clusters:
            if c[6] == group_name:
                raise ValueError("Can't delete parameter group associated with any DB clusters")
        self.dal.deleteInMemoryDBCluster(group_name)
        file_name = f'db_cluster_parameter_groups/db_cluster_parameter_group_{group_name}.json'
        self.storage_manager.delete_file(file_name)
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
            data = self.get(parameter_group_name)
            parameter_groups_local.append(self.describe(data))
        else:
            parameter_groups = self.dal.get_all_groups()
            count = 0
            for p in parameter_groups:
                if p[DBClusterParameterGroupService.column_index_mapping['group_name']] == marker or marker is None:
                    marker = None
                    count += 1
                    if count <= max_records:
                        parameter_groups_local.append(self.describe(p))
                    else:
                        marker = p[DBClusterParameterGroupService.column_index_mapping['group_name']]
        if marker is None: 
            return {title: parameter_groups_local}
        return {'Marker': marker, title: parameter_groups_local}

    def convert_camel_case_string_to_snake(self, name: str) -> str:
        """
        Convert a CamelCase string to snake_case.

        :param name: The CamelCase string to convert.
        :return: The snake_case version of the input string.
        """
        return ''.join(['_' + c.lower() if c.isupper() else c for c in name]).lstrip('_')

    def convert_dict_keys_from_camel_case_to_snake(self, input_dict: Dict) -> Dict:
        """
        Convert all keys in a dictionary from CamelCase to snake_case.

        :param input_dict: The input dictionary with CamelCase keys.
        :return: A new dictionary with snake_case keys.
        """
        return {self.convert_camel_case_string_to_snake(key): value for key, value in input_dict.items()}

    def modify(self, title: str, group_name: str, parameters: Optional[list[Dict[str, any]]] = None):
        """
        Modify an existing parameter group.

        :param title: The title for the output data.
        :param group_name: The name of the parameter group to modify.
        :param parameters: A list of dictionaries with updates to apply to the parameter group.
        :return: A dictionary containing details about the modified parameter group.
        """
        parameter_group = self.get(group_name)
        parameters_in_parameter_group=parameter_group[DBClusterParameterGroupService.column_index_mapping['parameters']]
        parameters_in_parameter_group=json.loads(parameters_in_parameter_group)
        for new_parameter in parameters:
            is_valid(new_parameter['IsModifiable'], [True, False], 'IsModifiable')
            is_valid(new_parameter['ApplyMethod'], ['immediate', 'pending-reboot'], 'ApplyMethod')
            for idx, old_parameter in enumerate(parameters_in_parameter_group):
                if new_parameter['ParameterName'] == old_parameter['parameter_name']:
                    if old_parameter['is_modifiable'] == False:
                        raise ValueError(f"You can't modify the parameter {old_parameter['parameter_name']}")
                    new_parameter_updates = self.convert_dict_keys_from_camel_case_to_snake(new_parameter)
                    updated_parameter = {**old_parameter, **new_parameter_updates}
                    parameters_in_parameter_group[idx] = updated_parameter
        self.dal.modifyDBCluster(group_name, f"parameters='{json.dumps(parameters_in_parameter_group)}'")
        file_name=f'db_cluster_parameter_groups/db_cluster_parameter_group_{group_name}.json' 
        group_family=parameter_group[DBClusterParameterGroupService.column_index_mapping['group_family']]
        description=parameter_group[DBClusterParameterGroupService.column_index_mapping['description']]
        group = DBClusterParameterGroup(group_name, group_family, description)
        parameter_group_dict=group.to_dict()
        parameter_group_dict['parameters']=parameters_in_parameter_group
        self.storage_manager.write_to_file(file_name, json.dumps(parameter_group_dict))
        return {title: group_name}

    def describe(self, data: tuple) -> Dict:
        """
        Abstract method to describe a parameter group.

        :param name: The name of the parameter group.
        :param arn: The Amazon Resource Name (ARN) for the parameter group.
        :param data: The data for the parameter group.
        :return: A dictionary containing the description of the parameter group.
        """
        describe = {
            'DBClusterParameterGroupName': data[DBClusterParameterGroupService.column_index_mapping['group_name']],
            'DBParameterGroupFamily': data[DBClusterParameterGroupService.column_index_mapping['group_family']],
            'Description': data[DBClusterParameterGroupService.column_index_mapping['description']],
            'DBClusterParameterGroupArn': f'arn:aws:rds:region:account:dbcluster-parameter_group/{data[DBClusterParameterGroupService.column_index_mapping["group_name"]]}'
        }
        return describe

    def get(self, group_name: str) -> Dict:
        """
        Retrieve a parameter group by its name.
    
        :param group_name: The name of the parameter group to retrieve.
        :return: A dictionary representing the parameter group.
        :raises ValueError: If the parameter group does not exist.
        
        This method queries the data access layer (DAL) to retrieve the parameter group with the specified name.
        If no parameter group is found, it raises a ValueError indicating that the parameter group does not exist.
        Otherwise, it returns the first result as a dictionary.
        """
        result = self.dal.get(group_name)
        if result == []:
            raise ValueError(f"Parameter Group '{group_name}' does not exist.")
        return result[0]