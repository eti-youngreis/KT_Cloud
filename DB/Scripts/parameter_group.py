import json

from DB.Scripts.Management import insert_into_management_table, delete_from_Management
from abc import abstractmethod


class ParameterGroup:
    default_parameter_group = None

    def __init__(self, db_cluster_parameter_group_name, db_parameter_group_family, description, tags):
        """
        Initializes a ParameterGroup instance.

        Args:
        db_cluster_parameter_group_name (str): The name of the parameter group
        db_parameter_group_family (str): The family of the parameter group
        description (str): Description of the parameter group
        tags (list): Tags associated with the parameter group
        # parameters (dict, optional): Parameters for the parameter group (default: None)
        """
        self.parameter_group_name = db_cluster_parameter_group_name
        self.db_parameter_group_family = db_parameter_group_family
        self.description = description
        self.tags = tags
        self.parameters = self.load_default_parameters()

    @abstractmethod
    def load_default_parameters(self):
        pass

    def get_metadata(self):
        """
        Retrieves metadata of the parameter group as a JSON string.

        Returns:
        str: JSON string representing the parameter group's metadata
        """
        data = {
            'parameter_group_name': self.parameter_group_name,
            'db_parameter_group_family': self.db_parameter_group_family,
            'description': self.description,
            'tags': self.tags,
            'parameters': self.parameters
        }
        metadata = {k: v for k, v in data.items() if v is not None}
        metadata_json = json.dumps(metadata)
        return metadata_json

    def save_to_db(self, class_name,conn=None):
        """
        Saves the parameter group to the database.

        Args:
        class_name (str): The name of the class for saving to the management table
        """
        metadata_json = self.get_metadata()
        insert_into_management_table(class_name, self.parameter_group_name, metadata_json,conn)

    def describe(self, name):
        """
        Describes the parameter group with details.

        Args:
        name (str): The name to be used in the description

        Returns:
        dict: Description of the parameter group
        """
        data = {
            name: self.parameter_group_name,
            'DBParameterGroupFamily': self.db_parameter_group_family,
            'Description': self.description,
            'DBClusterParameterGroupArn': f'arn:aws:rds:region:account:dbcluster-parameter_group/{self.parameter_group_name}'
        }
        return data

    def delete(self, class_name):
        """
        Deletes the parameter group from the management table.
        """
        delete_from_Management(class_name, self.parameter_group_name)

    def modify_parameters(self, parameters):
        for new_parameter in parameters:
            for old_parameter in self.parameters:
                if old_parameter.parameter_name == new_parameter['ParameterName']:
                    old_parameter.update(new_parameter)


    @staticmethod
    def create_parameter_group(module_name, class_name, db_cluster_parameter_group_name, db_parameter_group_family,
                               description, tags):
        """
        Creates an instance of a parameter group by dynamically importing the module and class.

        Args:
        module_name (str): The module name where the class is defined
        class_name (str): The class name to instantiate
        db_cluster_parameter_group_name (str): The name of the parameter group
        db_parameter_group_family (str): The family of the parameter group
        description (str): Description of the parameter group
        tags (list): Tags associated with the parameter group

        Returns:
        ParameterGroup: An instance of the specified parameter group class
        """
        module = __import__(module_name, fromlist=[class_name])
        cls = getattr(module, class_name)
        return cls(db_cluster_parameter_group_name, db_parameter_group_family, description, tags)

    @staticmethod
    def build_default_parameter_group():
        """
        Builds and returns the default parameter group if it does not exist.

        Returns:
        ParameterGroup: The default parameter group instance
        """
        if ParameterGroup.default_parameter_group is None:
            ParameterGroup.default_parameter_group = ParameterGroup("default", "aurora-mysql5.7", "default", None)
        return ParameterGroup.default_parameter_group
