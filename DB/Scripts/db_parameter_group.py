import json

from DB.Scripts.parameter import Parameter
from DB.Scripts.Management import insert_into_management_table, delete_from_management


class DBParameterGroup():
    def __init__(self, db_cluster_parameter_group_name, db_parameter_group_family, description, tags):
        """
        Initializes a DBParameterGroup instance.

        Args:
        db_cluster_parameter_group_name (str): The name of the DB parameter group
        db_parameter_group_family (str): The family of the DB parameter group
        description (str): Description of the DB parameter group
        tags (list): List of tags associated with the DB parameter group
        """
        self.parameter_group_name = db_cluster_parameter_group_name
        self.db_parameter_group_family = db_parameter_group_family
        self.description = description
        self.tags = tags
        self.parameters = self.load_default_parameters()

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

    def save_to_db(self, conn=None):
        """
        Saves the parameter group to the database.

        Args:
        class_name (str): The name of the class for saving to the management table
        """
        metadata_json = self.get_metadata()
        insert_into_management_table(
            self.__class__.__name__, self.parameter_group_name, metadata_json, conn)

    def describe(self, with_title=True):
        """
        Describes the DB parameter group.

        Args:
        with_title (bool, optional): Whether to include the class name in the description (default: True)

        Returns:
        dict: Description of the DB parameter group, with or without class name as title
        """

        data = {
            "name": self.parameter_group_name,
            'DBParameterGroupFamily': self.db_parameter_group_family,
            'Description': self.description,
            'DBClusterParameterGroupArn': f'arn:aws:rds:region:account:dbcluster-parameter_group/{self.parameter_group_name}'
        }
        if with_title:
            return {self.__class__.__name__: data}
        return data

    def delete(self):
        delete_from_management(self.__class__.__name__,
                               self.parameter_group_name)

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
        if DBParameterGroup.default_parameter_group is None:
            DBParameterGroup.default_parameter_group = DBParameterGroup(
                "default", "aurora-mysql5.7", "default", None)
        return DBParameterGroup.default_parameter_group

    @staticmethod
    def load_default_parameters():
        """
        Loads default parameters for the DB parameter group.

        Returns:
        list: Default parameters for the DB parameter group
        """
        # Loading default parameters - can be replaced with actual parameters
        parameters = [Parameter('max_connections', 100),
                      Parameter('innodb_buffer_pool_size', '128M'),
                      Parameter('character_set_server', 'utf8'),
                      Parameter('time_zone', 'UTC')]
        return parameters
