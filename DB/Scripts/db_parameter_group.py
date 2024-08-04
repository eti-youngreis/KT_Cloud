from parameter_group import ParameterGroup


class DBParameterGroup(ParameterGroup):
    def __init__(self, db_cluster_parameter_group_name, db_parameter_group_family, description, tags):
        """
        Initializes a DBParameterGroup instance.

        Args:
        db_cluster_parameter_group_name (str): The name of the DB parameter group
        db_parameter_group_family (str): The family of the DB parameter group
        description (str): Description of the DB parameter group
        tags (list): List of tags associated with the DB parameter group
        """
        super().__init__(db_cluster_parameter_group_name, db_parameter_group_family, description, tags,
                         self.load_default_parameters())

    def describe(self, with_title=True):
        """
        Describes the DB parameter group.

        Args:
        with_title (bool, optional): Whether to include the class name in the description (default: True)

        Returns:
        dict: Description of the DB parameter group, with or without class name as title
        """
        if with_title:
            return {self.__class__.__name__: super().describe('DBParameterGroupName')}
        return super().describe('DBParameterGroupName')

    def save_to_db(self):
        """
        Saves the DB parameter group to the database.
        """
        super().save_to_db(self.__class__.__name__)

    @staticmethod
    def load_default_parameters():
        """
        Loads default parameters for the DB parameter group.

        Returns:
        dict: Default parameters for the DB parameter group
        """
        # Loading default parameters - can be replaced with actual parameters
        return {
            'max_connections': 100,
            'innodb_buffer_pool_size': '128M',
            'character_set_server': 'utf8',
            'time_zone': 'UTC'
        }
