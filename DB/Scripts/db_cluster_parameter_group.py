from DB.Scripts.parameter import Parameter
from DB.Scripts import DBParameterGroup


class DBClusterParameterGroup(DBParameterGroup):
    def __init__(self, db_cluster_parameter_group_name, db_parameter_group_family, description, tags):
        """
        Initializes a DBClusterParameterGroup instance.

        Args:
        db_cluster_parameter_group_name (str): The name of the DB cluster parameter group
        db_parameter_group_family (str): The family of the parameter group
        description (str): Description of the parameter group
        tags (list): Tags associated with the parameter group
        """
        super(DBParameterGroup, self).__init__(db_cluster_parameter_group_name, db_parameter_group_family, description,
                                               tags)

    def describe(self, with_title=True):
        """
        Provides a description of the DB cluster parameter group.

        Args:
        with_title (bool): Whether to include the class name in the description (default: True)

        Returns:
        dict: A dictionary containing the description of the parameter group
        """
        return super().describe(with_title=False)

    def save_to_db(self, conn=None):
        """
        Saves the DB cluster parameter group to the database.

        Calls the parent class's save_to_db method with the class name.
        """
        super().save_to_db(conn)

    def delete(self):
        super().delete()

    @staticmethod
    def load_default_parameters():
        """
        Loads default parameters for the DB cluster parameter group.

        Returns:
        list: A dictionary containing default parameters
        """
        # Example of default parameters; can be replaced with actual defaults
        parameters = [Parameter('backup_retention_period', 7),
                      Parameter('preferred_backup_window', '03:00-03:30'),
                      Parameter('preferred_maintenance_window', 'Mon:00:00-Mon:00:30')]
        return parameters
