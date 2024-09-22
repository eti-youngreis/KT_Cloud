from DB.NEW_KT_DB.Service.Classes.DBProxyService import DBProxyService


class DBProxyController:
    def __init__(self, service: DBProxyService) -> None:
        """
        Initialize the DBProxyController.

        Parameters:
        service (DBProxyService): An instance of DBProxyService for managing DBProxy operations.
        """
        self.service = service

    def create_db_proxy(self, **kwargs):
        """
        Create a new DBProxy.

        Parameters:
        **kwargs: Arbitrary keyword arguments for DBProxy creation.
        """
        self.service.create(**kwargs)

    def delete_db_proxy(self, id):
        """
        Delete a DBProxy by its ID.

        Parameters:
        id (str): The ID of the DBProxy to delete.
        """
        self.service.delete(id)

    def modify_db_proxy(self, **kwargs):
        """
        Modify an existing DBProxy.

        Parameters:
        **kwargs: Arbitrary keyword arguments for DBProxy modification.
        """
        self.service.modify(**kwargs)

    def describe_db_proxy(self, id):
        """
        Retrieve details of a DBProxy by its ID.

        Parameters:
        id (str): The ID of the DBProxy to describe.

        Returns:
        dict: Metadata of the requested DBProxy.
        """
        return self.service.describe(id)

