import datetime
from typing import Optional, Dict
from Service.Classes.DBInstanceService import DBInstanceService

class DBInstanceController:

    def __init__(self, service: DBInstanceService):
        self.service = service

    def create_db_instance(self, **kwargs):
        """
        Create a new DBInstance by passing the necessary attributes to the service.

        Params: kwargs: The required and optional attributes for creating a DBInstance.
        Return: A dictionary containing the newly created DBInstance.
        """
        return self.service.create(**kwargs)

    def delete_db_instance(self, db_instance_identifier: str):
        """
        Delete a DBInstance by its identifier.

        Params: db_instance_identifier: The primary key (ID) of the DBInstance to delete.
        """
        self.service.delete(db_instance_identifier)

    def modify_db_instance(self, **kwargs):
        """
        Modify an existing DBInstance by passing updated attributes.

        Params: kwargs: The attributes to modify for the DBInstance.
        Return: A dictionary containing the modified DBInstance.
        """
        return self.service.modify(**kwargs)

    def describe_db_instance(self, db_instance_identifier: str):
        """
        Retrieve details of a DBInstance by its identifier.

        Params: db_instance_identifier: The primary key (ID) of the DBInstance to describe.
        Return: A dictionary containing the details of the DBInstance.
        """
        return self.service.describe(db_instance_identifier)
