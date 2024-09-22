import datetime
from typing import Optional, Dict
from Service.Classes.DBInstanceNaiveService import DBInstanceService

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

    def delete_db_instance(self,db_instance_identifier,skip_final_snapshot=False,final_db_snapshot_identifier=None,delete_automated_backups=False):
        """
        Delete a DBInstance by its identifier with options to handle final snapshots and automated backups.

        Params: db_instance_identifier: The primary key (ID) of the DBInstance to delete.
                skip_final_snapshot: If True, skip creating a final snapshot before deletion. Defaults to False.
                final_db_snapshot_identifier: If skip_final_snapshot is False, specify an identifier for the final DB snapshot.
                delete_automated_backups: If True, delete automated backups along with the DBInstance. Defaults to False.
        """
        self.service.delete(db_instance_identifier,skip_final_snapshot,final_db_snapshot_identifier,delete_automated_backups)

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
