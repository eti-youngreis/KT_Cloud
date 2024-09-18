import os
import shutil
import sys
from typing import Dict, Optional
from Exception.exception import DBInstanceNotFoundError, ParamValidationError
from Validation.DBInstanceValidition import check_extra_params, check_required_params, is_valid_db_instance_identifier
from Models.DBInstanceModel import DBInstance
from Service.Abc.DBO import DBO
from DataAccess.DBInstanceManager import DBInstanceManager
from Exception.exception import AlreadyExistsError
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager


class DBInstanceService(DBO):
    
    def __init__(self, dal: DBInstanceManager):
        self.dal = dal

    def create(self, **attributes):
        """
        Create a new DBInstance.

        Params: attributes: dict containing attributes for the new DBInstance. 
                           Required fields: 'db_instance_identifier', 'master_username', 'master_user_password'
                           Optional fields: 'db_name', 'port', 'allocated_storage'
        
        Raises: ValueError: if the db_instance_identifier is invalid.
                AlreadyExistsError: if a DB instance with the given identifier already exists.

        Return: dict with a 'DBInstance' key containing the created instance's details.
        """
        required_params = ['db_instance_identifier', 'master_username', 'master_user_password']
        all_params = ['db_name', 'port', 'allocated_storage']
        all_params.extend(required_params)
        
        # Validate required and extra parameters
        check_required_params(required_params, attributes)
        check_extra_params(all_params, attributes)
        
        db_instance_identifier = attributes['db_instance_identifier']
        
        # Validate DB instance identifier format
        if not is_valid_db_instance_identifier(db_instance_identifier, 63):
            raise ValueError('db_instance_identifier is invalid')
        
        if self.dal.is_db_instance_exist(db_instance_identifier):
            raise AlreadyExistsError(f"The ID {db_instance_identifier} already exists")
        
        db_instance = DBInstance(**attributes)
        self.dal.createInMemoryDBInstance(db_instance)
        
        return {'DBInstance': db_instance.to_dict()}
        
    def delete(self, db_instance_identifier,skip_final_snapshot=False,final_db_snapshot_identifier=None,delete_automated_backups=False):
        """
        Delete a DBInstance by its identifier with options to handle final snapshots and automated backups.

        Params: db_instance_identifier: The primary key (ID) of the DBInstance to delete.
                skip_final_snapshot: If True, skip creating a final snapshot before deletion. Defaults to False.
                final_db_snapshot_identifier: If skip_final_snapshot is False, specify an identifier for the final DB snapshot.
                delete_automated_backups: If True, delete automated backups along with the DBInstance. Defaults to False.
        """
        if not self.dal.is_db_instance_exist(db_instance_identifier):
            raise DBInstanceNotFoundError('This DB instance identifier does not exist')
        
        # Get the DBInstance to delete - object
        db_instance = self.get(db_instance_identifier)
        
        # Handle final snapshot before deletion if required
        if skip_final_snapshot == False:
            if final_db_snapshot_identifier is None:
                raise ParamValidationError('If skip_final_snapshot is False, final_db_snapshot_identifier must be specified')
            create_db_snapshot(
                db_instance_identifier=db_instance_identifier,
                db_snapshot_identifier=final_db_snapshot_identifier
            )
        
        self.dal.deleteInMemoryDBInstance(db_instance_identifier)
        
        # Clean up storage (delete associated directory)
        endpoint = db_instance.endpoint
        storageManager = StorageManager(DBInstance.BASE_PATH)
        storageManager.delete_directory(endpoint)
        
        # Delete the DBInstance object
        del db_instance

    def describe(self, db_instance_identifier):
        """
        Describe the details of a DBInstance.

        Params: db_instance_identifier: str, identifier of the DBInstance to describe.
        
        Raises: DBInstanceNotFoundError: if the DB instance does not exist.

        Return: dict with a 'DBInstance' key containing the details of the instance.
        """
        if not self.dal.is_db_instance_exist(db_instance_identifier):
            raise DBInstanceNotFoundError('This DB instance identifier does not exist')
        
        # Retrieve and structure the DBInstance description
        describe_db_instance = self.dal.describeDBInstance(db_instance_identifier)[0]
        describe_db_instance_dict = {
            'db_instance_identifier': describe_db_instance[0],
            'allocated_storage': describe_db_instance[1],
            'master_username': describe_db_instance[2],
            'master_user_password': describe_db_instance[3],
            'db_name': describe_db_instance[4],
            'port': describe_db_instance[5],
            'status': describe_db_instance[6],
            'created_time': str(describe_db_instance[7]),
            'endpoint': describe_db_instance[8],
            'databases': describe_db_instance[9],
            'pk_value': describe_db_instance[10]
        }
    
        return {'DBInstance': describe_db_instance_dict}

    def modify(self, **updates):
        """
        Modify an existing DBInstance.

        Params: updates: dict containing the attributes to modify in the DBInstance.
                        Required field: 'db_instance_identifier'
                        Optional fields: 'port', 'allocated_storage', 'master_user_password'
        
        Raises: DBInstanceNotFoundError: if the DB instance does not exist.
        
        Return: dict with a 'DBInstance' key containing the updated instance's details.
        """
        required_params = ['db_instance_identifier']
        all_params = ['port', 'allocated_storage', 'master_user_password']
        all_params.extend(required_params)
        
        # Validate required and extra parameters
        check_required_params(required_params, updates)
        check_extra_params(all_params, updates)
        
        db_instance_identifier = updates['db_instance_identifier']
        
        # Prepare the SQL-like set clause for updates
        filtered_updates = {key: value for key, value in updates.items() if key != 'db_instance_identifier'}
        set_clause = ', '.join([f"{key} = '{value}'" for key, value in filtered_updates.items()])
        
        self.dal.modifyDBInstance(db_instance_identifier, set_clause)
        
        # Return the updated DB instance details
        update_db_instance = self.get(db_instance_identifier)
        return {'DBInstance': update_db_instance}

    def get(self, db_instance_identifier):
        """
        Retrieve a DBInstance object from the database.

        Params: db_instance_identifier: str, identifier of the DBInstance to retrieve.
        
        Return: DBInstance object or None if not found.
        """
        describe_result = self.describe(db_instance_identifier)
        
        if describe_result:
            describe_result = describe_result['DBInstance']
            return DBInstance(**describe_result)
        
        return None
