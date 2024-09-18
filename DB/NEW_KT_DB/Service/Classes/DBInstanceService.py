import os
import shutil
import sys
from typing import Dict, Optional
from Exception.exception import DBInstanceNotFoundError, ParamValidationError
from Validation.DBInstanceValidition import check_extra_params, check_required_params,is_valid_db_instance_identifier
from Models.DBInstanceModel import DBInstance
from Service.Abc.DBO import DBO
from DataAccess.DBInstanceManager import DBInstanceManager
from Exception.exception import AlreadyExistsError
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager


class DBInstanceService(DBO):
    def __init__(self, dal:DBInstanceManager ):
        self.dal = dal

    def create(self,**attributes):
        '''Create a new DBCluster.'''
        required_params = ['db_instance_identifier', 'master_username', 'master_user_password']
        all_params = ['db_name', 'port','allocated_storage']
        all_params.extend(required_params)
        check_required_params(required_params, attributes)  # check if there are all required parameters
        check_extra_params(all_params, attributes) #check if there are not extra params that the function can't get
        db_instance_identifier=attributes['db_instance_identifier']
        if not is_valid_db_instance_identifier(db_instance_identifier,63):
            raise ValueError('db_instance_identifier is invalid')
        if self.dal.is_db_instance_identifier_exist(db_instance_identifier):
            raise AlreadyExistsError(f"the id {db_instance_identifier} is already exist")
        db_instance = DBInstance(**attributes)
        self.dal.createInMemoryDBInstance(db_instance)
        return {'DBInstance': db_instance.to_dict()}
        
    def delete(self,kwargs):
        """Delete a DB instance."""
        required_params = ['db_instance_identifier']
        all_params = ['skip_final_snapshot', 'final_db_snapshot_identifier', 'delete_automated_backups']
        all_params.extend(required_params)
        check_required_params(required_params, kwargs)  # check if there are all required parameters
        check_extra_params(all_params, kwargs) #check if there are not extra params that the function can't get
        db_instance_identifier = kwargs['db_instance_identifier']
        if not self.dal.is_db_instance_identifier_exist(db_instance_identifier):  # check if db to delete exists
            raise DBInstanceNotFoundError('This DB instance identifier does not exist')
        db_instance=self.get(db_instance_identifier)
        if 'skip_final_snapshot' not in kwargs or kwargs['skip_final_snapshot'] == False: #if need to do final snapshot
            if 'final_db_snapshot_identifier' not in kwargs: #raise when snapshot id was not given
                raise ParamValidationError('If you do not enable skip_final_snapshot parameter, you must specify the FinalDBSnapshotIdentifier parameter')
            create_db_snapshot(db_instance_identifier=kwargs['db_instance_identifier'], #create final snapshot
                               db_snapshot_identifier=kwargs['final_db_snapshot_identifier'])
        self.dal.deleteInMemoryDBInstance(db_instance_identifier)
        endpoint = db_instance.endpoint
        storageManager=StorageManager(DBInstance.BASE_PATH)
        storageManager.delete_directory(endpoint)
        del db_instance

    def describe(self,db_instance_identifier):
        '''Describe the details of DBCluster.'''
        if not self.dal.is_db_instance_identifier_exist(db_instance_identifier):  # check if db to delete exists
            raise DBInstanceNotFoundError('This DB instance identifier does not exist')
        describe_db_instance=self.dal.describeDBInstance(db_instance_identifier)[0]
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
    
        return {'DBInstance':describe_db_instance_dict}

    def modify(self, **updates):
        '''Modify an existing DBCluster.'''
        required_params = ['db_instance_identifier']
        all_params = ['port','allocated_storage', 'master_user_password']
        all_params.extend(required_params)
        check_required_params(required_params, updates)  # check if there are all required parameters
        check_extra_params(all_params, updates) #check if there are not extra params that the function can't get
        db_instance_identifier=updates['db_instance_identifier']
        filtered_updates = {key: value for key, value in updates.items() if key != 'db_instance_identifier'}
        set_clause = ', '.join([f"{key} = '{value}'" for key, value in filtered_updates.items()])
        self.dal.modifyDBInstance(db_instance_identifier,set_clause)
        update_db_instance=self.get(db_instance_identifier)
        return {'DBInstance':update_db_instance}

    def get(self,db_instance_identifier):
        '''get code object.'''
        describe_result=self.describe(db_instance_identifier)
        if describe_result :
            describe_result=describe_result['DBInstance']
            return DBInstance(
                **describe_result
            )
        return None

