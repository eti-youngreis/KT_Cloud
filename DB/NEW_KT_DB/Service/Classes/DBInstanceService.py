import os
import shutil
from typing import Dict, Optional
from Exception.exception import DBInstanceNotFoundError, ParamValidationError
from KT_Cloud.Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from Validation.DBInstanceValidition import check_extra_params, check_required_params,is_valid_db_instance_identifier
from Models.DBInstanceModel import DBInstance
from Service.Abc.DBO import DBO
from DataAccess.DBInstanceManager import DBInstanceManager
from Exception.exception import AlreadyExistsError

class DBInstanceService(DBO):
    def __init__(self, dal:DBInstanceManager ):
        self.dal = dal
        
    # validations here

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
        print('db_instance:',db_instance)
        self.dal.createInMemoryDBInstance(db_instance)
        # create object in code using DBClusterModel.init()- assign all **attributes
        # create physical object as described in task
        # save in memory using DBClusterManager.createInMemoryDBCluster() function
        return {'DBInstance': db_instance.to_dict()}
        
    def delete(self,kwargs):
        #check if working in this db_instance
        # assign None to code object
        # delete physical object
        # delete from memory using DBClusterManager.deleteInMemoryDBCluster() function- send criteria using self attributes
        """Delete a DB instance."""
        required_params = ['db_instance_identifier']
        all_params = ['skip_final_snapshot', 'final_db_snapshot_identifier', 'delete_automated_backups']
        all_params.extend(required_params)
        check_required_params(required_params, kwargs)  # check if there are all required parameters
        check_extra_params(all_params, kwargs) #check if there are not extra params that the function can't get
        db_instance_identifier = kwargs['db_instance_identifier']
        if not self.dal.is_db_instance_identifier_exist(db_instance_identifier):  # check if db to delete exists
            raise DBInstanceNotFoundError('This DB instance identifier does not exist')
        if 'skip_final_snapshot' not in kwargs or kwargs['skip_final_snapshot'] == False: #if need to do final snapshot
            if 'final_db_snapshot_identifier' not in kwargs: #raise when snapshot id was not given
                raise ParamValidationError('If you do not enable skip_final_snapshot parameter, you must specify the FinalDBSnapshotIdentifier parameter')
            create_db_snapshot(db_instance_identifier=kwargs['db_instance_identifier'], #create final snapshot
                               db_snapshot_identifier=kwargs['final_db_snapshot_identifier'])
        db_instance=self.describe(db_instance_identifier)
        self.dal.deleteInMemoryDBInstance(db_instance_identifier)
        print('db_instance:',db_instance)
        endpoint = db_instance['endpoint']
        storageManager=StorageManager(DBInstance.BASE_PATH)
        storageManager.delete_directory(endpoint)
        del db_instance

    def describe(self,db_instance_identifier):
        '''Describe the details of DBCluster.'''
        if not self.dal.is_db_instance_identifier_exist(db_instance_identifier):  # check if db to delete exists
            raise DBInstanceNotFoundError('This DB instance identifier does not exist')
        print('describe:',self.dal.describeDBInstance(db_instance_identifier))
        return self.dal.describeDBInstance(db_instance_identifier)
        # use DBClusterManager.describeDBCluster() function 

    def modify(self, **updates):
        '''Modify an existing DBCluster.'''
        required_params = ['db_instance_identifier']
        all_params = ['port','allocated_storage', 'master_user_password']
        all_params.extend(required_params)
        check_required_params(required_params, updates)  # check if there are all required parameters
        check_extra_params(all_params, updates) #check if there are not extra params that the function can't get
        db_instance_identifier=updates['db_instance_identifier']
        #check if working - the status in this db_instance
        update_db_instance=self.describe(db_instance_identifier)
        for param in all_params:
            print('update_db_instance:',update_db_instance)
            update_db_instance[param]=updates.get(param,update_db_instance[param])
        self.dal.modifyDBInstance(db_instance_identifier,update_db_instance)
        # update object in code
        # modify physical object
        # update object in memory using DBClusterManager.modifyInMemoryDBCluster() function- send criteria using self attributes
        return {'DBInstance':update_db_instance}

    def get(self,db_instance_identifier):
        '''get code object.'''
        return self.describe(db_instance_identifier)
        # return real time object
