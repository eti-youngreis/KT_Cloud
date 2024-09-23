import json
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from typing import Dict, Optional
from DataAccess import DBClusterManager
from Models import DBClusterModel
from Abc import DBO
from Validation import DBClusterValiditions
from DataAccess import DBClusterManager
from Validation.DBClusterValiditions import (
    validate_db_cluster_identifier, 
    validate_engine, 
    validate_database_name, 
    validate_db_cluster_parameter_group_name, 
    validate_db_subnet_group_name, 
    validate_port,
    check_required_params,
    validate_master_user_password,
    validate_master_username
)
import Exceptions.DBClusterExceptions as DBClusterExceptions
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager

class DBClusterService:
    def __init__(self, dal: DBClusterManager, storage_manager: StorageManager, directory:str):
        self.dal = dal
        self.directory = directory
        self.storage_manager = storage_manager
        if not self.storage_manager.is_directory_exist(directory):
            self.storage_manager.create_directory(directory)
    
    def get_file_path(self, cluster_name: str):
        return str(self.directory)+'\\'+str(cluster_name)+'.json'

    def is_cluster_exist(self, cluster_identifier: str):
        cluster_path = self.get_file_path(cluster_identifier)
        cluster_configurations_path = self.get_file_path(cluster_identifier+"_configurations")
        if not self.storage_manager.is_file_exist(cluster_configurations_path) or not self.storage_manager.is_directory_exist(cluster_path):
            return False
        
        if not self.dal.is_exists(cluster_identifier):
            return False
        
        return True
    
    def _validate_parameters(self, **kwargs):
        # Perform validations
        if 'db_cluster_identifier' in kwargs and self.dal.is_db_cluster_exist(kwargs.get('db_cluster_identifier')):
            raise DBClusterExceptions.DBClusterAlreadyExists(kwargs.get('db_cluster_identifier'))
        
        if 'db_cluster_identifier' in kwargs :
            validate_db_cluster_identifier(kwargs.get('db_cluster_identifier'))
        validate_engine(kwargs.get('engine', ''))
        validate_db_subnet_group_name(kwargs.get('db_subnet_group_name'))

        if 'database_name' in kwargs:
            validate_database_name(kwargs['database_name'])
        if 'db_cluster_parameter_group_name' in kwargs :
            validate_db_cluster_parameter_group_name(kwargs['db_cluster_parameter_group_name'])
        if 'port' in kwargs:
            validate_port(kwargs['port'])
        if 'master_username' in kwargs :
            validate_master_username(kwargs['master_username'])
        if 'master_user_password' in kwargs :
            validate_master_user_password(kwargs['master_user_password'], kwargs.get('manage_master_user_password', False))


    def create(self, instance_controller, **kwargs):

        '''Create a new DBCluster.'''

        # Validate required parameters
        # required_params = ['db_cluster_identifier', 'engine', 'db_subnet_group_name', 'allocated_storage']
        required_params = ['db_cluster_identifier', 'engine', 'allocated_storage']
        check_required_params(required_params, **kwargs)
        self._validate_parameters(**kwargs)

        
        # Create the cluster object
        cluster = DBClusterModel.Cluster(**kwargs)

        # Create physical folder structure
        cluster_directory = str(self.directory)+'\\'+str(cluster.db_cluster_identifier)
        self.storage_manager.create_directory(cluster_directory)

        # Set cluster endpoint
        cluster.cluster_endpoint = cluster_directory

        primary_instance_name = f'{cluster.db_cluster_identifier}-primary'
        primary_instance = instance_controller.create_db_instance(
            db_instance_identifier=primary_instance_name,
            # cluster_identifier=cluster.db_cluster_identifier,
            allocated_storage=cluster.allocated_storage,
            master_username=cluster.master_username,
            master_user_password=cluster.master_user_password
        )

        # Retrieve primary instance details
        primary_instance_json_string = primary_instance.get("DBInstance")
        cluster.instances_endpoints["primary_instance"] = primary_instance_json_string.get("endpoint")
        cluster.primary_writer_instance = primary_instance_json_string.get('db_instance_identifier')

        # # Create configuration file
        configuration_file_path = cluster_directory+'\\'+cluster.db_cluster_identifier + "_configurations.json"
        json_object = json.dumps(cluster.to_dict())
        self.storage_manager.create_file(
            file_path=configuration_file_path, content=json_object)
        
        cluster_to_sql = cluster.to_sql()
        return self.dal.createInMemoryDBCluster(cluster_to_sql)


    def delete(self,instance_controller, cluster_identifier:str):
        '''Delete an existing DBCluster.'''
        
        if not self.dal.is_db_cluster_exist(cluster_identifier):
            raise DBClusterExceptions.DBClusterNotFoundException(cluster_identifier)
          
        file_path = self.get_file_path(cluster_identifier+"_configurations")
        self.storage_manager.delete_file(file_path=file_path)

        directory_path = str(self.directory)+'\\'+str(cluster_identifier)
        self.storage_manager.delete_directory(directory_path)

        columns = ['db_cluster_identifier', 'engine', 'allocated_storage', 'copy_tags_to_snapshot',
                'db_cluster_instance_class', 'database_name', 'db_cluster_parameter_group_name',
                'db_subnet_group_name', 'deletion_protection', 'engine_version', 'master_username',
                'master_user_password', 'manage_master_user_password', 'option_group_name', 'port',
                'replication_source_identifier', 'storage_encrypted', 'storage_type', 'tags',
                'created_at', 'status', 'primary_writer_instance', 'reader_instances', 'cluster_endpoint',
                'instances_endpoints', 'pk_column', 'pk_value']
        
        #update configurations
        current_cluster = self.describe(cluster_identifier)
        cluster_dict = dict(zip(columns, current_cluster[0]))
        instance_controller.delete_db_instance(db_instance_identifier = cluster_dict['primary_writer_instance'],skip_final_snapshot = True)
        if cluster_dict['reader_instances'] != '[]' :
            for id in cluster_dict['reader_instances']:
                instance_controller.delete_db_instance(db_instance_identifier = id, skip_final_snapshot = True)


        self.dal.deleteInMemoryDBCluster(cluster_identifier)


    def describe(self, cluster_id):
        '''Describe the details of DBCluster.'''
        if not self.dal.is_db_cluster_exist(cluster_id):
            raise DBClusterExceptions.DBClusterNotFoundException(cluster_id)
        
        return self.dal.describeDBCluster(cluster_id)


    def modify(self, cluster_id: str, **kwargs):
        '''Modify an existing DBCluster.'''

        if not self.dal.is_db_cluster_exist(cluster_id):
            raise DBClusterExceptions.DBClusterNotFoundException(cluster_id)
        
        self._validate_parameters(**kwargs)

        str_parts = ', '.join(f"{key} = '{value}'" for key, value in kwargs.items())

        #update in memory
        self.dal.modifyDBCluster(cluster_id,str_parts)
 
        columns = ['db_cluster_identifier', 'engine', 'allocated_storage', 'copy_tags_to_snapshot',
                'db_cluster_instance_class', 'database_name', 'db_cluster_parameter_group_name',
                'db_subnet_group_name', 'deletion_protection', 'engine_version', 'master_username',
                'master_user_password', 'manage_master_user_password', 'option_group_name', 'port',
                'replication_source_identifier', 'storage_encrypted', 'storage_type', 'tags',
                'created_at', 'status', 'primary_writer_instance', 'reader_instances', 'cluster_endpoint',
                'instances_endpoints', 'pk_column', 'pk_value']
        
        #update configurations
        current_cluster = self.describe(cluster_id)
        cluster_dict = dict(zip(columns, current_cluster[0]))
        cluster_string = json.dumps(cluster_dict, indent=4)

        file_path = self.get_file_path(cluster_id+'_configurations')
        self.storage_manager.delete_file(file_path)
        self.storage_manager.create_file(file_path, cluster_string)


    def get_all_cluster(self):
        return self.dal.get_all_clusters()