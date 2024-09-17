from typing import Dict, Optional

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from DataAccess import DBClusterManager
from Models import DBClusterModel
from Abc import DBO
from Validation import DBClusterValiditions
from DataAccess import DBClusterManager
# from DBInstanceService import DBInstanceService
import os
import json
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
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager

class DBClusterService:
    def __init__(self, dal: DBClusterManager, storage_manager: StorageManager, directory:str):
        self.dal = dal,
        self.directory = directory,
        self.storage_manager = storage_manager
        if not self.storage_manager.is_directory_exist(directory):
            self.storage_manager.create_directory(directory)
    
    def get_file_path(self, cluster_name: str):
        return os.path.join(self.directory, cluster_name + '.json')

    def is_cluster_exist(self, cluster_identifier: str):
        cluster_path = self.get_file_path(cluster_identifier)
        cluster_configurations_path = self.get_file_path(cluster_identifier+"_configurations")
        if not self.storage_manager.is_file_exist(cluster_configurations_path) or not self.storage_manager.is_directory_exist(cluster_path):
            return False
        
        if not self.dal.is_exists(cluster_identifier):
            return False
        
        return True


    def create(self, **kwargs):

        '''Create a new DBCluster.'''

        # Validate required parameters
        required_params = ['db_cluster_identifier', 'engine', 'db_subnet_group_name', 'allocated_storage']
        if not check_required_params(required_params, **kwargs):
            raise ValueError("Missing required parameters")

        # Perform validations
        if self.dal.is_exists(kwargs.get('db_cluster_identifier')):
            raise ValueError(f"Cluster {kwargs.get('db_cluster_identifier')} already exists")
        
        if not validate_db_cluster_identifier(kwargs.get('db_cluster_identifier')):
            raise ValueError(f"Invalid DBClusterIdentifier: {kwargs.get('db_cluster_identifier')}")

        if not validate_engine(kwargs.get('engine', '')):
            raise ValueError(f"Invalid engine: {kwargs.get('engine')}")

        if 'database_name' in kwargs and kwargs['database_name'] and not validate_database_name(kwargs['database_name']):
            raise ValueError(f"Invalid DatabaseName: {kwargs['database_name']}")

        if 'db_cluster_parameter_group_name' in kwargs and kwargs['db_cluster_parameter_group_name'] and not validate_db_cluster_parameter_group_name(kwargs['db_cluster_parameter_group_name']):
            raise ValueError(f"Invalid DBClusterParameterGroupName: {kwargs['db_cluster_parameter_group_name']}")

        if kwargs.get('db_subnet_group_name') and not validate_db_subnet_group_name(kwargs.get('db_subnet_group_name')):
            raise ValueError(f"Invalid DBSubnetGroupName: {kwargs['db_subnet_group_name']}")

        if 'port' in kwargs and kwargs['port'] and not validate_port(kwargs['port']):
            raise ValueError(f"Invalid port: {kwargs['port']}. Valid range is 1150-65535.")

        if 'master_username' in kwargs and not validate_master_username(kwargs['master_username']):
            raise ValueError("Invalid master username")

        if 'master_user_password' in kwargs and not validate_master_user_password(kwargs['master_user_password'], kwargs.get('manage_master_user_password', False)):
            raise ValueError("Invalid master user password")

        # Create the cluster object
        cluster = DBClusterModel.Cluster(**kwargs)

        # Create physical folder structure
        # desktop_path = os.path.join(os.path.expanduser('~'), 'Desktop')
        # cluster_directory = os.path.join(desktop_path, f'Clusters/{cluster.db_cluster_identifier}')
        cluster_directory = self.get_file_path(cluster.db_cluster_identifier)
        self.storage_manager.create_directory(cluster_directory)
        # os.makedirs(cluster_directory, exist_ok=True)

        # Set cluster endpoint
        cluster.cluster_endpoint = cluster_directory

        # Create the primary writer instance
        primary_instance_name = f'{cluster.db_cluster_identifier}-primary'
        # primary_instance = self.DBInstanceService(
        #     instance_name=primary_instance_name,
        #     cluster_identifier=cluster.db_cluster_identifier,
        #     allocated_storage=cluster.allocated_storage,
        #     master_username=cluster.master_username,
        #     master_user_password=cluster.master_user_password
        # )
        primary_instance = {
            "DBInstance": {
                "db_instance_identifier": "my-db-instance-1",
                "endpoint": {
                    "address": "my-db-instance-1.123456789012.us-west-2.rds.amazonaws.com",
                    "port": 3306,
                    "hosted_zone_id": "Z1PVIF0EXAMPLE"
                },
            }
        }

        # Retrieve primary instance details
        primary_instance_json_string = primary_instance.get("DBInstance")
        # primary_instance_json_data = json.loads(primary_instance_json_string)
        cluster.instances_endpoints["primary_instance"] = primary_instance_json_string.get("endpoint")
        cluster.primary_writer_instance = primary_instance_json_string.get('db_instance_identifier')

        # # Create configuration file
        # cluster_config_path = os.path.join(cluster_directory, 'cluster_config.json')
        # cluster_dict = cluster.to_dict()
        # try:
        #     with open(cluster_config_path, 'w') as file:
        #         json.dump(cluster_dict, file, indent=4)
        # except IOError as e:
        #     raise RuntimeError(f"Failed to write configuration file: {e}")
        
        json_object = json.dumps(cluster.to_dict())
        file_path = self.get_file_path(cluster.db_cluster_identifier+"_configurations")
        self.storage_manager.create_file(
            file_path=file_path, content=json_object)
        
        # cluster_to_sql = cluster.to_sql()
        ccc = cluster.cluster_to_dict()
        bbb = json.dumps(ccc)
        # Store the cluster information in the database
        self.dal.createInMemoryDBCluster(bbb)

        return {"DBCluster": cluster_dict}



    def delete(self, cluster_identifier:str):
        '''Delete an existing DBCluster.'''
        
        if not self.is_cluster_exist(cluster_identifier):
            raise ValueError("Cluster does not exist!!")
          
        file_path = self.get_file_path(cluster_identifier+"_configurations")
        self.storage_manager.delete_file(file_path=file_path)

        directory_path = self.get_file_path(cluster_identifier)
        self.storage_manager.delete_directory(directory_path)

        self.dal.deleteInMemoryDBCluster(cluster_identifier)


    def describe(self, cluster_id):
        '''Describe the details of DBCluster.'''
        if not self.is_cluster_exist(cluster_id):
            raise ValueError("Cluster does not exist!!")
        
        return self.dal.describeDBCluster(cluster_id)


    def modify(self, cluster_id: str, **kwargs):
        '''Modify an existing DBCluster.'''
        # update object in code
        # modify physical object
        # update object in memory using DBClusterManager.modifyInMemoryDBCluster() function- send criteria using self attributes
        if not self.is_cluster_exist(cluster_id):
            raise ValueError("Cluster does not exist!!")
        
        if 'db_cluster_identifier' in kwargs and not validate_db_cluster_identifier(kwargs.get('db_cluster_identifier')):
            raise ValueError(f"Invalid DBClusterIdentifier: {kwargs.get('db_cluster_identifier')}")

        if 'engine' in kwargs and not validate_engine(kwargs.get('engine', '')):
            raise ValueError(f"Invalid engine: {kwargs.get('engine')}")

        if 'database_name' in kwargs and kwargs['database_name'] and not validate_database_name(kwargs['database_name']):
            raise ValueError(f"Invalid DatabaseName: {kwargs['database_name']}")

        if 'db_cluster_parameter_group_name' in kwargs and kwargs['db_cluster_parameter_group_name'] and not validate_db_cluster_parameter_group_name(kwargs['db_cluster_parameter_group_name']):
            raise ValueError(f"Invalid DBClusterParameterGroupName: {kwargs['db_cluster_parameter_group_name']}")

        if kwargs.get('db_subnet_group_name') and not validate_db_subnet_group_name(kwargs.get('db_subnet_group_name')):
            raise ValueError(f"Invalid DBSubnetGroupName: {kwargs['db_subnet_group_name']}")

        if 'port' in kwargs and kwargs['port'] and not validate_port(kwargs['port']):
            raise ValueError(f"Invalid port: {kwargs['port']}. Valid range is 1150-65535.")

        if 'master_username' in kwargs and not validate_master_username(kwargs['master_username']):
            raise ValueError("Invalid master username")

        if 'master_user_password' in kwargs and not validate_master_user_password(kwargs['master_user_password'], kwargs.get('manage_master_user_password', False)):
            raise ValueError("Invalid master user password")

        current_cluster = self.describe(cluster_id)

        # Update the cluster object
        for key, value in kwargs.items():
            setattr(current_cluster, key, value)
        #update in memory
        self.dal.modifyDBCluster(cluster_id,current_cluster)

        #update configurations
        file_path = self.get_file_path(cluster_id+'_configurations')
        self.storage_manager.delete_file(file_path)
        self.storage_manager.create_file(file_path, current_cluster)
        # self.storage_manager.create_file(file_path, json.dumps(current_cluster))



        
