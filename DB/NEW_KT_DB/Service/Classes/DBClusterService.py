from typing import Dict, Optional
from DataAccess import ClusterManager
from Models import DBClusterModel
from Abc import DBO
from Validation import DBClusterValiditions
from DataAccess import DBClusterManager
from DBInstanceService import DBInstanceService
import os
import json
from DBClusterValiditions import (
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

class DBClusterService(DBO):
    def __init__(self, dal: DBClusterManager):
        self.dal = dal
    
    
    # validations here
    


    def create(self, **kwargs):

        '''Create a new DBCluster.'''

        # Validate required parameters
        required_params = ['db_cluster_identifier', 'engine', 'db_subnet_group_name']
        if not check_required_params(required_params, **kwargs):
            raise ValueError("Missing required parameters")

        # Perform validations
        if not validate_db_cluster_identifier(kwargs.get('db_cluster_identifier', '')):
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
        desktop_path = os.path.join(os.path.expanduser('~'), 'Desktop')
        cluster_directory = os.path.join(desktop_path, f'Clusters/{cluster.db_cluster_identifier}')
        os.makedirs(cluster_directory, exist_ok=True)

        # Set cluster endpoint
        cluster.cluster_endpoint = cluster_directory

        # Create the primary writer instance
        primary_instance_name = f'{cluster.db_cluster_identifier}-primary'
        primary_instance = self.DBInstanceService(
            instance_name=primary_instance_name,
            cluster_identifier=cluster.db_cluster_identifier,
            allocated_storage=cluster.allocated_storage,
            master_username=cluster.master_username,
            master_user_password=cluster.master_user_password
        )

        # Retrieve primary instance details
        primary_instance_json_string = primary_instance.get("DBInstance")
        primary_instance_json_data = json.loads(primary_instance_json_string)
        cluster.instances_endpoints["primary_instance"] = primary_instance_json_data.get("endpoint")
        cluster.primary_writer_instance = primary_instance_json_data.get('db_instance_identifier')

        # Create configuration file
        cluster_config_path = os.path.join(cluster_directory, 'cluster_config.json')
        cluster_dict = cluster.to_dict()
        try:
            with open(cluster_config_path, 'w') as file:
                json.dump(cluster_dict, file, indent=4)
        except IOError as e:
            raise RuntimeError(f"Failed to write configuration file: {e}")

        # Store the cluster information in the database
        self.dal.createInMemoryDBCluster(cluster)

        return {"DBCluster": cluster_dict}



    def delete(self):
        '''Delete an existing DBCluster.'''
        # assign None to code object
        # delete physical object
        # delete from memory using DBClusterManager.deleteInMemoryDBCluster() function- send criteria using self attributes
        pass


    def describe(self):
        '''Describe the details of DBCluster.'''
        # use DBClusterManager.describeDBCluster() function
        pass


    def modify(self, **updates):
        '''Modify an existing DBCluster.'''
        # update object in code
        # modify physical object
        # update object in memory using DBClusterManager.modifyInMemoryDBCluster() function- send criteria using self attributes
        pass


    def get(self):
        '''get code object.'''
        # return real time object
        pass
