from datetime import datetime
from typing import Dict
from DataAccess import ObjectManager
import os
class Cluster:

    def __init__(self, **kwargs): 

        self.db_cluster_identifier = kwargs['db_cluster_identifier']
        self.engine = kwargs['engine']
        self.allocated_storage = kwargs['allocated_storage']
        self.copy_tags_to_snapshot = kwargs.get('copy_tags_to_snapshot', False)
        self.db_cluster_instance_class = kwargs.get('db_cluster_instance_class', False)
        self.database_name = kwargs.get('database_name', None)
        self.db_cluster_parameter_group_name = kwargs.get('db_cluster_parameter_group_name', None)
        self.db_subnet_group_name = kwargs.get('db_subnet_group_name', None)
        self.deletion_protection = kwargs.get('deletion_protection', False)
        self.engine_version = kwargs.get('engine_version', None)
        self.master_username = kwargs.get('master_username',None)
        self.master_user_password = kwargs.get('master_user_password',None)
        self.manage_master_user_password = kwargs.get('manage_master_user_password',False)
        self.option_group_name = kwargs.get('option_group_name', None)
        self.port = kwargs.get('port', None) #handle defuelt values
        self.replication_source_identifier = kwargs.get('replication_source_identifier', None)
        self.storage_encrypted = kwargs.get('storage_encrypted', None)
        self.storage_type = kwargs.get('storage_type', 'aurora')
        self.tags  = kwargs.get('tags', None)
        self.created_at = datetime.now()
        self.status = 'available'
        self.primary_writer_instance = None
        self.reader_instances = []
        self.cluster_endpoint = None
        self.instances_endpoints = {}  # Added attribute to store endpoints

        self.pk_column = kwargs.get('pk_column', 'ClusterID')
        self.pk_value = kwargs.get('pk_value', self.db_cluster_identifier)


    def to_dict(self) -> Dict:
        '''Retrieve the data of the DB cluster as a dictionary.'''

        return ObjectManager.convert_object_attributes_to_dictionary(
            db_cluster_identifier=self.db_cluster_identifier,
            engine=self.engine,
            allocated_storage=self.allocated_storage,
            copy_tags_to_snapshot=self.copy_tags_to_snapshot,
            db_cluster_instance_class=self.db_cluster_instance_class,
            database_name=self.database_name,
            db_cluster_parameter_group_name=self.db_cluster_parameter_group_name,
            db_subnet_group_name=self.db_subnet_group_name,
            deletion_protection=self.deletion_protection,
            engine_version=self.engine_version,
            master_username = self.master_username,
            master_user_password = self.master_user_password,
            manage_master_user_password = self.manage_master_user_password,
            option_group_name=self.option_group_name,
            port=self.port,
            replication_source_identifier=self.replication_source_identifier,
            storage_encrypted=self.storage_encrypted,
            storage_type=self.storage_type,
            tags=self.tags,
            created_at=self.created_at,
            status=self.status,
            primary_writer_instance=self.primary_writer_instance,
            reader_instances=self.reader_instances,
            cluster_endpoint = self.cluster_endpoint,
            instances_endpoints=self.instances_endpoints,
            pk_column=self.pk_column,
            pk_value=self.pk_value
        )

    # def to_dict(self) -> Dict:
    #     '''Retrieve the data of the DB cluster as a dictionary.'''

    #     return ObjectManager.convert_object_attributes_to_dictionary(
    #         db_cluster_identifier=self.db_cluster_identifier, 
    #         engine=self.engine, 
    #         availability_zones=self.availability_zones, 
    #         copy_tags_to_snapshot=self.copy_tags_to_snapshot, 
    #         database_name=self.database_name, 
    #         db_cluster_parameter_group_name=self.db_cluster_parameter_group_name, 
    #         db_subnet_group_name=self.db_subnet_group_name, 
    #         deletion_protection=self.deletion_protection, 
    #         enable_cloudwatch_logs_exports=self.enable_cloudwatch_logs_exports, 
    #         enable_global_write_forwarding=self.enable_global_write_forwarding, 
    #         enable_http_endpoint=self.enable_http_endpoint, 
    #         enable_limitless_database=self.enable_limitless_database, 
    #         enable_local_write_forwarding=self.enable_local_write_forwarding, 
    #         engine_version=self.engine_version, 
    #         global_cluster_identifier=self.global_cluster_identifier, 
    #         option_group_name=self.option_group_name, 
    #         port=self.port, 
    #         replication_source_identifier=self.replication_source_identifier, 
    #         scaling_configuration=self.scaling_configuration, 
    #         storage_encrypted=self.storage_encrypted, 
    #         storage_type=self.storage_type, 
    #         tags=self.tags, 
    #         created_at=self.created_at, 
    #         status=self.status, 
    #         primary_writer_instance = self.primary_writer_instance,
    #         reader_instances = self.reader_instances,
    #         endpoints=self.endpoints,
    #         pk_column=self.pk_column,
    #         pk_value=self.pk_value
    #     )