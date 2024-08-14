from datetime import datetime
from typing import Dict

class Cluster:

    def __init__(self, **kwargs): 

        self.db_cluster_identifier = kwargs['db_cluster_identifier']
        self.engine = kwargs['engine']
        self.allocated_storage = kwargs.get('allocated_storage',None)
        self.copy_tags_to_snapshot = kwargs.get('copy_tags_to_snapshot',False)
        self.database_name = kwargs.get('database_name',None)
        self.db_cluster_parameter_group_name = kwargs.get('db_cluster_parameter_group_name',None)
        self.db_subnet_group_name = kwargs.get('db_subnet_group_name',None)
        self.deletion_protection = kwargs.get('deletion_protection',False)
        self.option_group_name = kwargs.get('option_group_name',None)
        self.master_username = kwargs.get('master_username',None)
        self.master_user_password = kwargs.get('master_user_password',None)
        self.port = kwargs.get('port',None)#handle defuelt values
        self.tags  = kwargs.get('tags',None)
        self.created_at = datetime.now()
        self.status = 'available'
        # self.primary_writer_instance = create db instance
        # self.reader_instances = replica of the instance in different azs


    def to_dict(self) -> Dict:
        '''Retrieve the data of the DB cluster as a dictionary.'''
        return {
           'db_cluster_identifier': self.db_cluster_identifier ,
            'engine': self.engine,
            'allocated_storage': self.allocated_storage ,
            'copy_tags_to_snapshot': self.copy_tags_to_snapshot ,
            'database_name': self.database_name ,
            'db_cluster_parameter_group_name': self.db_cluster_parameter_group_name ,
            'db_subnet_group_name': self.db_subnet_group_name ,
            'deletion_protection': self.deletion_protection ,
            'option_group_name': self.option_group_name ,
            'master_username' : self.master_username ,
            'master_user_password' : self.master_user_password ,
            'port': self.port ,
            'tags': self.tags ,
            'created_at': self.created_at,
            'status':self.status,
            # 'primary_writer_instance' : self.primary_writer_instance,
            # 'reader_instances' : self.reader_instances
        }

    